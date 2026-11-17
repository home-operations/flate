package loader

import (
	"context"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

type declarationInventory struct {
	input *store.SourceArtifact
	files map[string]manifest.FileDeclarations
	owned bool
	paths []string
}

// CompleteDeclarations MUST finish before readers consume the immutable index.
func (l *Loader) CompleteDeclarations(
	ctx context.Context,
	demand selection.DeclarationDemand,
) (*manifest.DiscoveryIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	changes, side := demand.Changes, demand.Side
	if changes == nil {
		return l.Index, nil
	}
	reader := l.documents
	restoreSkipping := reader.skipDeclarations
	if reader.cache == nil {
		reader.cache = map[string]document{}
	}
	reader.declarationsOnly, reader.skipDeclarations = true, false
	reader.mountSources(l.Store, nil, l.Index)
	reader.intent, reader.coordinate = manifest.IncidentalDocuments, nil
	index := *l.Index
	index.Declarations = map[manifest.NamedResource]map[string]manifest.FileDeclarations{}
	copiedSources := false
	var sourceBuffer [16]manifest.NamedResource
	sources := sourceBuffer[:0]
	for source := range changes.Sources() {
		sources = append(sources, source)
	}
	slices.SortFunc(sources, manifest.NamedResource.Compare)
	for _, source := range sources {
		delta, input := l.declarationInput(side, source, changes)
		if input == nil ||
			!input.SameMaterial(
				reader.artifacts[input.LocalPath],
				store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
			) {
			continue
		}
		if !input.SameMaterial(
			l.documents.artifacts[input.LocalPath],
			store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
		) {
			if !copiedSources {
				index.CompleteSources = maps.Clone(index.CompleteSources)
				copiedSources = true
			}
			delete(index.CompleteSources, source)
		}
		inventory := declarationInventory{
			input: input,
			files: l.Index.Declarations[source],
			paths: demand.Files[source],
		}
		err := reader.completeSource(ctx, delta, side, &inventory)
		if err != nil {
			return nil, err
		}
		index.Declarations[source] = inventory.files
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader.declarationsOnly, reader.skipDeclarations = false, restoreSkipping
	l.documents = reader
	l.Index = &index
	return l.Index, nil
}

func (l *Loader) declarationInput(
	side manifest.TreeSide,
	source manifest.NamedResource,
	changes *change.Set,
) (*change.Set, *store.SourceArtifact) {
	delta, inputs, declarations := changes.ForSource(source)
	input, _ := l.Store.GetArtifact(source, store.ArtifactContent|store.ArtifactRevision).(*store.SourceArtifact)
	if input == nil || side > manifest.TreeHead || inputs[side] == nil || !input.Reads.Valid() {
		return nil, nil
	}
	if input != inputs[side] {
		binding := l.sourceBinding(source)
		if !l.frozen || binding == nil || binding.initial != input || !l.sideKnown {
			return nil, nil
		}
		if l.side != side || inputs[side].Input(store.ArtifactContent|store.ArtifactRevision) !=
			input.Input(store.ArtifactContent|store.ArtifactRevision) {
			return nil, nil
		}
	}
	if input.FS == nil || input.LocalPath == "" || l.Store.GetObject(source) != declarations[side] {
		return nil, nil
	}
	indexed, mapped := input.IndexPath(l.SourceRoot, ".")
	relative, local := input.SourcePath(l.SourceRoot, indexed)
	if !mapped || !local || relative != "." {
		return nil, nil
	}
	return delta, input
}

func (r *documentReader) completeSource(
	ctx context.Context,
	delta *change.Set,
	side manifest.TreeSide,
	inventory *declarationInventory,
) error {
	input := inventory.input
	observed, err := r.sourceDeclarations(input, inventory.files)
	if err != nil {
		return err
	}
	inventory.files = observed
	if len(inventory.paths) == 0 {
		return nil
	}

	required := r.requiredDeclarationFiles(input)
	for _, file := range inventory.paths {
		present, known := delta.Present(side, file)
		if !known || !filepath.IsLocal(file) {
			inventory.update(file, manifest.FileDeclarations{}, false)
			continue
		}
		if !present {
			inventory.update(file, manifest.FileDeclarations{Complete: true}, true)
			continue
		}
		if !isManifestFile(file) && !required[file] {
			inventory.update(file, manifest.FileDeclarations{Present: true, Complete: true}, true)
			continue
		}
		if observation := inventory.files[file]; observation.Present && observation.Complete {
			continue
		}
		path := filepath.Join(input.LocalPath, filepath.FromSlash(file))
		previous, retained := r.cache[path]
		observation, err := r.completeFile(ctx, path)
		if err != nil {
			if retained {
				r.cache[path] = previous
			} else {
				delete(r.cache, path)
			}
			return err
		}
		inventory.update(file, observation, true)
	}
	return nil
}

func (i *declarationInventory) update(file string, observation manifest.FileDeclarations, retain bool) {
	prior, exists := i.files[file]
	if exists == retain && (!retain || sameFileDeclarations(prior, observation)) {
		return
	}
	if !i.owned {
		i.files = maps.Clone(i.files)
		if i.files == nil {
			i.files = map[string]manifest.FileDeclarations{}
		}
		i.owned = true
	}
	if retain {
		i.files[file] = observation
	} else {
		delete(i.files, file)
	}
}

func (r *documentReader) requiredDeclarationFiles(input *store.SourceArtifact) map[string]bool {
	var required map[string]bool
	retain := func(file string) {
		if isManifestFile(file) {
			return
		}
		relative, err := filepath.Rel(input.LocalPath, file)
		if err != nil || !filepath.IsLocal(relative) {
			return
		}
		if required == nil {
			required = map[string]bool{}
		}
		required[filepath.ToSlash(relative)] = true
	}
	for file := range r.cache {
		if doc := r.cache[file]; doc.sourceRoot == input.LocalPath && doc.coverage == manifest.RequiredDocuments {
			retain(file)
		}
	}
	if r.parsed == nil || r.parsed.compact != r.compact || !r.parsed.Matches(input) {
		return required
	}
	source := r.parsed.sources[input.LocalPath]
	for layer := &source.parsedDocuments; layer != nil; layer = layer.previous {
		for file, doc := range layer.documents {
			if doc.coverage == manifest.RequiredDocuments && source.lookup(file) == doc {
				retain(file)
			}
		}
	}
	return required
}

func (r *documentReader) sourceDeclarations(
	input *store.SourceArtifact,
	previous map[string]manifest.FileDeclarations,
) (map[string]manifest.FileDeclarations, error) {
	if input == nil ||
		!input.SameMaterial(
			r.artifacts[input.LocalPath],
			store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
		) {
		return nil, nil
	}
	count := 0
	unchanged := true
	for file, doc := range r.cache { //nolint:gocritic // Immutable observations stay paired with their source documents.
		if doc.sourceRoot != input.LocalPath {
			continue
		}
		if doc.declarationsData != nil {
			if err := doc.restoreDeclarations(); err != nil {
				return nil, err
			}
			r.cache[file] = doc
		}
		if doc.declarations == nil {
			continue
		}
		relative, err := filepath.Rel(input.LocalPath, file)
		if err != nil {
			continue
		}
		count++
		prior, exists := previous[filepath.ToSlash(relative)]
		if !exists || !sameFileDeclarations(prior, doc.declarations.FileDeclarations) {
			unchanged = false
		}
	}
	if unchanged && len(previous) == count {
		return previous, nil
	}
	return r.copySourceDeclarations(input.LocalPath, count), nil
}

func (r *documentReader) copySourceDeclarations(root string, count int) map[string]manifest.FileDeclarations {
	var files map[string]manifest.FileDeclarations
	for file, doc := range r.cache { //nolint:gocritic // Immutable observations stay paired with their source documents.
		if doc.sourceRoot != root || doc.declarations == nil {
			continue
		}
		relative, err := filepath.Rel(root, file)
		if err != nil {
			continue
		}
		if files == nil {
			files = make(map[string]manifest.FileDeclarations, count)
		}
		files[filepath.ToSlash(relative)] = doc.declarations.FileDeclarations
	}
	return files
}

func sameFileDeclarations(a, b manifest.FileDeclarations) bool {
	return a.Present == b.Present && a.Complete == b.Complete && maps.Equal(a.Objects, b.Objects)
}

func (r *documentReader) completeFile(ctx context.Context, file string) (manifest.FileDeclarations, error) {
	r.restore(file)
	cached := r.cache[file]
	if cached.declarationsData != nil {
		if err := cached.restoreDeclarations(); err != nil {
			return manifest.FileDeclarations{}, err
		}
		r.cache[file] = cached
	}
	if cached.declarations != nil && !cached.declarations.limited {
		return cached.declarations.FileDeclarations, cached.declarations.err
	}
	// Completion MUST preserve the census borrowed by later construction rounds.
	prepared := cached.objects != nil && !cached.unprepared
	pending := cached
	pending.objects, pending.declarations, pending.parsed = nil, nil, nil
	r.cache[file] = pending
	reader := *r
	reader.parsed = nil
	objects, err := reader.fileObjects(ctx, file, manifest.ParseDocOptions{})
	if err != nil {
		return manifest.FileDeclarations{}, err
	}
	observation := r.cache[file].declarations
	if prepared {
		current := r.cache[file]
		current.objects, current.coverage, current.parsed, current.unprepared = cached.objects, cached.coverage, cached.parsed, false
		r.cache[file] = current
	}
	if observation == nil {
		for i := range objects {
			if objects[i].outcome.Err != nil {
				return manifest.FileDeclarations{}, objects[i].outcome.Err
			}
		}
		return manifest.FileDeclarations{}, nil
	}
	if observation.err == nil {
		return observation.FileDeclarations, nil
	}
	for i := range objects {
		document := objects[i].Doc
		if document == nil {
			document = objects[i].outcome.Document
		}
		manifest.ReleaseDoc(document)
	}
	return observation.FileDeclarations, observation.err
}
