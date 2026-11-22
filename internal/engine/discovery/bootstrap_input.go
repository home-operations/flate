package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (d *Session) resolveBootstrapInput(ctx context.Context) (*store.SourceArtifact, error) {
	var bootstrap *store.SourceArtifact
	if d.resolvedSources == nil {
		admitted, err := d.admitConstructionInput(ctx)
		if err != nil {
			return nil, err
		}
		bootstrap, d.bootstrapInput = admitted, admitted
		d.resolvedSources = &entrySourcePublisher{
			store: d.cfg.Store, root: admitted.LocalPath,
			mutable: map[manifest.NamedResource]entryResolution{},
		}
	} else {
		bootstrap = d.refreshConstructionInput()
	}
	if d.cfg.Entry != nil {
		// Generic aliases MUST be admitted from this store's declarations.
		for id, resolution := range d.cfg.Entry.prepared.sources {
			prepared := resolution.entry || resolution.projected || len(resolution.producers) != 0
			if id == manifest.BootstrapSourceID || !resolution.synthetic && prepared {
				d.resolvedSources.mutable[id] = resolution
			}
		}
		d.publicOperands = d.cfg.Entry.prepared.publicOperands
		d.cfg.Entry = nil
	}
	if previous, ok := d.resolvedSources.snapshot()[manifest.BootstrapSourceID]; ok && previous.input != bootstrap {
		d.aliasViews = nil
		if d.entryAdmission != nil {
			d.entryAdmission.aliasViews = nil
		}
	}
	d.resolvedSources.root = bootstrap.LocalPath
	return bootstrap, nil
}

func (d *Session) refreshConstructionInput() *store.SourceArtifact {
	id := manifest.BootstrapSourceID
	input, _ := d.cfg.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	binding := d.resolvedSources.snapshot()[id]
	material := store.ArtifactContent | store.ArtifactRevision | store.ArtifactOrigin
	if input == nil || input.SameMaterial(d.cfg.Input, material) || input.SameMaterial(d.bootstrapInput, material) {
		return d.bootstrapInput
	}
	if binding.object != nil && len(binding.object.Composition().Includes) != 0 &&
		input.Content != "" && input.Reads.Valid() {
		return d.bootstrapInput
	}
	d.bootstrapInput = input
	return input
}

func (d *Session) admitConstructionInput(ctx context.Context) (*store.SourceArtifact, error) {
	if d.cfg.Entry != nil && d.cfg.Entry.Input == d.cfg.Input {
		if input := d.cfg.Entry.prepared.sources[manifest.BootstrapSourceID].input; input != nil {
			d.cfg.Input = input
		}
	}
	return d.admitBootstrapInput(ctx)
}

func (d *Session) bootstrapDeclaration(ctx context.Context, input *store.SourceArtifact) (*entrySourceGroup, error) {
	id := manifest.BootstrapSourceID
	group := &entrySourceGroup{required: true}
	if supplied := d.cfg.Store.GetDeclaration(id); supplied != nil {
		if len(supplied.Composition().Includes) == 0 {
			return group, nil
		}
		group.object = supplied
	}
	if previous := d.resolvedSources.snapshot()[id]; previous.input == input && previous.object != nil {
		if !previous.synthetic {
			group.object, group.projected = previous.object, previous.projected
		}
		return group, nil
	}
	index := d.loader
	previousOpts, previousIgnore := index.Options, index.IgnoreFile
	index.Options.Cache, index.Options.Changes = nil, nil
	index.Options.ParsedInputs = nil
	defer func() {
		index.Options, index.IgnoreFile = previousOpts, previousIgnore
	}()
	index.SourceRoot, index.IgnoreFile = input.LocalPath, d.cfg.KRMIgnoreFile
	work := d.cfg.Work.Record(manifest.WorkPreparation, d.cfg.Path)
	relative, err := filepath.Rel(input.LocalPath, d.cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("bootstrap path: %w", err)
	}
	if _, statErr := fs.Stat(input.FS, filepath.ToSlash(relative)); errors.Is(statErr, fs.ErrNotExist) {
		return group, nil
	}
	index.StageProvisionalInput(input)
	_, err = index.Load(ctx, d.cfg.Path, manifest.NamedResource{}, work)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err := d.retainBootstrapProbe(ctx, group, index); err != nil {
		return nil, err
	}
	return group, nil
}

func (d *Session) retainBootstrapProbe(ctx context.Context, group *entrySourceGroup, index *loader.Loader) error {
	if group.object == nil {
		if err := group.indexBootstrap(ctx, index); err != nil {
			return err
		}
	}
	if group.object != nil && len(group.object.Composition().Includes) != 0 {
		if err := d.bootstrapIncludes(ctx, index, group.object); err != nil {
			return err
		}
	}
	return nil
}

func (g *entrySourceGroup) indexBootstrap(ctx context.Context, index *loader.Loader) error {
	for child, err := range index.Sources(ctx) {
		if err != nil {
			return err
		}
		if child.Obj.Named() == manifest.BootstrapSourceID {
			g.object, g.document = child.Obj, child.Doc
			break
		}
	}
	return nil
}

func (d *Session) bootstrapIncludes(ctx context.Context, index *loader.Loader, root manifest.BaseManifest) error {
	index.IgnoreFile = ""
	if _, err := index.Load(ctx, index.SourceRoot, manifest.NamedResource{}, nil); err != nil {
		return err
	}
	sources := map[manifest.NamedResource]store.Child{}
	for child, err := range index.Sources(ctx) {
		if err != nil {
			return err
		}
		sources[child.Obj.Named()] = child
	}
	queue := []manifest.BaseManifest{root}
	visited := map[manifest.NamedResource]bool{root.Named(): true}
	for len(queue) > 0 {
		source := queue[0]
		queue = queue[1:]
		includes := source.Composition().Includes
		for i := range includes {
			id := includes[i].Reference.NamedResource
			if visited[id] {
				continue
			}
			visited[id] = true
			if existing := d.cfg.Store.GetDeclaration(id); existing != nil {
				queue = append(queue, existing)
				continue
			}
			if child, found := sources[id]; found {
				d.cfg.Store.AddConstructionDeclaration(child.Obj, child.Doc)
				queue = append(queue, child.Obj)
			}
		}
	}
	return nil
}
