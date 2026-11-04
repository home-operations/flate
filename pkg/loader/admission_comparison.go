package loader

import (
	"cmp"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (c *admissionClosure) delta(source manifest.NamedResource) (*change.Set, *store.SourceArtifact, bool) {
	evidence := c.boundSource(source)
	return evidence.delta, evidence.input, evidence.paired
}

func (c *admissionClosure) boundSource(source manifest.NamedResource) *admissionSource {
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	return c.bindSource(c.sourceEvidence(source))
}

func (c *admissionClosure) bindSource(evidence *admissionSource) *admissionSource {
	if evidence.bound {
		return evidence
	}
	source := evidence.source
	delta, snapshots, declarations := evidence.changes, evidence.snapshots, evidence.declarations
	input, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	declaration := c.objects.GetObject(source)
	if bindings := c.loader.bindings.Load(); bindings != nil {
		cached := (*bindings)[source]
		if cached.bound && cached.comparison == c.loader.Options.Changes && cached.changes == delta &&
			cached.snapshots == snapshots &&
			cached.declarations == declarations &&
			cached.input == input &&
			cached.declaration == declaration &&
			(input == nil || input.Reads.Valid()) {
			*evidence = cached
			return evidence
		}
		evidence.initial = cached.initial
	}
	c.comparisons.changed = true
	evidence.comparison, evidence.declaration = c.loader.Options.Changes, declaration
	evidence.delta, evidence.input, evidence.bound, evidence.paired = delta, input, true, false
	evidence.origin = c.loader.SourceRoot
	if input != nil {
		evidence.origin = input.OriginPath(input.LocalPath)
	}
	evidence.toIndex, _ = filepath.Rel(c.loader.SourceRoot, evidence.origin)
	for side, snapshot := range snapshots {
		if snapshot == nil || input == nil || declaration != declarations[side] {
			continue
		}
		bound := snapshot == input
		if c.loader.frozen {
			bound = c.loader.sideKnown && manifest.TreeSide(side) == c.loader.side && evidence.initial == input &&
				snapshot.Input(
					store.ArtifactContent|store.ArtifactRevision,
				) == input.Input(
					store.ArtifactContent|store.ArtifactRevision,
				)
		}
		if bound && input.Reads.Valid() {
			evidence.paired = true
			return evidence
		}
	}
	if source == manifest.BootstrapSourceID {
		return evidence
	}
	_, pinned, external := sourceView(source, c.loader.SourceRoot, input, declaration)
	if !pinned && !external {
		delta, _, _ = c.loader.Options.Changes.ForSource(manifest.BootstrapSourceID)
		evidence.delta = delta
		return evidence
	}
	evidence.delta = nil
	return evidence
}

func (c *admissionClosure) fileChanged(source manifest.NamedResource, file string) bool {
	evidence := c.boundSource(source)
	delta := evidence.delta
	if delta == nil {
		return false
	}
	if evidence.paired && evidence.input != nil {
		if evidence.toIndex == "." {
			return delta.Contains(filepath.ToSlash(filepath.Clean(file)))
		}
		relative, err := filepath.Rel(evidence.origin, filepath.Join(c.loader.SourceRoot, filepath.FromSlash(file)))
		return err == nil && delta.Contains(filepath.ToSlash(relative))
	}
	return delta.Contains(file)
}

func (c *admissionClosure) inputsChanged(source manifest.NamedResource, inputs []string) bool {
	if len(inputs) == 0 {
		return false
	}
	for file := range c.sourceFiles(source) {
		if slices.Contains(inputs, file) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) outputChanged(
	id manifest.NamedResource,
	refValue *manifest.Reference,
	obj manifest.BaseManifest,
) (bool, bool) {
	ref := *refValue

	source := c.source(id)
	_, _, paired := c.delta(source)
	if !paired || !c.loader.sideKnown {
		return false, false
	}
	record, _ := c.loader.Index.Lookup(id)
	if prepared := record.Declaration; prepared != nil && obj != prepared && record.Builder != "" &&
		manifest.Equal(obj, prepared) {
		child := c.loader.preparedObject(id)
		var doc map[string]any
		if c.incoming.Obj != nil && c.incoming.Obj.Named() == id && c.incoming.Doc != nil {
			doc = c.incoming.Doc
		} else if c.objects.GetDeclaration(id) == obj {
			_, current, output := c.objects.GetEmission(id)
			defer output.Finish()
			doc = current
		}
		if child.Obj == prepared && manifest.ValueEqual(doc, child.Doc) {
			obj = prepared
		}
	}
	if ref.Use != "" && ref.Name != "" {
		input := c.object(ref.NamedResource)
		if input == nil {
			return false, false
		}
		if _, known := c.outputChanged(
			ref.NamedResource,
			new(manifest.Reference{NamedResource: ref.NamedResource}),
			input,
		); !known {
			return false, false
		}
	}
	return c.loader.Options.Changes.OutputChanged(c.loader.side, source, ref, obj)
}

func (c *admissionClosure) declarationChanged(id manifest.NamedResource, object manifest.BaseManifest) (bool, bool) {
	var document map[string]any
	if c.loader.sideKnown && object != nil {
		if c.incoming.Obj == object {
			document = c.incoming.Doc
		} else if c.objects.GetDeclaration(id) == object {
			_, current, output := c.objects.GetEmission(id)
			document = current
			output.Finish()
		} else if record, _ := c.loader.Index.Lookup(id); record.Declaration == object && record.Builder != "" {
			document = c.loader.preparedObject(id).Doc
		}
	}
	return c.loader.Options.Changes.DeclarationChanged(c.loader.side, c.source(id), id, document)
}

func (c *admissionClosure) affected(id manifest.NamedResource, obj manifest.BaseManifest) bool {
	if c.loader.Options.Changes == nil {
		return false
	}
	record := c.record(id)
	direct, fileOwned := c.affectedProductions(id, &record)
	if !fileOwned {
		delta, _, _ := c.delta(manifest.BootstrapSourceID)
		direct = direct || delta.Contains(record.File)
	}
	if direct {
		changed, known := c.outputChanged(id, new(manifest.Reference{NamedResource: id}), obj)
		if !known {
			changed, known = c.declarationChanged(id, obj)
		}
		if !known || changed {
			return true
		}
	}
	if c.inputsChanged(manifest.BootstrapSourceID, record.Inputs) {
		if changed, known := c.outputChanged(id, new(manifest.Reference{NamedResource: id}), obj); !known || changed {
			return true
		}
	}
	roots := c.contentRoots(id, obj, &record)
	for source, roots := range roots {
		if c.affectedSourceRoots(id, source, roots, &record) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) releaseComparisons() {
	if c.comparisons != nil {
		c.publishComparisons()
		clear(c.comparisons.bindings)
		clear(c.comparisons.storage)
		c.comparisons.storage = c.comparisons.storage[:0]
		clear(c.comparisons.names)
		c.comparisons.names = c.comparisons.names[:0]
		admissionSourcePool.Put(c.comparisons)
		c.comparisons = nil
	}
}

func (c *admissionClosure) publishComparisons() {
	if c.comparisons.changed {
		for {
			previous := c.loader.bindings.Load()
			next := make(map[manifest.NamedResource]admissionSource, len(c.comparisons.bindings))
			if previous != nil {
				maps.Copy(next, *previous)
			}
			for source, evidence := range c.comparisons.bindings {
				if evidence.bound {
					next[source] = *evidence
				}
			}
			if c.loader.bindings.CompareAndSwap(previous, &next) {
				break
			}
		}
		c.comparisons.changed = false
	}
}

func (c *admissionClosure) affectedProductions(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) (bool, bool) {
	direct := false
	fileOwned := false
	{
		productionValues := c.productions(id)
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			_, indexed := c.coordinates(new(production))
			fileOwned = fileOwned || indexed == record.File
			if production.Declaration == (manifest.NamedResource{}) &&
				(production.Materializer(id) != (manifest.NamedResource{}) || production.Owner == (manifest.NamedResource{}) || manifest.Resolve(production.Authored, id.Namespace, "") == id) &&
				c.changedProduction(production) {
				direct = true
			}
		}
	}
	return direct, fileOwned
}

func (c *admissionClosure) affectedCoordinates(
	root, file string,
	input *store.SourceArtifact,
	paired bool,
) (string, string, bool) {
	base := cmp.Or(root, ".")
	if !paired && input != nil {
		relative, err := filepath.Rel(
			c.loader.SourceRoot,
			input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(root))),
		)
		if err != nil {
			return "", "", false
		}
		base = filepath.ToSlash(relative)
	}
	if base != "." && file != base && !strings.HasPrefix(file, strings.TrimSuffix(base, "/")+"/") {
		return "", "", false
	}
	prefix, indexed := base, file
	if paired && input != nil {
		physical := input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(root)))
		relative, err := filepath.Rel(c.loader.SourceRoot, physical)
		if err != nil {
			return "", "", false
		}
		prefix = filepath.ToSlash(relative)
		physical = input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(file)))
		relative, err = filepath.Rel(c.loader.SourceRoot, physical)
		if err != nil {
			return "", "", false
		}
		indexed = filepath.ToSlash(relative)
	}
	if prefix == "." {
		prefix = ""
	}
	return prefix, indexed, true
}

func (c *admissionClosure) populateContentRoots(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	roots map[manifest.NamedResource][]string,
) {
	var paths []string
	initialized := false
	for root := range manifest.RenderRoots(obj) {
		if !initialized {
			initialized = true
			for source := range c.sources(id) {
				if _, input, _ := c.delta(source); input != nil && input.FS != nil {
					roots[source] = nil
				}
			}
		}
		if len(roots) != 0 {
			paths = append(paths, root)
		}
	}
	for source := range roots {
		roots[source] = paths
	}
}

func (c *admissionClosure) incomingContentSource(
	obj manifest.BaseManifest,
	record *manifest.DiscoveryRecord,
) manifest.NamedResource {
	source := manifest.BootstrapSourceID
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == sourceReferenceUse {
			source = ref.NamedResource
			break
		}
	}
	if source == manifest.BootstrapSourceID {
		for _, ref := range record.Refs {
			if ref.KindInfo().SourceArtifact {
				source = ref
				break
			}
		}
	}
	return source
}

func (c *admissionClosure) affectedSourceRoots(
	id, source manifest.NamedResource,
	roots []string,
	record *manifest.DiscoveryRecord,
) bool {
	delta, input, paired := c.delta(source)
	if delta == nil || len(roots) == 0 || record.Pinned && !paired {
		return false
	}
	files := delta.Paths()
	for _, root := range roots {
		root = c.affectedRoot(source, root)
		for _, file := range files {
			prefix, indexed, known := c.affectedCoordinates(root, file, input, paired)
			if !known {
				continue
			}
			if c.affectedClaim(id, prefix, indexed) {
				return true
			}
		}
	}
	return false
}

func (c *admissionClosure) affectedRoot(source manifest.NamedResource, root string) string {
	if strings.Contains(root, "/../") || strings.HasSuffix(root, "/..") {
		if resolved, ok := resolveRoot(c.objects, source, root); ok {
			root = resolved
		} else {
			root = "."
		}
	}
	return root
}

func (c *admissionClosure) affectedClaim(id manifest.NamedResource, prefix, indexed string) bool {
	claim := prefix
	if claim != "" {
		claim += "/"
	}
	if slices.Contains(c.loader.Index.Claims[claim], id) {
		owners, _ := c.loader.Index.OwnersOfFile(indexed)
		if !slices.Contains(owners, id) {
			return false
		}
	}
	return true
}

func (c *admissionClosure) contentRoots(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	record *manifest.DiscoveryRecord,
) map[manifest.NamedResource][]string {
	roots := record.ContentRoots
	if roots == nil && obj != nil {
		roots = map[manifest.NamedResource][]string{}
		c.populateContentRoots(id, obj, roots)
	}

	if c.incoming.Obj != nil && c.incoming.Obj.Named() == id && (record.ContentRoots == nil || !id.ReportHidden()) {
		source := c.incomingContentSource(obj, record)
		roots = map[manifest.NamedResource][]string{source: slices.Collect(manifest.RenderRoots(obj))}
	}
	return roots
}
