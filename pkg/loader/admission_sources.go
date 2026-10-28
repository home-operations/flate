package loader

import (
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (c *admissionClosure) sources(id manifest.NamedResource) iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		record := c.record(id)
		if len(record.ContentRoots) != 0 && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) {
			for source := range record.ContentRoots {
				if !yield(source) {
					return
				}
			}
			return
		}
		for ref := range manifest.References(c.object(id), manifest.ReferenceRelations) {
			if ref.Use == "source" {
				yield(ref.NamedResource)
				return
			}
		}
		for _, ref := range record.Refs {
			if ref.KindInfo().SourceArtifact {
				yield(ref)
				return
			}
		}
		yield(manifest.BootstrapSourceID)
	}
}

func (c *admissionClosure) sourceFiles(source manifest.NamedResource) iter.Seq[string] {
	return func(yield func(string) bool) {
		delta, input, paired := c.delta(source)
		if delta == nil {
			return
		}
		for _, file := range delta.Paths() {
			if paired {
				_, file = c.coordinates(manifest.Production{Source: source, File: file, Path: filepath.Join(input.LocalPath, filepath.FromSlash(file))})
			}
			if !yield(file) {
				return
			}
		}
	}
}

func (c *admissionClosure) changedClaim(reader manifest.FileReader, file string) bool {
	delta, input, paired := c.delta(reader.Source)
	if delta == nil {
		return false
	}
	record := c.record(reader.Reader)
	changed := paired && c.fileChanged(reader.Source, file) || !paired && !record.Pinned && delta.Contains(file)
	if !changed {
		return false
	}
	for actual := range c.loader.Index.ReadersOfFile(file) {
		if actual.Reader == reader.Reader {
			qualified, _, _ := c.delta(actual.Source)
			if qualified == delta {
				return true
			}
		}
	}
	if roots, known := record.ContentRoots[reader.Source]; known {
		if input != nil {
			relative, err := filepath.Rel(input.OriginPath(input.LocalPath), filepath.Join(c.loader.SourceRoot, filepath.FromSlash(file)))
			if err != nil {
				return false
			}
			file = filepath.ToSlash(relative)
		}
		return slices.ContainsFunc(roots, func(root string) bool {
			return root == "." || root == "" || manifest.HasEnvsubstReference(root) || file == root || strings.HasPrefix(file, strings.TrimSuffix(root, "/")+"/")
		})
	}
	return true
}

func (c *admissionClosure) changedInput(consumer, dependency manifest.NamedResource) bool {
	for source := range c.loader.Options.Changes.Sources() {
		delta, input, paired := c.delta(source)
		if delta == nil {
			continue
		}
		proofs := c.productions(dependency)
		if len(proofs) != 0 && !slices.ContainsFunc(proofs, func(proof manifest.Production) bool {
			qualified, _, _ := c.delta(proof.Source)
			return qualified == delta
		}) {
			continue
		}
		for id, changed := range delta.Inputs() {
			if id.Group != dependency.Group || id.Kind != dependency.Kind || id.Name != dependency.Name || id.Namespace != "" && id.Namespace != dependency.Namespace {
				continue
			}
			file := changed.File
			if occurrence, located := c.loader.occurrence(dependency); changed.Unbound && (located || len(c.loader.Index.Claims) != 0 || len(c.loader.Index.Readers) != 0) {
				indexed := file
				if paired && input != nil {
					_, indexed = c.coordinates(manifest.Production{Source: source, File: file, Path: filepath.Join(input.LocalPath, filepath.FromSlash(file))})
				}
				if located {
					if indexed != c.loader.fileCoordinate(filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))) {
						continue
					}
				} else {
					owners, _ := c.loader.Index.OwnersOfFile(indexed)
					if len(owners) != 0 && !slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool {
						return c.changedClaim(manifest.FileReader{Reader: owner, Source: source}, indexed)
					}) {
						continue
					}
				}
			}
			if !paired {
				if input == nil {
					continue
				}
				origin := input.OriginPath(input.LocalPath)
				base, err := filepath.Rel(c.loader.SourceRoot, origin)
				base = filepath.ToSlash(base)
				if err != nil || base != "." && file != base && !strings.HasPrefix(file, base+"/") {
					continue
				}
				_, origins, _ := delta.ForSource(manifest.BootstrapSourceID)
				local := false
				for _, origin := range origins {
					local = local || origin != nil && (origin.LocalPath == c.loader.SourceRoot || origin.LocalRoot == c.loader.SourceRoot)
				}
				if (origins[0] != nil || origins[1] != nil) && !local {
					continue
				}
			}
			if id.Namespace != "" {
				return true
			}
			for readerSource := range c.sources(consumer) {
				if readerSource == source {
					return true
				}
				if source == manifest.BootstrapSourceID {
					_, artifact, paired := c.delta(readerSource)
					if !paired && artifact != nil && artifact.OriginPath(artifact.LocalPath) == c.loader.SourceRoot {
						return true
					}
				}
			}
		}
	}
	return false
}

func (c *admissionClosure) coordinates(p manifest.Production) (relative, indexed string) {
	relative, indexed = p.File, p.File
	if filepath.IsAbs(p.Path) {
		art, _ := c.objects.GetArtifact(p.Source, store.ArtifactContent).(*store.SourceArtifact)
		root := c.loader.SourceRoot
		if art != nil {
			root = art.LocalPath
		}
		rel, err := filepath.Rel(root, p.Path)
		if err != nil {
			return "", ""
		}
		relative = filepath.ToSlash(rel)
		rel, err = filepath.Rel(c.loader.SourceRoot, art.OriginPath(p.Path))
		if err != nil {
			return "", ""
		}
		indexed = filepath.ToSlash(rel)
	}
	return relative, indexed
}

func (c *admissionClosure) changedProduction(p manifest.Production) bool {
	if p.File == "" {
		return false
	}
	if p.Owner == (manifest.NamedResource{}) && p.Source == manifest.BootstrapSourceID {
		if record, _ := c.loader.Index.Lookup(p.Authored); record.Path != "" && record.Path != p.Path {
			return false
		}
	}
	delta, _, paired := c.delta(p.Source)
	if delta == nil {
		return false
	}
	relative, indexed := c.coordinates(p)
	if paired {
		return delta.Contains(relative)
	}
	return delta.Contains(indexed)
}

func (c *admissionClosure) reverse(id manifest.NamedResource, object manifest.BaseManifest, ref manifest.Reference) bool {
	if object != nil {
		for input := range manifest.ContentInputs(object, true) {
			if input == ref.NamedResource {
				return true
			}
		}
		return false
	}
	if occurrence, ok := c.loader.occurrence(id); ok {
		for _, original := range occurrence.facts.refs {
			if !slices.Contains(occurrence.facts.reverse, original.NamedResource) {
				continue
			}
			input := original.NamedResource
			if original.NamespaceDefault {
				input.Namespace = id.Namespace
			}
			if input == ref.NamedResource {
				return true
			}
		}
		return false
	}
	return ref.ContentInput()
}
