package loader

import (
	"context"
	"maps"
	"path/filepath"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// EntryFrontier must conservatively retain packages whose applications cannot be ruled out.
func (l *Loader) EntryFrontier(ctx context.Context, input *store.SourceArtifact, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	if input == nil || input.FS == nil || !input.Reads.Valid() {
		return true, nil
	}
	l.refreshSources()
	mounted := maps.Clone(l.documents.artifacts)
	if mounted == nil {
		mounted = map[string]*store.SourceArtifact{}
	}
	mounted[input.LocalPath] = input
	l.documents.setSources(mounted)
	reader := l.documents
	reader.intent = manifest.ApplicationDocuments
	dir := filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(path))
	if !safepath.Contains(input.LocalPath, dir) {
		return true, nil
	}
	body := reader.read(dir).body
	if reader.cache[dir].pathState == pathMissing {
		return false, nil
	}
	if body == nil {
		return true, nil
	}
	possible := false
	walk := traversal{documents: reader, policy: traversalScope}
	walk.walk(
		ctx,
		graphVisit{root: input.LocalPath, dir: manifest.NormalizeClaimBase(path), role: kustomizeinput.Resource},
		func(v graphVisit) bool {
			possible = reader.frontierVisit(ctx, &v)
			return !possible
		},
	)
	return possible, ctx.Err() //nolint:wrapcheck // Cancellation preserves the caller context error identity.
}

func (r *documentReader) frontierVisit(ctx context.Context, visit *graphVisit) bool {
	if visit.role == kustomizeinput.File {
		return false
	}
	if visit.incomplete != "" || !visit.readable {
		return true
	}
	if visit.document != nil {
		return packageMayApply(visit)
	}
	if visit.role != kustomizeinput.Resource {
		return false
	}
	objects, err := r.censusFile(ctx, visit.file, manifest.ParseDocOptions{})
	if err != nil {
		return true
	}
	if r.cache[visit.file].possible {
		return true
	}
	for i := range objects {
		if parsedApplication(&objects[i]) {
			return true
		}
	}
	return false
}

func packageMayApply(visit *graphVisit) bool {
	body := visit.document
	if !kustomizeinput.PreservesOperands(body) || len(body.Generators) != 0 || len(body.HelmCharts) != 0 {
		return true
	}
	for ref := range kustomizeinput.Kustomization(body) {
		if ref.Inline || ref.Role == kustomizeinput.File {
			continue
		}
		if _, local := kustomizeinput.Local(visit.root, visit.dir, ref.Path); !local {
			return true
		}
	}
	return false
}
