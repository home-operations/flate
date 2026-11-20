package loader

import (
	"context"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// EntryFrontier must conservatively retain packages whose applications cannot be ruled out.
func (l *Loader) EntryFrontier(ctx context.Context, input *store.SourceArtifact, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if input == nil || input.FS == nil || !input.Reads.Valid() {
		return true, nil
	}
	l.refreshSources(input)
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
	return possible, ctx.Err()
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

// EntryPackageReached uses the source's resource graph to establish a descendant entry.
func (l *Loader) EntryPackageReached(
	input *store.SourceArtifact,
	root string,
	ks *manifest.Kustomization,
	entry string,
) bool {
	return packageReached(input, l.packageResources(input, root, ks), entry)
}

func (l *Loader) packageResources(
	input *store.SourceArtifact, root string, ks *manifest.Kustomization,
) []graphVisit {
	l.refreshSources(input)
	l.ensureScopes()
	visit := graphVisit{root: input.LocalPath, dir: manifest.NormalizeClaimBase(root), role: kustomizeinput.Resource}
	resources := l.scopeWalk(&visit)
	if ks != nil && len(ks.Components) != 0 {
		return l.scopeComponents(&visit, resources, ks)
	}
	return resources
}

func packageReached(input *store.SourceArtifact, resources []graphVisit, entry string) bool {
	for i := range resources {
		resource := &resources[i]
		if resource.reaches(input, entry) {
			return true
		}
	}
	return false
}

func (v *graphVisit) reaches(input *store.SourceArtifact, entry string) bool {
	return v.role == kustomizeinput.Resource && v.edge == kustomizeinput.Resource && v.readable &&
		safepath.Contains(entry, input.OriginPath(v.file))
}

// EnclosingPackages requires both a resource edge into the request and a declaration in the package.
func (l *Loader) EnclosingPackages(
	ctx context.Context, input *store.SourceArtifact, relative string, kind schema.GroupKind,
) ([]string, error) {
	var dirs []string
	entry := input.OriginPath(filepath.Join(input.LocalPath, relative))
	l.refreshSources(input)
	reader := l.documents
	reader.intent = manifest.IncidentalDocuments
	reader.declarationsOnly, reader.skipDeclarations = true, false
	for dir := relative; dir != "."; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dir = filepath.Dir(dir)
		if reader.read(filepath.Join(input.LocalPath, dir)).body == nil {
			continue
		}
		resources := l.packageResources(input, dir, nil)
		if !packageReached(input, resources, entry) {
			continue
		}
		if reader.packageDeclares(ctx, resources, kind) {
			dirs = append(dirs, filepath.ToSlash(dir))
		}
	}
	return dirs, ctx.Err()
}

func (r *documentReader) packageDeclares(ctx context.Context, resources []graphVisit, kind schema.GroupKind) bool {
	for i := range resources {
		resource := &resources[i]
		if resource.role != kustomizeinput.Resource || !resource.readable || resource.document != nil {
			continue
		}
		declarations, _ := r.completeFile(ctx, resource.file)
		for id := range declarations.Objects {
			if id.Group == kind.Group && id.Kind == kind.Kind {
				return true
			}
		}
	}
	return false
}
