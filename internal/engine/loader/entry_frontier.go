package loader

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// EntryFrontier must conservatively retain packages whose applications cannot be ruled out.
func (l *Loader) EntryFrontier(
	ctx context.Context, input *store.SourceArtifact, path string, projected func(manifest.BaseManifest) bool,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if input == nil || input.FS == nil || !input.Reads.Valid() {
		return true, nil
	}
	if !l.frozen {
		l.refreshSources(input)
	}
	reader := l.documents
	mounted := reader.artifacts[input.LocalPath]
	if !input.SameMaterial(mounted, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin) {
		// A qualification probe MUST NOT invalidate frozen ownership evidence.
		reader.cache = map[string]document{}
		reader.views, reader.artifacts = nil, nil
		reader.setSources(map[string]*store.SourceArtifact{input.LocalPath: input})
	}
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
	walk := traversal{documents: reader, policy: traversalScope, qualify: true}
	walk.walk(
		ctx,
		graphVisit{root: input.LocalPath, dir: manifest.NormalizeClaimBase(path), role: kustomizeinput.Resource},
		func(v graphVisit) bool {
			possible = reader.frontierVisit(ctx, &v, projected)
			return !possible
		},
	)
	return possible, ctx.Err()
}

// EntryProducerHint MUST affect ordering only. Its view includes declarations awaiting a build.
func (l *Loader) EntryProducerHint(ctx context.Context, input *store.SourceArtifact, path string) (bool, error) {
	visits := l.packageResources(input, path, nil)
	reader := l.documents
	reader.intent = manifest.IncidentalDocuments
	reader.compact = false
	reader.parsed = nil
	reader.cache = make(map[string]document)
	for i := range visits {
		visit := &visits[i]
		readable := visit.readable || visit.deferDecode && safepath.Contains(visit.root, visit.file)
		if visit.role != kustomizeinput.Resource || visit.document != nil || !readable {
			continue
		}
		children, readErr := reader.fileObjects(ctx, visit.file, manifest.ParseDocOptions{})
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if readErr != nil {
			continue
		}
		for i := range children {
			obj := children[i].Obj
			kind := manifest.KindOf(obj)
			if !kind.Render || kind.Structural {
				continue
			}
			_, hint := manifest.ProjectProducer(obj, func(func([]byte) bool) {})
			if hint {
				return true, nil
			}
		}
	}
	return false, ctx.Err()
}

func (r *documentReader) frontierVisit(
	ctx context.Context, visit *graphVisit, projected func(manifest.BaseManifest) bool,
) bool {
	if visit.role == kustomizeinput.File {
		return false
	}
	if visit.requiresBuild || visit.incomplete != "" || !visit.readable {
		return true
	}
	if visit.document != nil {
		return false
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
		child := &objects[i]
		if parsedApplication(child) || projected != nil && projected(parsedFactObject(child)) {
			return true
		}
	}
	return false
}

// EntryPackageReached uses the source's resource graph to establish a descendant entry.
func (l *Loader) EntryPackageReached(
	input *store.SourceArtifact,
	root string,
	owner *manifest.Kustomization,
	entry string,
) bool {
	return packageReached(input, l.packageResources(input, root, owner), entry)
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

// EnclosingPackages retains reached ancestors with a relevant declaration or an opaque recipe.
func (l *Loader) EnclosingPackages(
	ctx context.Context, input *store.SourceArtifact, relative string, target manifest.NamedResource,
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
		builder := reader.read(filepath.Join(input.LocalPath, dir))
		if builder.file == "" {
			continue
		}
		if builder.body == nil {
			dirs = append(dirs, filepath.ToSlash(dir))
			continue
		}
		resources := l.packageResources(input, dir, nil)
		if !packageReached(input, resources, entry) {
			continue
		}
		if reader.packageMayDeclare(ctx, resources, target) {
			dirs = append(dirs, filepath.ToSlash(dir))
		}
	}
	return dirs, ctx.Err()
}

func (r *documentReader) packageMayDeclare(
	ctx context.Context, resources []graphVisit, target manifest.NamedResource,
) bool {
	for i := range resources {
		resource := &resources[i]
		opaque := resource.document != nil &&
			r.cache[filepath.Join(resource.root, resource.dir)].recipe&recipeResourceOnly == 0
		if opaque || resource.requiresBuild || resource.incomplete != "" || !resource.readable {
			return true
		}
		if resource.role != kustomizeinput.Resource || resource.document != nil || target.Name != "" {
			continue
		}
		declarations, _ := r.completeFile(ctx, resource.file)
		for id := range declarations.Objects {
			if id.Group == target.Group && id.Kind == target.Kind {
				return true
			}
		}
	}
	return false
}

// ApplicationPackage holds the authored routing evidence for an enclosing build.
type ApplicationPackage struct {
	Path          string
	Owners        []store.Child
	RequiresBuild bool
}

// ApplicationPackages follows listed resource edges and retains their enclosing build context.
func ApplicationPackages(
	ctx context.Context, input *store.SourceArtifact, root, exclude string,
) ([]store.Child, []ApplicationPackage, error) {
	reader := documentReader{cache: map[string]document{}, intent: manifest.IncidentalDocuments}
	reader.setSources(map[string]*store.SourceArtifact{input.LocalPath: input})
	var excluded string
	if exclude != "" {
		excluded = filepath.Join(input.LocalPath, exclude)
	}
	walk := traversal{documents: reader, policy: traversalScope, qualificationEntry: excluded, qualify: true}
	var standalone []store.Child
	var packages []ApplicationPackage
	indices := map[string]int{}
	walk.walk(
		ctx,
		graphVisit{root: input.LocalPath, dir: root, role: kustomizeinput.Resource},
		func(visit graphVisit) bool {
			if visit.enclosing != "" {
				index, known := indices[visit.enclosing]
				if !known {
					index = len(packages)
					indices[visit.enclosing] = index
					packages = append(packages, ApplicationPackage{Path: visit.enclosing})
				}
				pkg := &packages[index]
				pkg.RequiresBuild = pkg.RequiresBuild || visit.requiresBuild || visit.incomplete != ""
				if pkg.RequiresBuild || visit.document != nil {
					return true
				}
				pkg.Owners = reader.applicationDeclarations(ctx, visit.file, pkg.Owners)
			} else if visit.readable {
				standalone = reader.applicationDeclarations(ctx, visit.file, standalone)
			}
			return true
		},
	)
	packages = slices.DeleteFunc(packages, func(pkg ApplicationPackage) bool {
		return !pkg.RequiresBuild && len(pkg.Owners) == 0
	})
	return standalone, packages, ctx.Err()
}

func (r *documentReader) applicationDeclarations(ctx context.Context, path string, into []store.Child) []store.Child {
	children, _ := r.fileObjects(ctx, path, manifest.ParseDocOptions{})
	for i := range children {
		if kind := manifest.KindOf(children[i].Obj); kind.Runnable && kind.Structural {
			into = append(into, children[i].Child)
		}
	}
	return into
}

// EntryContributorSides projects contributing file occurrences onto the requested path.
func EntryContributorSides(input *store.SourceArtifact, entry string, files []string) []bool {
	sides := make([]bool, len(files))
	for i, file := range files {
		sides[i] = safepath.Contains(entry, input.OriginPath(filepath.Join(input.LocalPath, file)))
	}
	return sides
}
