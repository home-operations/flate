package loader

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/builtins" //nolint:staticcheck // Public builtins preserve upstream prefix/suffix kind exclusions.
	"sigs.k8s.io/kustomize/api/filters/namespace"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/api/resource"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// OccurrenceID identifies an authored document within one admitted source view.
type OccurrenceID struct {
	Source   string
	File     string
	Document int
}

// Occurrence retains an immutable declaration and its contextual resource identity.
type Occurrence struct {
	Declaration OccurrenceID
	Identity    manifest.NamedResource
	Authored    manifest.NamedResource
	Object      manifest.BaseManifest
}

// PackageEvidence borrows parsed package and scan provenance, without deciding root ownership.
type PackageEvidence struct {
	Path        string
	Scans       []string
	Occurrences []Occurrence
}

// OccurrenceClaims derives claims from mounted source views without admitting document identities.
func (l *Loader) OccurrenceClaims(repoRoot string, occurrences []Occurrence) []manifest.KSClaim {
	l.ensureScopes()
	claims := make([]ownerClaims, 0, len(occurrences))
	for _, o := range occurrences {
		if !manifest.KindOf(o.Object).Structural {
			continue
		}
		ks := o.Object.(*manifest.Kustomization)
		if ks.Name != o.Identity.Name || ks.Namespace != o.Identity.Namespace {
			projected := *ks
			projected.Name, projected.Namespace = o.Identity.Name, o.Identity.Namespace
			ks = &projected
		}
		var claimBuf [1]ownerClaims
		built := l.documents.buildClaims(repoRoot, []*manifest.Kustomization{ks}, func(id manifest.NamedResource) sourceScope {
			return l.scopes.sources[l.scopeSource(l.sourceState(id, repoRoot))]
		}, claimBuf[:0])
		claims = append(claims, built[0])
	}
	out, _ := projectClaims(len(claims), func(i int) ownerClaims { return claims[i] })
	return out
}

// PathObjects returns live path witnesses, retaining occurrences until construction completes.
func (l *Loader) PathObjects() []store.Child {
	objects := l.Kustomizations()
	if l.scopes == nil || !l.scopes.complete {
		files := make([]string, 0, len(l.documents.cache))
		for file := range l.documents.cache {
			files = append(files, file)
		}
		slices.Sort(files)
		for _, file := range files {
			doc := l.documents.cache[file]
			if doc.eligible == fileInactive {
				continue
			}
			for _, child := range doc.objects {
				if child.Obj == nil || child.outcome.Err != nil {
					continue
				}
				source, path, ok := child.Obj.SourcePath()
				if !ok {
					continue
				}
				if slices.ContainsFunc(objects, func(other store.Child) bool {
					otherSource, otherPath, _ := other.Obj.SourcePath()
					return other.Obj.Named() == child.Obj.Named() && otherSource == source && otherPath == path
				}) {
					continue
				}
				objects = append(objects, child.Child)
			}
		}
	}
	slices.SortStableFunc(objects, func(a, b store.Child) int {
		if c := a.Obj.Named().Compare(b.Obj.Named()); c != 0 {
			return c
		}
		sa, pa, _ := a.Obj.SourcePath()
		sb, pb, _ := b.Obj.SourcePath()
		if c := sa.Compare(sb); c != 0 {
			return c
		}
		return strings.Compare(pa, pb)
	})
	return objects
}

// Packages snapshots resource graph inputs already traversed by Load.
func (l *Loader) Packages(ctx context.Context) ([]PackageEvidence, error) {
	l.mountSources()
	l.ensureScopes()
	packages := map[string]struct{}{}
	scans := map[string]string{}
	for dir, doc := range l.documents.cache {
		if doc.adHoc && l.documents.mountedRoot(dir) == l.SourceRoot {
			relative, _ := filepath.Rel(l.SourceRoot, dir)
			scans[dir] = filepath.ToSlash(relative)
		}
		if doc.traversed && doc.body != nil && !isKustomizeComponent(doc.body) && l.documents.mountedRoot(dir) == l.SourceRoot {
			packages[dir] = struct{}{}
		}
	}
	covered := map[OccurrenceID]bool{}
	var evidence []PackageEvidence
	for _, dir := range slices.Sorted(maps.Keys(packages)) {
		objects, err := l.Occurrences(ctx, dir, "")
		if err != nil {
			return nil, err
		}
		for _, o := range objects {
			covered[o.Declaration] = true
		}
		relative, _ := filepath.Rel(l.SourceRoot, dir)
		evidence = append(evidence, PackageEvidence{Path: filepath.ToSlash(relative), Occurrences: objects})
	}
	for file, doc := range l.documents.cache {
		if !doc.loaded || len(doc.objects) == 0 || doc.bootstrap || l.documents.mountedRoot(file) != l.SourceRoot {
			continue
		}
		relative, _ := filepath.Rel(l.SourceRoot, file)
		for _, child := range doc.objects {
			if child.Obj != nil && child.outcome.Err == nil && !covered[OccurrenceID{Source: l.SourceRoot, File: filepath.ToSlash(relative), Document: child.outcome.Ordinal}] {
				packages[filepath.Dir(file)] = struct{}{}
				break
			}
		}
	}
	byPath := map[string]PackageEvidence{}
	for _, e := range evidence {
		byPath[filepath.Join(l.SourceRoot, e.Path)] = e
	}
	evidence = nil
	for _, dir := range slices.Sorted(maps.Keys(packages)) {
		e, ok := byPath[dir]
		if !ok {
			objects, err := l.Occurrences(ctx, dir, "")
			if err != nil {
				return nil, err
			}
			relative, _ := filepath.Rel(l.SourceRoot, dir)
			objects = slices.DeleteFunc(objects, func(o Occurrence) bool {
				return o.Declaration.Source != l.SourceRoot || filepath.ToSlash(filepath.Dir(o.Declaration.File)) != filepath.ToSlash(relative)
			})
			e = PackageEvidence{Path: filepath.ToSlash(relative), Occurrences: objects}
		}
		for scan, relative := range scans {
			if safepath.Contains(scan, dir) {
				e.Scans = append(e.Scans, relative)
			}
		}
		slices.Sort(e.Scans)
		evidence = append(evidence, e)
	}
	return evidence, nil
}

// Occurrences projects cached declarations through the existing contextual traversal.
func (l *Loader) Occurrences(ctx context.Context, dir, namespace string) ([]Occurrence, error) {
	l.mountSources()
	l.ensureScopes()
	root := l.documents.mountedRoot(dir)
	if root == "" {
		return nil, nil
	}
	relative, _ := filepath.Rel(root, dir)
	visits := l.scopeWalk(graphVisit{root: root, dir: filepath.ToSlash(relative), namespace: namespace, fallback: namespace, role: kustomizeinput.Resource})
	var out []Occurrence
	for _, v := range visits {
		if v.role != kustomizeinput.Resource || v.document != nil || !v.readable || v.deferDecode {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		doc := l.documents.cache[v.file]
		if doc.eligible == fileInactive {
			continue
		}
		origin := l.documents.localPath(v.file)
		source := l.documents.mountedRoot(origin)
		file, _ := filepath.Rel(source, origin)
		for _, child := range doc.objects {
			if child.Obj == nil || child.outcome.Err != nil {
				continue
			}
			id, err := l.projectIdentity(child.Obj, v, child.version)
			if err != nil {
				return nil, err
			}
			out = append(out, Occurrence{Declaration: OccurrenceID{Source: source, File: filepath.ToSlash(file), Document: child.outcome.Ordinal}, Identity: id, Authored: child.Obj.Named(), Object: child.Obj})
		}
	}
	return out, nil
}

type identityProjection struct {
	identity                           manifest.NamedResource
	version, namespace, prefix, suffix string
}

func (l *Loader) projectIdentity(obj manifest.BaseManifest, v graphVisit, version string) (manifest.NamedResource, error) {
	id := obj.Named()
	namespaced := manifest.KindOf(obj).Namespaced()
	if namespaced {
		id = manifest.Resolve(id, v.namespace, v.fallback)
	}
	if v.namePrefix == "" && v.nameSuffix == "" && (v.namespace == "" || namespaced) {
		return id, nil
	}
	l.ensureScopes()
	key := identityProjection{identity: id, version: version, namespace: v.namespace, prefix: v.namePrefix, suffix: v.nameSuffix}
	if projected, ok := l.scopes.projections[key]; ok {
		return projected, nil
	}
	doc := map[string]any{"apiVersion": version, "kind": id.Kind, "metadata": map[string]any{"name": id.Name}}
	r, err := resource.NewFactory(nil).FromMap(doc)
	if err != nil {
		return id, fmt.Errorf("%w: project identity: %w", manifest.ErrFlux, err)
	}
	if v.namespace != "" {
		ns := namespace.Filter{Namespace: v.namespace, FsSlice: types.FsSlice{{Version: "v1", Kind: "Namespace", Path: types.MetadataNamePath}}}
		if err := r.ApplyFilter(ns); err != nil {
			return id, fmt.Errorf("%w: project namespace: %w", manifest.ErrFlux, err)
		}
	}
	resources := resmap.New()
	if err := resources.Append(r); err != nil {
		return id, fmt.Errorf("%w: project identity: %w", manifest.ErrFlux, err)
	}
	fields := types.FsSlice{{Path: "metadata/name"}}
	p := builtins.PrefixTransformerPlugin{Prefix: v.namePrefix, FieldSpecs: fields}
	if err := p.Transform(resources); err != nil {
		return id, fmt.Errorf("%w: project prefix: %w", manifest.ErrFlux, err)
	}
	s := builtins.SuffixTransformerPlugin{Suffix: v.nameSuffix, FieldSpecs: fields}
	if err := s.Transform(resources); err != nil {
		return id, fmt.Errorf("%w: project suffix: %w", manifest.ErrFlux, err)
	}
	id.Name = r.GetName()
	if l.scopes.projections == nil {
		l.scopes.projections = make(map[identityProjection]manifest.NamedResource)
	}
	l.scopes.projections[key] = id
	return id, nil
}

// AdmitsOccurrence applies native root admission in the retained source view.
func (l *Loader) AdmitsOccurrence(occurrence Occurrence) bool {
	for id, record := range l.Index.Files() {
		if record.Authored != occurrence.Authored {
			continue
		}
		origin := l.documents.localPath(record.Path)
		if origin != filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File)) {
			continue
		}
		doc := l.documents.cache[record.Path]
		for _, child := range doc.objects {
			if child.Obj == nil || child.outcome.Err != nil {
				continue
			}
			if child.Obj.Named() == record.Authored && child.outcome.Ordinal == occurrence.Declaration.Document {
				return l.AdmitsRoot(l.IndexedObject(id, record).Obj, record.Path)
			}
		}
	}
	return false
}

// EntryFailure assesses retained failures admitted as unowned native entry resources.
func (l *Loader) EntryFailure(ctx context.Context, repoRoot string) error {
	l.Admit(repoRoot)
	relative, _ := filepath.Rel(repoRoot, l.entryPath)
	var files []string
	for _, visit := range l.scopeWalk(graphVisit{root: repoRoot, dir: filepath.ToSlash(relative), role: kustomizeinput.Resource, entryResources: true}) {
		if visit.role != kustomizeinput.Resource || visit.document != nil || !visit.readable || visit.deferDecode {
			continue
		}
		doc := l.documents.cache[visit.file]
		if doc.selected {
			continue
		}
		admission := l.rootAdmission(visit.file, visit.coordinate, false)
		if !admission.denied && len(admission.owners) == 0 {
			files = append(files, visit.file)
		}
	}
	slices.Sort(files)
	for _, file := range slices.Compact(files) {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, child := range l.documents.cache[file].objects {
			if child.outcome.Err != nil {
				return fmt.Errorf("entry %s: %w", l.fileCoordinate(l.documents.localPath(file)), manifest.DocumentFailure([]manifest.DocOccurrence{child.outcome}))
			}
		}
	}
	return nil
}
