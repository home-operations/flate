package loader

import (
	"context"
	"fmt"
	"iter"
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
	item     int
}

// Occurrence retains an immutable declaration and its contextual resource identity.
type Occurrence struct {
	Declaration OccurrenceID
	Identity    manifest.NamedResource
	Authored    manifest.NamedResource
	object      manifest.BaseManifest
	facts       *occurrenceFacts
}

type producerFact struct {
	identity manifest.NamedResource
	facts    *occurrenceFacts
}

type occurrenceFacts struct {
	source                         manifest.NamedResource
	path                           string
	hasPath, namespaced, uncertain bool
	tracked                        bool
	structural                     *manifest.Kustomization
	catalog                        manifest.BaseManifest
	reverse                        []manifest.NamedResource
	refs                           []manifest.Reference
	templateInputs                 []manifest.TemplateInput
	templateObjects                []manifest.BaseManifest
	roots                          []string
	targets, authoredTargets       []manifest.ProducerTarget
	needsCandidates                bool
	placeholder                    bool
	policy                         *manifest.EmissionPolicy
	labels                         map[string]string
	labeled                        bool
}

// SourcePath returns the source and path in the occurrence's effective namespace.
func (o Occurrence) SourcePath() (manifest.NamedResource, string, bool) {
	if o.facts == nil {
		obj := o.object
		if obj == nil {
			return manifest.NamedResource{}, "", false
		}
		if o.Identity.Namespace != o.Authored.Namespace {
			obj = obj.WithNamespace(o.Identity.Namespace)
		}
		return obj.SourcePath()
	}
	return manifest.Resolve(o.facts.source, "", o.Identity.Namespace), o.facts.path, o.facts.hasPath
}

func compactOccurrence(obj manifest.BaseManifest, projection manifest.TemplateProjection) Occurrence {
	source, path, hasPath := obj.SourcePath()
	targets := manifest.ProducerTargets(obj)
	facts := &occurrenceFacts{source: source, path: path, hasPath: hasPath,
		namespaced: manifest.KindOf(obj).Namespaced(), tracked: manifest.KindOf(obj).Tracked,
		targets: targets, authoredTargets: targets, needsCandidates: manifest.ProducerNeedsCandidates(obj), roots: slices.Collect(manifest.RenderRoots(obj))}

	facts.templateInputs = projection.Inputs
	facts.templateObjects = projection.Objects
	facts.targets = append(facts.targets, projection.Targets...)

	generated, _ := manifest.GeneratedObjects(obj)
	for _, child := range generated {
		facts.targets = append(facts.targets, manifest.ProducerTarget{NamedResource: child.Named()})
	}

	facts.catalog = manifest.CatalogObject(obj, obj.Named())
	facts.structural, _ = facts.catalog.(*manifest.Kustomization)

	if data, ok := obj.(interface{ HasPlaceholder() bool }); ok {
		facts.placeholder = data.HasPlaceholder()
	}
	if producer, ok := obj.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok {
		policy := producer.EmissionPolicy()
		facts.policy = &manifest.EmissionPolicy{Namespace: policy.Namespace, InternalNamespace: policy.InternalNamespace, ExcludeChildren: policy.ExcludeChildren}
	}
	if labeled, ok := facts.catalog.(interface{ GetLabels() map[string]string }); ok {
		facts.labels, facts.labeled = labeled.GetLabels(), true
	}
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		facts.refs = append(facts.refs, ref.Clone())
		if ref.AffectedInput() {
			facts.reverse = append(facts.reverse, ref.NamedResource)
		}
	}
	id := obj.Named()
	if manifest.HasEnvsubstReference(id.Name) {
		id.Name, facts.uncertain = "", true
	}
	if manifest.HasEnvsubstReference(id.Namespace) {
		id.Namespace, facts.uncertain = "", true
	}
	return Occurrence{Identity: id, Authored: id, facts: facts}
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
		if !o.Identity.KindInfo().Structural {
			continue
		}
		var ks *manifest.Kustomization
		if o.facts != nil {
			ks = o.facts.structural
		} else {
			ks = o.object.(*manifest.Kustomization)
		}
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
func (l *Loader) PathObjects() []Occurrence {
	stored := l.Store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)
	objects := make([]Occurrence, 0, len(stored))
	l.eachKustomization(stored, func(child store.Child) {
		if !l.inactiveKustomization(child.Obj.Named()) {
			objects = append(objects, Occurrence{Identity: child.Obj.Named(), Authored: child.Obj.Named(), object: child.Obj})
		}
	})
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
				if child.outcome.Err != nil || child.Obj == nil && child.fact == nil {
					continue
				}
				var o Occurrence
				if child.fact != nil {
					o = *child.fact
				} else {
					o = Occurrence{Identity: child.Obj.Named(), Authored: child.Obj.Named(), object: child.Obj}
				}
				source, path, ok := o.SourcePath()
				if !ok || o.facts != nil && o.facts.uncertain {
					continue
				}
				if slices.ContainsFunc(objects, func(other Occurrence) bool {
					otherSource, otherPath, _ := other.SourcePath()
					return other.Identity == o.Identity && otherSource == source && otherPath == path
				}) {
					continue
				}
				objects = append(objects, o)
			}
		}
	}
	slices.SortStableFunc(objects, func(a, b Occurrence) int {
		if c := a.Identity.Compare(b.Identity); c != 0 {
			return c
		}
		sa, pa, _ := a.SourcePath()
		sb, pb, _ := b.SourcePath()
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
			if child.outcome.Err != nil || child.Obj == nil && child.fact == nil {
				continue
			}
			var o Occurrence
			var namespaced bool
			if child.fact != nil {
				o, namespaced = *child.fact, child.fact.facts.namespaced
				if o.facts.uncertain {
					continue
				}
			} else {
				o = Occurrence{Declaration: OccurrenceID{Source: source, File: filepath.ToSlash(file), Document: child.outcome.Ordinal}, Authored: child.Obj.Named(), Identity: child.Obj.Named(), object: child.Obj}
				namespaced = manifest.KindOf(child.Obj).Namespaced()
			}
			id, err := l.projectIdentity(o.Authored, v, child.version, namespaced)
			if err != nil {
				return nil, err
			}
			o.Identity = id
			out = append(out, o)
		}
	}
	return out, nil
}

type identityProjection struct {
	identity                           manifest.NamedResource
	version, namespace, prefix, suffix string
}

func (l *Loader) projectIdentity(id manifest.NamedResource, v graphVisit, version string, namespaced bool) (manifest.NamedResource, error) {
	projection := 0
	if namespaced {
		id = manifest.Resolve(id, v.namespace, v.fallback)
		projection = 1
	}
	if v.namePrefix == "" && v.nameSuffix == "" && (v.namespace == "" || namespaced) {
		return id, nil
	}
	l.ensureScopes()
	key := identityProjection{identity: id, version: version, namespace: v.namespace, prefix: v.namePrefix, suffix: v.nameSuffix}
	if projected, ok := l.scopes.projections[projection][key]; ok {
		return projected, nil
	}
	doc := map[string]any{"apiVersion": version, "kind": id.Kind, "metadata": map[string]any{"name": id.Name}}
	r, err := resource.NewFactory(nil).FromMap(doc)
	if err != nil {
		return id, manifest.NewError(manifest.ErrFlux, fmt.Errorf("project identity: %w", err), "")
	}
	if v.namespace != "" {
		ns := namespace.Filter{Namespace: v.namespace, FsSlice: types.FsSlice{{Version: "v1", Kind: "Namespace", Path: types.MetadataNamePath}}}
		if err := r.ApplyFilter(ns); err != nil {
			return id, manifest.NewError(manifest.ErrFlux, fmt.Errorf("project namespace: %w", err), "")
		}
	}
	resources := resmap.New()
	if err := resources.Append(r); err != nil {
		return id, manifest.NewError(manifest.ErrFlux, fmt.Errorf("project identity: %w", err), "")
	}
	fields := types.FsSlice{{Path: "metadata/name"}}
	p := builtins.PrefixTransformerPlugin{Prefix: v.namePrefix, FieldSpecs: fields}
	if err := p.Transform(resources); err != nil {
		return id, manifest.NewError(manifest.ErrFlux, fmt.Errorf("project prefix: %w", err), "")
	}
	s := builtins.SuffixTransformerPlugin{Suffix: v.nameSuffix, FieldSpecs: fields}
	if err := s.Transform(resources); err != nil {
		return id, manifest.NewError(manifest.ErrFlux, fmt.Errorf("project suffix: %w", err), "")
	}
	id.Name = r.GetName()
	if l.scopes.projections[projection] == nil {
		l.scopes.projections[projection] = make(map[identityProjection]manifest.NamedResource)
	}
	l.scopes.projections[projection][key] = id
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
			if child.outcome.Err != nil || child.Obj == nil && child.fact == nil {
				continue
			}
			if child.fact != nil && child.fact.Declaration == occurrence.Declaration || child.fact == nil && child.Obj.Named() == record.Authored && child.outcome.Ordinal == occurrence.Declaration.Document {
				return l.AdmitsRoot(l.CatalogObject(id), record.Path)
			}
		}
	}
	return false
}

// EntryFailure assesses retained failures admitted as unowned native entry resources.
func (l *Loader) EntryFailure(ctx context.Context, repoRoot string) error {
	if !l.Options.Selection.Matches(manifest.NamedResource{}) {
		return nil
	}
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
		if l.Options.Selection.Changed && l.Options.Changes != nil {
			evidence := admissionClosure{loader: l, objects: l.Store}
			if !evidence.fileChanged(manifest.BootstrapSourceID, visit.coordinate) {
				continue
			}
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
				return fmt.Errorf("entry %s: %w (invalid YAML)", l.fileCoordinate(l.documents.localPath(file)), manifest.DocumentFailure([]manifest.DocOccurrence{child.outcome}))
			}
		}
	}
	return nil
}

func (l *Loader) exactObject(ctx context.Context, occurrence Occurrence, input *store.SourceArtifact) (store.Child, error) {
	if err := ctx.Err(); err != nil {
		return store.Child{}, err
	}
	reader := l.documents
	reader.cache, reader.parsed = nil, nil
	reader.coordinate = &occurrence.Declaration
	if input != nil {
		if input.LocalPath != occurrence.Declaration.Source {
			return store.Child{}, nil
		}
		reader.views = nil
		reader.artifacts = nil
		reader.setSources(map[string]*store.SourceArtifact{input.LocalPath: input})
	}
	file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
	parsed, err := reader.fileObjects(file, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
	if err != nil {
		return store.Child{}, manifest.NewError(manifest.ErrInput, fmt.Errorf("hydrate %s: %w", occurrence.Identity, err), "")
	}
	if err := ctx.Err(); err != nil {
		return store.Child{}, err
	}
	var children []store.Child
	for _, child := range parsed {
		if child.outcome.Err != nil {
			return store.Child{}, manifest.NewError(manifest.ErrInput, fmt.Errorf("hydrate %s: %w", occurrence.Identity, manifest.DocumentFailure([]manifest.DocOccurrence{child.outcome})), "")
		}
		if child.Obj != nil && manifest.KindOf(child.Obj).Tracked {
			children = append(children, child.Child)
		}
	}
	if len(children) != 1 || children[0].Obj.Named() != occurrence.Authored {
		return store.Child{}, nil
	}
	child := children[0]
	if child.Obj.Named().Namespace != occurrence.Identity.Namespace {
		child.Obj = child.Obj.WithNamespace(occurrence.Identity.Namespace)
	}
	return child, nil
}

func (o Occurrence) references() iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		for _, ref := range o.facts.refs {
			if ref.NamespaceDefault {
				ref.Namespace = o.Identity.Namespace
			}
			if !yield(ref) {
				return
			}
		}
	}
}

func (l *Loader) occurrence(id manifest.NamedResource) (Occurrence, bool) {
	if o, ok := l.locations[id]; ok {
		return o, true
	}
	if id.Namespace != "" {
		bare := id
		bare.Namespace = ""
		if o, ok := l.locations[bare]; ok && o.facts != nil && o.facts.namespaced && o.Authored.Namespace == "" {
			o.Identity = id
			return o, true
		}
	}
	return Occurrence{}, false
}

// IsFileIndexed reports whether source input supplies an authoritative declaration for id.
func (l *Loader) IsFileIndexed(id manifest.NamedResource) bool {
	if l.Index.IsFileIndexed(id) {
		return true
	}
	if record, ok := l.Index.Lookup(id); ok && record.Builder != "" && l.candidates[id].Obj != nil {
		return true
	}
	if occurrence, ok := l.occurrence(id); ok && occurrence.Authored.Namespace == "" && id.Namespace != "" {
		id.Namespace = ""
		return l.Index.IsFileIndexed(id)
	}
	return false
}

// CatalogObject returns immutable reporting facts without hydrating or admitting the declaration.
func (l *Loader) CatalogObject(id manifest.NamedResource) manifest.BaseManifest {
	if occurrence, ok := l.occurrence(id); ok && occurrence.facts.catalog != nil {
		obj := occurrence.facts.catalog
		if obj.Named() == id {
			return obj
		}
		return manifest.CatalogObject(obj, id)
	}
	record, _ := l.Index.Lookup(id)
	obj := l.IndexedObject(id, record).Obj
	if obj == nil {
		obj = l.Store.GetObject(id)
	}
	if obj == nil {
		return nil
	}
	if snapshot := l.producerFacts.Load(); snapshot != nil {
		if cached, known := (*snapshot)[obj]; known && cached.facts.catalog != nil {
			obj = cached.facts.catalog
			if obj.Named() == id {
				return obj
			}
		}
	}
	return manifest.CatalogObject(obj, id)
}
