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
	Declaration   OccurrenceID
	Identity      manifest.NamedResource
	Authored      manifest.NamedResource
	object        manifest.BaseManifest
	facts         *occurrenceFacts
	requiresBuild bool
}

type producerFact struct {
	identity manifest.NamedResource
	facts    *occurrenceFacts
}

type occurrenceFacts struct {
	declared                       string
	source                         manifest.NamedResource
	path                           string
	hasPath, namespaced, uncertain bool
	retained                       bool
	structural                     *manifest.Kustomization
	catalog                        manifest.BaseManifest
	reverse                        []manifest.NamedResource
	refs                           []manifest.Reference
	templateInputs                 []manifest.TemplateInput
	templateObjects                []manifest.BaseManifest
	roots                          []string
	targets, authoredTargets       []manifest.ProducerTarget
	needsCandidates                bool
	policy                         *manifest.EmissionPolicy
	labels                         map[string]string
	labeled                        bool
}

func (f *occurrenceFacts) opaque(targets []manifest.ProducerTarget) bool {
	return f != nil && f.policy != nil && f.structural == nil && !f.hasPath && len(f.authoredTargets) == 0 &&
		len(targets) == 0 &&
		len(f.templateInputs) == 0 &&
		len(f.templateObjects) == 0
}

// SourcePath returns the source and path in the occurrence's effective namespace.
//
//nolint:gocritic // The SDK value receiver preserves a snapshot of the occurrence identity.
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
	facts := &occurrenceFacts{
		source:          source,
		path:            path,
		hasPath:         hasPath,
		namespaced:      manifest.KindOf(obj).Namespaced(),
		retained:        manifest.KindOf(obj).Tracked || hasPath,
		targets:         targets,
		authoredTargets: targets,
		needsCandidates: manifest.ProducerNeedsCandidates(obj),
		roots:           slices.Collect(manifest.RenderRoots(obj)),
	}
	facts.templateInputs = projection.Inputs
	facts.templateObjects = projection.Objects
	facts.targets = append(facts.targets, projection.Targets...)
	generated, _ := manifest.GeneratedObjects(obj)
	for _, child := range generated {
		facts.targets = append(facts.targets, manifest.ProducerTarget{NamedResource: child.Named()})
	}
	facts.catalog = manifest.CatalogObject(obj, obj.Named())
	facts.structural, _ = facts.catalog.(*manifest.Kustomization)
	if producer, ok := obj.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok {
		policy := producer.EmissionPolicy()
		facts.policy = &manifest.EmissionPolicy{
			Namespace:         policy.Namespace,
			InternalNamespace: policy.InternalNamespace,
			ExcludeChildren:   policy.ExcludeChildren,
		}
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

// PackageEvidence retains package provenance and shared root admission.
type PackageEvidence struct {
	Path        string
	Scans       []string
	Occurrences []Occurrence
	Independent bool
}

// OccurrenceClaims derives claims from mounted source views without admitting document identities.
func (l *Loader) OccurrenceClaims(repoRoot string, occurrences []Occurrence) []manifest.KSClaim {
	l.ensureScopes()
	claims := make([]ownerClaims, 0, len(occurrences))
	{
		oValues := occurrences
		for oIndex := range oValues {
			o := oValues[oIndex]
			if !o.Identity.KindInfo().Structural {
				continue
			}
			var ks *manifest.Kustomization
			if o.facts != nil {
				ks = o.facts.structural
			} else {
				//nolint:errcheck // Structural occurrence identity requires the retained Kustomization assertion.
				ks = o.object.(*manifest.Kustomization)
			}
			if ks.Name != o.Identity.Name || ks.Namespace != o.Identity.Namespace {
				projected := *ks
				projected.Name, projected.Namespace = o.Identity.Name, o.Identity.Namespace
				ks = &projected
			}
			var claimBuf [1]ownerClaims
			built := new(l.documents).buildClaims(
				repoRoot,
				[]*manifest.Kustomization{ks},
				func(id manifest.NamedResource) sourceScope {
					return l.scopes.sources[l.scopeSource(new(l.sourceState(id, repoRoot)))]
				},
				claimBuf[:0],
			)
			claims = append(claims, built[0])
		}
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
			objects = append(
				objects,
				Occurrence{Identity: child.Obj.Named(), Authored: child.Obj.Named(), object: child.Obj},
			)
		}
	})
	if l.scopes == nil || !l.scopes.complete {
		objects = l.appendCachedPathObjects(objects)
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
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	l.mountSources()
	l.ensureScopes()
	packages := map[string]struct{}{}
	scans := map[string]string{}
	l.collectPackages(packages, scans)
	covered := map[OccurrenceID]bool{}
	byPath := map[string]PackageEvidence{}
	for _, dir := range slices.Sorted(maps.Keys(packages)) {
		objects, err := l.Occurrences(ctx, dir, "")
		if err != nil {
			return nil, err
		}
		for i := range objects {
			covered[objects[i].Declaration] = true
		}
		relative, _ := filepath.Rel(l.SourceRoot, dir)
		byPath[dir] = PackageEvidence{Path: filepath.ToSlash(relative), Occurrences: objects}
	}
	uncovered, err := l.collectUncoveredPackages(covered)
	if err != nil {
		return nil, err
	}
	for dir, objects := range uncovered {
		if _, native := byPath[dir]; !native {
			relative, _ := filepath.Rel(l.SourceRoot, dir)
			byPath[dir] = PackageEvidence{Path: filepath.ToSlash(relative), Occurrences: objects}
		}
	}
	var evidence []PackageEvidence
	for _, dir := range slices.Sorted(maps.Keys(byPath)) {
		e := byPath[dir]
		for scan, relative := range scans {
			if safepath.Contains(scan, dir) {
				e.Scans = append(e.Scans, relative)
			}
		}
		slices.Sort(e.Scans)
		evidence = append(evidence, e)
	}
	l.classifyPackages(evidence)
	return evidence, nil
}

// Occurrences projects cached declarations through the existing contextual traversal.
func (l *Loader) Occurrences(ctx context.Context, dir, targetNamespace string) ([]Occurrence, error) {
	l.mountSources()
	l.ensureScopes()
	root := new(l.documents).mountedRoot(dir)
	if root == "" {
		return nil, nil
	}
	relative, _ := filepath.Rel(root, dir)
	visit := graphVisit{
		root:      root,
		dir:       filepath.ToSlash(relative),
		namespace: targetNamespace,
		fallback:  targetNamespace,
		role:      kustomizeinput.Resource,
	}
	repoRoot := l.SourceRoot
	if repoRoot == "" {
		repoRoot = root
	}
	location := sourceView(manifest.NamedResource{}, repoRoot, l.documents.artifacts[root], nil)
	if !new(l.documents).scopeCoordinates(&visit, repoRoot, location.Pinned) {
		return nil, nil
	}
	visits := l.scopeWalk(&visit)
	var out []Occurrence
	{
		vValues := visits
		for vIndex := range vValues {
			v := vValues[vIndex]
			if v.role != kustomizeinput.Resource || v.document != nil || !v.readable || v.deferDecode {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
			}
			{
				var err error
				out, err = l.appendVisitOccurrences(out, new(v))
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}

type identityProjection struct {
	identity                           manifest.NamedResource
	version, namespace, prefix, suffix string
}

func (l *Loader) projectIdentity(
	id manifest.NamedResource,
	vValue *graphVisit,
	version string,
	namespaced bool,
) (manifest.NamedResource, error) {
	v := *vValue

	projection := 0
	if namespaced {
		id = manifest.Resolve(id, v.namespace, v.fallback)
		projection = 1
	}
	if v.namePrefix == "" && v.nameSuffix == "" && (v.namespace == "" || namespaced) {
		return id, nil
	}
	l.ensureScopes()
	key := identityProjection{
		identity:  id,
		version:   version,
		namespace: v.namespace,
		prefix:    v.namePrefix,
		suffix:    v.nameSuffix,
	}
	if projected, ok := l.scopes.projections[projection][key]; ok {
		return projected, nil
	}
	doc := map[string]any{"apiVersion": version, "kind": id.Kind, "metadata": map[string]any{"name": id.Name}}
	r, err := resource.NewFactory(nil).FromMap(doc)
	if err != nil {
		return id, manifest.NewError(manifest.ErrFlux, fmt.Errorf("project identity: %w", err), "")
	}
	if v.namespace != "" {
		ns := namespace.Filter{
			Namespace: v.namespace,
			FsSlice:   types.FsSlice{{Version: "v1", Kind: "Namespace", Path: types.MetadataNamePath}},
		}
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
//
//nolint:gocritic // The SDK contract accepts an occurrence value snapshot.
func (l *Loader) AdmitsOccurrence(occurrence Occurrence) bool {
	//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
	for id, record := range l.Index.Files() {
		if record.Authored != occurrence.Authored {
			continue
		}
		origin := new(l.documents).localPath(record.Path)
		if origin != filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File)) {
			continue
		}
		doc := l.documents.cache[record.Path]
		{
			childValues4 := doc.objects
			for childIndex4 := range childValues4 {
				child := childValues4[childIndex4]
				if child.outcome.Err != nil || child.Obj == nil && child.fact == nil {
					continue
				}
				if child.fact != nil && child.fact.Declaration == occurrence.Declaration ||
					child.fact == nil && child.Obj.Named() == record.Authored &&
						child.outcome.Ordinal == occurrence.Declaration.Document {
					return l.AdmitsRoot(l.CatalogObject(id), record.Path)
				}
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
	{
		walkVisits := l.entryFailureVisits(repoRoot, filepath.ToSlash(relative))
		for walkIndex := range walkVisits {
			visit := walkVisits[walkIndex]

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
	}

	return l.entryFilesFailure(ctx, files)
}

func (l *Loader) entryFailureVisits(repoRoot, relative string) []graphVisit {
	if l.entry != entryPlain {
		return l.scopeWalk(
			new(graphVisit{root: repoRoot, dir: relative, role: kustomizeinput.Resource, entryResources: true}),
		)
	}
	var visits []graphVisit
	for file := range l.documents.cache {
		doc := l.documents.cache[file]
		if doc.loaded && doc.initial && filepath.Dir(file) == l.entryPath {
			visits = append(visits, graphVisit{file: file, coordinate: l.fileCoordinate(file), readable: true})
		}
	}
	return visits
}

// OccurrenceObject MUST hydrate only the declaration in its admitted source view.
func (l *Loader) OccurrenceObject(
	ctx context.Context,
	occurrenceValue *Occurrence,
	input *store.SourceArtifact,
) (store.Child, error) {
	occurrence := *occurrenceValue

	if err := ctx.Err(); err != nil {
		return store.Child{}, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	if input != nil && input.LocalPath != occurrence.Declaration.Source {
		return store.Child{}, nil
	}
	reader := l.documents
	mounted := reader.artifacts[occurrence.Declaration.Source]
	matching := input == nil || mounted != nil && parsedInputIdentity(input) == parsedInputIdentity(mounted) &&
		input.Input(
			store.ArtifactContent|store.ArtifactRevision,
		) == mounted.Input(
			store.ArtifactContent|store.ArtifactRevision,
		)
	cached := !reader.compact && matching
	if !cached {
		reader.cache = nil
	}
	reader.parsed = nil
	reader.coordinate = &occurrence.Declaration
	if input != nil && !cached {
		reader.views = nil
		reader.artifacts = nil
		reader.setSources(map[string]*store.SourceArtifact{input.LocalPath: input})
	}
	var child store.Child
	var facts parsedSource
	if reader.compact && matching && !l.frozen {
		facts = l.sourceFacts(occurrence.Declaration.Source)
		child = facts.hydrated[occurrence.Declaration]
	}
	children := []store.Child{child}
	var parseErr error
	if child.Obj == nil {
		file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
		children, parseErr = reader.parseFile(ctx, file, manifest.ParseDocOptions{})
	}
	if err := ctx.Err(); err != nil {
		return store.Child{}, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	if parseErr != nil {
		return store.Child{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("hydrate %s: %w", occurrence.Identity, parseErr),
			"",
		)
	}
	if len(children) != 1 {
		return store.Child{}, nil
	}
	child = children[0]
	if child.Obj == nil || child.Obj.Named() != occurrence.Authored {
		return store.Child{}, nil
	}
	if facts.hydrated != nil {
		facts.hydrated[occurrence.Declaration] = child
	}
	child.Doc = manifest.DeepCopyMap(child.Doc)
	child.Warnings = slices.Clone(child.Warnings)
	if child.Obj.Named().Namespace != occurrence.Identity.Namespace {
		child.Obj = child.Obj.WithNamespace(occurrence.Identity.Namespace)
	}
	for i := range child.Warnings {
		child.Warnings[i].Resource = occurrence.Identity
	}
	return child, nil
}

//nolint:gocritic // The iterator retains its original occurrence value snapshot.
func (o Occurrence) references() iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		{
			refValues := o.facts.refs
			for refIndex := range refValues {
				ref := refValues[refIndex]
				if ref.NamespaceDefault {
					ref.Namespace = o.Identity.Namespace
				}
				if !yield(ref) {
					return
				}
			}
		}
	}
}

func (l *Loader) occurrence(id manifest.NamedResource) (Occurrence, bool) {
	o, ok := l.catalogOccurrence(id)
	if !ok || o.requiresBuild || o.Identity.Name != o.Authored.Name {
		return Occurrence{}, false
	}
	return o, true
}

func (l *Loader) catalogOccurrence(id manifest.NamedResource) (Occurrence, bool) {
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
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (l *Loader) CatalogObject(id manifest.NamedResource) manifest.BaseManifest {
	if l.nativeEntry != nil && l.nativeEntry.Named() == id {
		return l.nativeEntry
	}
	record, _ := l.Index.Lookup(id)
	return l.catalogObject(id, &record)
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (l *Loader) catalogObject(id manifest.NamedResource, record *manifest.DiscoveryRecord) manifest.BaseManifest {
	if record.Builder != "" && record.Declaration != nil {
		return manifest.CatalogObject(record.Declaration, id)
	}
	if occurrence, ok := l.catalogOccurrence(id); ok && occurrence.facts.catalog != nil {
		obj := occurrence.object
		if obj == nil {
			obj = occurrence.facts.catalog
		}
		if obj.Named() == id {
			return obj
		}
		return manifest.CatalogObject(obj, id)
	}
	obj := l.candidates[id].Obj
	if obj == nil {
		obj = l.IndexedObject(id, *record).Obj
	}
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

func (l *Loader) appendCachedPathObjects(objects []Occurrence) []Occurrence {
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
		{
			childValues := doc.objects
			for childIndex := range childValues {
				child := childValues[childIndex]
				objects = l.appendPathObject(objects, new(child))
			}
		}
	}
	return objects
}

func (l *Loader) appendPathObject(objects []Occurrence, childValue *parsedObject) []Occurrence {
	child := *childValue
	if child.outcome.Err != nil || child.Obj == nil && child.fact == nil {
		return objects
	}
	var o Occurrence
	if child.fact != nil {
		o = *child.fact
	} else {
		o = Occurrence{Identity: child.Obj.Named(), Authored: child.Obj.Named(), object: child.Obj}
	}
	source, path, ok := o.SourcePath()
	if !ok || o.facts != nil && o.facts.uncertain {
		return objects
	}
	if slices.ContainsFunc(objects, func(other Occurrence) bool {
		otherSource, otherPath, _ := other.SourcePath()
		return other.Identity == o.Identity && otherSource == source && otherPath == path
	}) {
		return objects
	}
	objects = append(objects, o)
	return objects
}

func (l *Loader) collectPackages(packages map[string]struct{}, scans map[string]string) {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for dir, doc := range l.documents.cache {
		if doc.adHoc && new(l.documents).mountedRoot(dir) == l.SourceRoot {
			relative, _ := filepath.Rel(l.SourceRoot, dir)
			scans[dir] = filepath.ToSlash(relative)
		}
		if doc.traversed && doc.body != nil && !isKustomizeComponent(doc.body) &&
			new(l.documents).mountedRoot(dir) == l.SourceRoot {
			packages[dir] = struct{}{}
		}
	}
}

func (l *Loader) collectUncoveredPackages(covered map[OccurrenceID]bool) (map[string][]Occurrence, error) {
	packages := map[string][]Occurrence{}
	for _, file := range slices.Sorted(maps.Keys(l.documents.cache)) {
		doc := l.documents.cache[file]
		if !doc.loaded || len(doc.objects) == 0 || doc.bootstrap ||
			new(l.documents).mountedRoot(file) != l.SourceRoot {
			continue
		}
		objects, err := l.appendVisitOccurrences(nil, new(graphVisit{file: file}))
		if err != nil {
			return nil, err
		}
		dir := filepath.Dir(file)
		for i := range objects {
			if !covered[objects[i].Declaration] {
				packages[dir] = append(packages[dir], objects[i])
			}
		}
	}
	return packages, nil
}

func (l *Loader) appendVisitOccurrences(out []Occurrence, vValue *graphVisit) ([]Occurrence, error) {
	v := *vValue
	doc := l.documents.cache[v.file]
	if doc.eligible == fileInactive && !doc.initial {
		return out, nil
	}
	origin := new(l.documents).localPath(v.file)
	source := new(l.documents).mountedRoot(origin)
	file, _ := filepath.Rel(source, origin)
	{
		childValues3 := doc.objects
		for childIndex3 := range childValues3 {
			child := childValues3[childIndex3]
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
				o = Occurrence{
					Declaration: OccurrenceID{
						Source:   source,
						File:     filepath.ToSlash(file),
						Document: child.outcome.Ordinal,
						item:     child.item,
					},
					Authored: child.Obj.Named(),
					Identity: child.Obj.Named(),
					object:   child.Obj,
				}
				namespaced = manifest.KindOf(child.Obj).Namespaced()
			}
			id, err := l.projectIdentity(o.Authored, new(v), child.version, namespaced)
			if err != nil {
				return nil, err
			}
			o.Identity = id
			out = append(out, o)
		}
	}
	return out, nil
}

func (l *Loader) entryFilesFailure(ctx context.Context, files []string) error {
	slices.Sort(files)
	for _, file := range slices.Compact(files) {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		{
			childValues5 := l.documents.cache[file].objects
			for childIndex5 := range childValues5 {
				child := childValues5[childIndex5]
				if child.outcome.Err != nil {
					return fmt.Errorf(
						"entry %s: %w (invalid YAML)",
						l.fileCoordinate(new(l.documents).localPath(file)),
						manifest.DocumentFailure([]manifest.DocOccurrence{child.outcome}),
					)
				}
			}
		}
	}
	return nil
}
