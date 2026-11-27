package loader

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/builtins"
	"sigs.k8s.io/kustomize/api/filters/namespace"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/api/resource"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// occurrenceID identifies an authored document within one admitted source view.
type occurrenceID struct {
	Source   string
	File     string
	Document int
	item     int
}

// occurrence retains an immutable declaration and its contextual resource identity.
type authoredOccurrence struct {
	occurrencePath

	Declaration   occurrenceID
	Identity      manifest.NamedResource
	Authored      manifest.NamedResource
	object        manifest.BaseManifest
	facts         *occurrenceFacts
	requiresBuild bool
	orderingKnown bool
}

type occurrencePath struct {
	source               manifest.NamedResource
	path                 string
	hasPath, application bool
}

type occurrenceFacts struct {
	// Census identity MUST survive projection and change when a declaration is reparsed.
	census                          *occurrenceFacts
	projection                      []string
	namespaced, uncertain           bool
	retained, completeCatalog       bool
	collection                      bool
	structural                      *manifest.Kustomization
	catalog                         manifest.BaseManifest
	reverse                         []manifest.NamedResource
	derivedInputs                   int
	refs                            []manifest.Reference
	implicitGate                    [1]manifest.Reference
	templateInputs                  []manifest.TemplateInput
	templateObjects                 []manifest.BaseManifest
	roots                           []string
	targets, authoredTargets        []manifest.ProducerTarget
	needsCandidates                 bool
	substituteDisabled, substituted bool
	policy                          *manifest.EmissionPolicy
	labels                          map[string]string
	labeled                         bool
}

// The implicit source precedes authored relations and MUST stay outside graph evidence.
func (f *occurrenceFacts) relations() []manifest.Reference {
	if len(f.refs) != 0 && f.refs[0].Use == "" && f.refs[0].Gate != 0 {
		return f.refs[1:]
	}
	return f.refs
}

func (o *authoredOccurrence) indexed() bool {
	return o.facts.retained || o.hasPath || len(o.facts.targets) != 0 || o.facts.needsCandidates
}

func (o *authoredOccurrence) opaque(targets []manifest.ProducerTarget) bool {
	f := o.facts
	return f != nil && f.policy != nil && f.structural == nil && !o.hasPath && len(f.authoredTargets) == 0 &&
		len(targets) == 0 &&
		len(f.templateInputs) == 0 &&
		len(f.templateObjects) == 0
}

// SourcePath reports the loader's immutable source and path facts.
func (o *authoredOccurrence) SourcePath() (manifest.NamedResource, string, bool) {
	return o.source, o.path, o.hasPath
}

func objectOccurrence(object manifest.BaseManifest) authoredOccurrence {
	id := object.Named()
	return authoredOccurrence{
		Identity: id, Authored: id, object: object,
		occurrencePath: objectPath(object),
	}
}

func objectPath(object manifest.BaseManifest) occurrencePath {
	source, path, hasPath := object.SourcePath()
	facts := occurrencePath{source: source, path: path, hasPath: hasPath}
	facts.setApplication(object, manifest.KindOf(object).Structural)
	return facts
}

func (o *occurrencePath) setApplication(object manifest.BaseManifest, structural bool) {
	o.application = o.hasPath
	if o.hasPath && structural {
		_, o.application = manifest.ApplicationFor(object)
	}
}

func (f *occurrenceFacts) compactOccurrence(
	obj manifest.BaseManifest,
	projection manifest.TemplateProjection,
) authoredOccurrence {
	path := objectPath(obj)
	targets := manifest.ProducerTargets(obj)
	*f = occurrenceFacts{
		namespaced:      manifest.KindOf(obj).Namespaced(),
		retained:        manifest.KindOf(obj).Tracked || path.hasPath,
		targets:         targets,
		authoredTargets: targets,
		needsCandidates: manifest.ProducerNeedsCandidates(obj),
		roots:           slices.Collect(manifest.RenderRoots(obj)),
	}
	f.census = f
	f.templateInputs = projection.Inputs
	f.templateObjects = projection.Objects
	f.targets = append(f.targets, projection.Targets...)
	generated, _ := manifest.GeneratedTargets(obj)
	f.targets = append(f.targets, generated...)
	f.catalog = manifest.CatalogObject(obj, obj.Named())
	f.completeCatalog = f.catalog == obj
	f.structural, _ = f.catalog.(*manifest.Kustomization)
	if manifest.KindOf(obj).Parent {
		policy := obj.EmissionPolicy()
		f.policy = &manifest.EmissionPolicy{
			Namespace:         policy.Namespace,
			InternalNamespace: policy.InternalNamespace,
			ExcludeChildren:   policy.ExcludeChildren,
		}
	}
	if manifest.KindOf(f.catalog).TypedLabels {
		f.labels, f.labeled = f.catalog.GetLabels(), true
	}
	for ref := range manifest.References(obj, manifest.ReferenceAll) {
		if f.refs == nil && ref.Use == "" && ref.Gate != 0 {
			f.refs = f.implicitGate[:0]
		}
		f.refs = append(f.refs, ref.Clone())
		if ref.Use != "" && ref.AffectedInput() {
			f.reverse = append(f.reverse, ref.NamedResource)
		}
	}
	id := obj.Named()
	if manifest.HasEnvsubstReference(id.Name) {
		id.Name, f.uncertain = "", true
	}
	if manifest.HasEnvsubstReference(id.Namespace) {
		id.Namespace, f.uncertain = "", true
	}
	o := authoredOccurrence{
		Identity: obj.Named(), Authored: obj.Named(), facts: f,
		occurrencePath: path,
	}
	f.substituted = o.indexed() && o.hasSubstitutions(o.Authored)
	o.setIdentity(id)
	return o
}

// PackageEvidence retains package provenance and shared root admission.
type PackageEvidence struct {
	Path        string
	Scans       []string
	Occurrences []selection.OccurrenceFacts
	Independent bool
}

// OccurrenceClaims derives claims from mounted source views without admitting document identities.
func (l *Loader) OccurrenceClaims(repoRoot string, occurrences []selection.OccurrenceFacts) []manifest.KSClaim {
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
			if o.Structural != nil {
				ks = o.Structural
			} else {
				//nolint:errcheck // Structural occurrence identity requires the retained Kustomization assertion.
				ks = o.Object.(*manifest.Kustomization)
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
func (l *Loader) PathObjects() []selection.OccurrenceFacts {
	stored := l.Store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)
	objects := make([]authoredOccurrence, 0, len(stored))
	for _, object := range stored {
		if !l.Store.Policy(object.Named()).Root || l.documents.compact && object.Named().ReportHidden() {
			continue
		}
		if l.inactiveKustomization(object.Named()) {
			continue
		}
		o := objectOccurrence(object)
		if _, _, applies := o.SourcePath(); applies {
			objects = append(objects, o)
		}
	}
	objects = l.appendIndexedPathObjects(objects)
	if l.scopes == nil || !l.scopes.complete {
		objects = l.appendCachedPathObjects(objects)
	}
	slices.SortStableFunc(objects, func(a, b authoredOccurrence) int {
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
	result := make([]selection.OccurrenceFacts, len(objects))
	for i := range objects {
		result[i] = occurrenceView(&objects[i])
	}
	return result
}

func (l *Loader) appendIndexedPathObjects(objects []authoredOccurrence) []authoredOccurrence {
	for id := range l.Index.Files() {
		if l.Store.GetObject(id) != nil || l.inactiveKustomization(id) || !l.declarationLocated(id, l.Store) {
			continue
		}
		object := l.CatalogObject(id)
		if object == nil {
			continue
		}
		o := objectOccurrence(object)
		if occurrence, known := l.catalogOccurrence(id); known && o.hasPath && l.Index.Records[id].Builder == "" {
			o.occurrencePath = occurrence.occurrencePath
			o.requiresBuild = occurrence.requiresBuild
		}
		if _, _, applies := o.SourcePath(); applies {
			objects = append(objects, o)
		}
	}
	return objects
}

// Packages snapshots resource graph inputs already traversed by Load.
func (l *Loader) Packages(ctx context.Context) ([]PackageEvidence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mountSources()
	l.ensureScopes()
	packages := map[string]struct{}{}
	scans := map[string]string{}
	l.collectPackages(packages, scans)
	covered := map[selection.OccurrenceID]bool{}
	byPath := map[string]PackageEvidence{}
	for _, dir := range slices.Sorted(maps.Keys(packages)) {
		objects, err := l.occurrences(ctx, dir, "")
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
func (l *Loader) Occurrences(ctx context.Context, dir, targetNamespace string) ([]selection.OccurrenceFacts, error) {
	l.mountSources()
	l.ensureScopes()
	return l.occurrences(ctx, dir, targetNamespace)
}

func (l *Loader) occurrences(ctx context.Context, dir, targetNamespace string) ([]selection.OccurrenceFacts, error) {
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
	location := sourceLocation(l.documents.artifacts[root], repoRoot, false)
	if !new(l.documents).scopeCoordinates(&visit, repoRoot, location.Pinned, location.External) {
		return nil, nil
	}
	visits := l.scopeWalk(&visit)
	var occurrences []authoredOccurrence
	{
		vValues := visits
		for vIndex := range vValues {
			v := vValues[vIndex]
			if v.role != kustomizeinput.Resource || v.document != nil || !v.readable || v.deferDecode {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := l.visitOccurrences(&v, func(o authoredOccurrence) {
				occurrences = append(occurrences, o)
			}); err != nil {
				return nil, err
			}
		}
	}
	var out []selection.OccurrenceFacts
	if len(occurrences) != 0 {
		out = make([]selection.OccurrenceFacts, len(occurrences))
	}
	for i := range occurrences {
		out[i] = occurrenceView(&occurrences[i])
	}
	return out, nil
}

type identityProjection struct {
	identity                           manifest.NamedResource
	version, namespace, prefix, suffix string
}

func (l *Loader) projectIdentity(
	id manifest.NamedResource,
	v *graphVisit,
	version string,
	namespaced bool,
) (manifest.NamedResource, error) {
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
	doc := map[string]any{apiVersionKey: version, kindKey: id.Kind, "metadata": map[string]any{"name": id.Name}}
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

// EntryFailure MUST defer admission until selection consumes its iterator.
func (l *Loader) EntryFailure(ctx context.Context, repoRoot string) error {
	var admissionErr error
	err := l.Options.Session.EntryFailure(ctx, func(yield func(selection.EntryFailure) bool) {
		if admissionErr = l.Admit(ctx, repoRoot); admissionErr != nil {
			return
		}
		relative, _ := filepath.Rel(repoRoot, l.entryPath)
		var failures []selection.EntryFailure
		visits := l.entryFailureVisits(repoRoot, filepath.ToSlash(relative))
		for i := range visits {
			visit := &visits[i]
			doc := l.documents.cache[visit.file]
			unreadable := !visit.readable || visit.deferDecode
			switch {
			case visit.role != kustomizeinput.Resource, visit.document != nil, unreadable, doc.selected:
				continue
			}
			index := slices.IndexFunc(doc.objects, func(child parsedObject) bool { return child.outcome.Err != nil })
			if index < 0 {
				continue
			}
			admission := l.rootAdmission(visit.file, visit.coordinate, false)
			record := manifest.DiscoveryRecord{Path: visit.file, File: visit.coordinate}
			view := l.physicalView()
			evidence := view.rootScope(l, &rootScopeInput{record: record, admission: admission})
			failures = append(failures, selection.EntryFailure{
				File:       l.fileCoordinate(new(l.documents).localPath(visit.file)),
				Coordinate: visit.coordinate,
				Evidence:   evidence.view(),
				Occurrence: doc.objects[index].outcome,
			})
		}
		slices.SortFunc(failures, func(a, b selection.EntryFailure) int {
			if compared := strings.Compare(a.Evidence.Record.Path, b.Evidence.Record.Path); compared != 0 {
				return compared
			}
			return strings.Compare(a.Coordinate, b.Coordinate)
		})
		for i := range failures {
			if !yield(failures[i]) {
				return
			}
		}
	})
	if admissionErr != nil {
		return admissionErr
	}
	return err
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
	occurrenceValue *selection.OccurrenceFacts,
	input *store.SourceArtifact,
) (store.Child, error) {
	occurrence := *occurrenceValue
	if occurrence.Declaration.File == "" {
		return store.Child{}, nil
	}
	coordinate := occurrenceID{
		Source: occurrence.Declaration.Source, File: occurrence.Declaration.File,
		Document: occurrence.Declaration.Document, item: occurrence.Declaration.Item,
	}

	if err := ctx.Err(); err != nil {
		return store.Child{}, err
	}
	if input != nil && input.LocalPath != occurrence.Declaration.Source {
		return store.Child{}, nil
	}
	reader := l.documents
	mounted := reader.artifacts[occurrence.Declaration.Source]
	matching := input == nil || mounted != nil &&
		new(parsedBindingFor(input)).matches(new(parsedBindingFor(mounted)), true)
	cached := !reader.compact && matching
	if !cached {
		reader.cache = nil
	}
	reader.parsed = nil
	reader.coordinate = &coordinate
	if input != nil && !cached {
		reader.views = nil
		reader.artifacts = nil
		reader.setSources(map[string]*store.SourceArtifact{input.LocalPath: input})
	}
	var child store.Child
	var facts parsedSource
	if reader.compact && matching && !l.frozen {
		facts = l.sourceFacts(occurrence.Declaration.Source)
		child, _, _ = facts.hydrated.get(coordinate)
	}
	var parseErr error
	children := []store.Child{child}
	if child.Obj == nil {
		file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
		children, parseErr = l.readOccurrence(ctx, &reader, file, matching)
	}
	if err := ctx.Err(); err != nil {
		return store.Child{}, err
	}
	if parseErr != nil {
		return store.Child{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("hydrate %s: %w", occurrence.Identity, parseErr),
			"",
		)
	}
	if len(children) != 1 || children[0].Obj == nil || children[0].Obj.Named() != occurrence.Authored {
		return store.Child{}, nil
	}
	child = children[0]
	if facts.hydrated.values != nil {
		facts.hydrated.values[coordinate] = child
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

func (o *authoredOccurrence) references() iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		view := selection.OccurrenceFacts{Identity: o.Identity, Refs: o.facts.relations(), Witness: o.facts}
		view.VisitReferences(manifest.ReferenceRelations, yield)
	}
}

// IndexedDeclarationFile supplies file evidence only when discovery knows the effective identity.
func (l *Loader) IndexedDeclarationFile(id manifest.NamedResource) string {
	occurrence, known := l.occurrence(id)
	if !known || occurrence.facts != nil && occurrence.facts.uncertain {
		return ""
	}
	return occurrence.Declaration.File
}

func (l *Loader) occurrence(id manifest.NamedResource) (authoredOccurrence, bool) {
	o, ok := l.catalogOccurrence(id)
	if !ok || o.requiresBuild || o.Identity.Name != o.Authored.Name {
		return authoredOccurrence{}, false
	}
	return o, true
}

func (l *Loader) catalogOccurrence(id manifest.NamedResource) (authoredOccurrence, bool) {
	return catalogOccurrence(l.locations, id)
}

func catalogOccurrence(
	locations map[manifest.NamedResource]authoredOccurrence,
	id manifest.NamedResource,
) (authoredOccurrence, bool) {
	if o, ok := locations[id]; ok {
		return o, true
	}
	if id.Namespace != "" {
		bare := id
		bare.Namespace = ""
		if o, ok := locations[bare]; ok && o.facts != nil && o.facts.namespaced && o.Authored.Namespace == "" {
			o.setIdentity(id)
			return o, true
		}
	}
	return authoredOccurrence{}, false
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
	view := l.physicalView()
	record, _ := l.Index.Lookup(id)
	return view.object(id, &record, l)
}

func (l *Loader) appendCachedPathObjects(objects []authoredOccurrence) []authoredOccurrence {
	files := make([]string, 0, len(l.documents.cache))
	for file := range l.documents.cache {
		files = append(files, file)
	}
	slices.Sort(files)
	for _, file := range files {
		doc := l.documents.cache[file]
		if doc.eligible == selection.RootInactive {
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

func (l *Loader) appendPathObject(
	objects []authoredOccurrence,
	childValue *parsedObject,
) []authoredOccurrence {
	child := *childValue
	if child.outcome.Err != nil || child.Obj == nil && child.fact == nil {
		return objects
	}
	var o authoredOccurrence
	if child.fact != nil {
		o = *child.fact
	} else {
		o = objectOccurrence(child.Obj)
	}
	source, path, ok := o.SourcePath()
	if !ok || o.facts != nil && o.facts.uncertain {
		return objects
	}
	if slices.ContainsFunc(objects, func(other authoredOccurrence) bool {
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

func (l *Loader) collectUncoveredPackages(
	covered map[selection.OccurrenceID]bool,
) (map[string][]selection.OccurrenceFacts, error) {
	packages := map[string][]selection.OccurrenceFacts{}
	for _, file := range slices.Sorted(maps.Keys(l.documents.cache)) {
		doc := l.documents.cache[file]
		if !doc.loaded || len(doc.objects) == 0 || doc.bootstrap ||
			new(l.documents).mountedRoot(file) != l.SourceRoot {
			continue
		}
		dir := filepath.Dir(file)
		if err := l.visitOccurrences(&graphVisit{file: file}, func(o authoredOccurrence) {
			view := occurrenceView(&o)
			if !covered[view.Declaration] {
				packages[dir] = append(packages[dir], view)
			}
		}); err != nil {
			return nil, err
		}
	}
	return packages, nil
}

func (l *Loader) visitOccurrences(vValue *graphVisit, yield func(authoredOccurrence)) error {
	v := *vValue
	doc := l.documents.cache[v.file]
	if doc.eligible == selection.RootInactive && !doc.initial {
		return nil
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
			var o authoredOccurrence
			if child.fact != nil {
				if child.fact.facts.uncertain {
					continue
				}
				projected, err := l.projectOccurrence(child.fact, &v, child.version)
				if err != nil {
					return err
				}
				o = projected
			} else {
				o = objectOccurrence(child.Obj)
				o.Declaration = occurrenceID{
					Source: source, File: filepath.ToSlash(file), Document: child.outcome.Ordinal, item: child.item,
				}
				id, err := l.projectIdentity(o.Authored, &v, child.version, manifest.KindOf(child.Obj).Namespaced())
				if err != nil {
					return err
				}
				o.setIdentity(id)
			}
			o.requiresBuild = o.requiresBuild || v.requiresBuild
			yield(o)
		}
	}
	return nil
}

func (l *Loader) readOccurrence(
	ctx context.Context,
	reader *documentReader,
	file string,
	matching bool,
) ([]store.Child, error) {
	var children []store.Child
	var payload *inputPayload
	var warnings []manifest.Warning
	if matching {
		objects := l.documents.cache[file].objects
		for i := range objects {
			object := &objects[i]
			if object.outcome.Ordinal == reader.coordinate.Document && object.item == reader.coordinate.item {
				if object.fact != nil && object.fact.object != nil {
					return []store.Child{
						{Obj: object.fact.object, Doc: object.Doc, Warnings: object.Warnings},
					}, nil
				}
				payload = object.payload
				warnings = object.Warnings
				break
			}
		}
	}
	if payload == nil {
		return reader.parseFile(ctx, file, manifest.ParseDocOptions{})
	}
	data, err := payload.read(ctx)
	if err != nil {
		return nil, err
	}
	docs, err := manifest.DecodeRender(data)
	if err != nil {
		return nil, err
	}
	for _, doc := range docs {
		doc.Warnings = warnings
		parsed, outcome, _ := Prepare(doc, manifest.ParseDocOptions{})
		if outcome.Err != nil {
			return children, outcome.Err
		}
		children = append(children, parsed)
	}
	return children, nil
}
