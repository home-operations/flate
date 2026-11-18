package discovery

import (
	"context"
	"io/fs"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type clusterRoot struct {
	Path               string
	Occurrences        []selection.OccurrenceFacts
	Anchors            []string
	pinned             manifest.DiscoveryIndex
	sources            map[manifest.NamedResource]entryResolution
	views              map[string]*store.SourceArtifact
	parsed             *loader.ParsedInputs
	seeds              []selection.OccurrenceFacts
	packageBuild       *plannedPackage
	bootstrap          bool
	sourceDeclarations []selection.OccurrenceID
}

type occurrenceProjection struct {
	path, namespace string
	input           *store.SourceArtifact
}

type entryPlanner struct {
	roots       []clusterRoot
	index       int
	queue       []rootVisit
	head        int
	parsed      *loader.ParsedInputs
	started     bool
	scope       rootScope
	catalog     rootScope
	catalogView rootScope
	coordinates map[selection.OccurrenceID]bool
	seen        map[occurrenceKey]bool

	session     *Session
	inputs      map[string]*store.SourceArtifact
	objects     map[occurrenceKey]store.Child
	projections map[occurrenceProjection][]selection.OccurrenceFacts
}

func (d *Session) clusterRoots(ctx context.Context) ([]clusterRoot, error) {
	if d.planner == nil {
		if err := d.beginRootPlan(ctx); err != nil {
			return nil, err
		}
	}
	planner := d.planner
	for planner.index < len(planner.roots) {
		root := &planner.roots[planner.index]
		if !planner.started {
			if err := planner.start(ctx, root); err != nil {
				return nil, err
			}
			planner.started = true
		}
		if err := planner.reach(ctx, root); err != nil {
			return nil, err
		}
		if planner.parsed == nil {
			planner.parsed = d.loader.ParsedInputs()
		}
		root.parsed = planner.parsed
		planner.index++
		planner.started = false
	}
	roots := d.unpinnedRoots(planner.roots)
	d.planner = nil
	return roots, nil
}

func (p *entryPlanner) start(ctx context.Context, root *clusterRoot) error {
	if err := p.prepareRoot(ctx, root); err != nil {
		return err
	}
	seeds := root.seeds
	p.queue = slices.Grow(p.queue[:0], len(seeds))
	p.head = 0
	clear(p.coordinates)
	clear(p.seen)
	p.scope = rootScope{occurrences: seeds}
	boundary := ""
	for i := range seeds {
		application, applies := manifest.ApplicationFor(occurrenceObject(&seeds[i]))
		if !applies {
			continue
		}
		target := filepath.Join(p.session.cfg.Input.LocalPath, manifest.NormalizeClaimBase(application.Path))
		file := filepath.Join(seeds[i].Declaration.Source, seeds[i].Declaration.File)
		if safepath.Contains(target, file) && (boundary == "" || len(target) < len(boundary)) {
			boundary = target
		}
	}
	p.catalogView = p.catalog
	if boundary != "" {
		p.catalogView.occurrences = nil
		for candidate := range p.catalog.objects() {
			if safepath.Contains(boundary, filepath.Join(candidate.Declaration.Source, candidate.Declaration.File)) {
				p.catalogView.occurrences = append(p.catalogView.occurrences, *candidate)
			}
		}
	}
	if err := p.prepareScope(ctx, &p.scope); err != nil {
		return err
	}
	for i := range seeds {
		p.queue = append(
			p.queue,
			rootVisit{occurrence: &seeds[i], scope: &p.scope},
		)
		p.coordinates[seeds[i].Declaration] = true
	}
	return nil
}

func (d *Session) unpinnedRoots(pending []clusterRoot) []clusterRoot {
	slices.SortFunc(pending, func(a, b clusterRoot) int { return strings.Compare(a.Path, b.Path) })
	claims := manifest.DiscoveryIndex{Claims: map[string][]manifest.NamedResource{}}
	owners := make(map[manifest.NamedResource]int, len(pending))
	for i := range pending {
		id := manifest.NewEntryKustomization(manifest.BootstrapSourceID, pending[i].Path, manifest.NamedResource{}).
			Named()
		owners[id] = i
		for prefix := range pending[i].pinned.Claims {
			claims.Claims[prefix] = append(claims.Claims[prefix], id)
		}
	}
	active := make([]bool, len(pending))
	for i := range active {
		active[i] = true
	}
	claimed := func(child, owner int) bool {
		for group := range claims.ClaimOwners(pending[child].Path+"/", manifest.NamedResource{}, manifest.NamedResource{},
			func(id manifest.NamedResource) bool {
				return owners[id] == owner && owner != child
			}) {
			if len(group) != 0 {
				return true
			}
		}
		return false
	}
	var roots []clusterRoot
	for {
		selected := pinnedRoot(&claims, owners, pending, active)
		if selected < 0 {
			break
		}
		roots = append(roots, pending[selected])
		for i := range pending {
			if active[i] && (i == selected || claimed(i, selected)) {
				active[i] = false
			}
		}
	}
	return roots
}

type rootVisit struct {
	occurrence *selection.OccurrenceFacts
	scope      *rootScope
}

type rootScope struct {
	occurrences []selection.OccurrenceFacts
	parent      *rootScope
}

func (s *rootScope) objects() iter.Seq[*selection.OccurrenceFacts] {
	return func(yield func(*selection.OccurrenceFacts) bool) {
		for scope := s; scope != nil; scope = scope.parent {
			for i := range scope.occurrences {
				if !yield(&scope.occurrences[i]) {
					return
				}
			}
		}
	}
}

func occurrenceReferences(occurrence *selection.OccurrenceFacts) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		for i := range occurrence.Refs {
			ref := occurrence.Refs[i]
			if ref.NamespaceDefault {
				ref.Namespace = occurrence.Identity.Namespace
			}
			if !yield(ref) {
				return
			}
		}
	}
}

func (p *entryPlanner) reach(ctx context.Context, root *clusterRoot) error {
	for p.head < len(p.queue) {
		visit := p.queue[p.head]
		key := occurrenceIdentity(visit.occurrence)
		if p.seen[key] {
			p.queue[p.head] = rootVisit{}
			p.head++
			continue
		}
		_, _, hasPath := visit.occurrence.SourcePath()
		if visit.occurrence.RequiresBuild || !hasPath && !visit.occurrence.Identity.KindInfo().Runnable {
			root.Occurrences = append(root.Occurrences, *visit.occurrence)
			p.seen[key] = true
			p.queue[p.head] = rootVisit{}
			p.head++
			continue
		}
		object := occurrenceObject(visit.occurrence)
		application, _ := manifest.ApplicationFor(object)
		references := manifest.References(object, manifest.ReferenceRelations)
		if visit.occurrence.Witness != nil {
			references = occurrenceReferences(visit.occurrence)
		}
		for ref := range references {
			if !ref.IsSource() || !ref.NamedResource.KindInfo().IsSource() || ref.NamedResource == application.Source {
				continue
			}
			if _, sourceErr := p.source(ctx, root, &visit, &ref); sourceErr != nil {
				return sourceErr
			}
		}
		children, err := p.follow(ctx, root, &visit, p.coordinates[visit.occurrence.Declaration])
		if err != nil {
			return err
		}
		root.Occurrences = append(root.Occurrences, *visit.occurrence)
		p.seen[key] = true
		p.queue[p.head] = rootVisit{}
		p.head++
		p.queue = append(p.queue, children...)
	}
	slices.Sort(root.Anchors)
	return nil
}

func (p *entryPlanner) follow(
	ctx context.Context,
	root *clusterRoot,
	visit *rootVisit,
	seed bool,
) ([]rootVisit, error) {
	occurrence := visit.occurrence
	object := occurrenceObject(occurrence)
	application, valid := manifest.ApplicationFor(object)
	if !valid {
		return nil, nil
	}
	reference := sourceReference(object, application.Source)
	reference.NamedResource = application.Source
	input, err := p.source(ctx, root, visit, &reference)
	if err != nil {
		return nil, err
	}
	application.Path = filepath.ToSlash(filepath.Clean(manifest.NormalizeClaimBase(application.Path)))
	if seed {
		root.bootstrap = root.bootstrap || p.bootstrapRoot(root, &occurrence.Declaration, input, &application)
	}
	absolute, local := p.followTarget(root, input, application.Path, seed)
	if absolute == "" {
		return nil, nil
	}
	total := 0
	_, err = p.session.loadAt(ctx, absolute, &total, &sourceLoad{
		work:     p.session.cfg.Work.Source(application.Source).Record(manifest.WorkPreparation, absolute),
		artifact: input,
	})
	if err != nil {
		return nil, err
	}
	pinned := p.session.pinnedWorkingTreeClaims(p.session.cfg.Input.LocalPath, []selection.OccurrenceFacts{*occurrence})
	if len(pinned.index.Claims) != 0 {
		if root.pinned.Claims == nil {
			root.pinned.Claims = map[string][]manifest.NamedResource{}
		}
		for prefix, ids := range pinned.index.Claims {
			root.pinned.Claims[prefix] = append(root.pinned.Claims[prefix], ids...)
		}
	}
	children, err := p.occurrences(ctx, input, object, absolute)
	if err != nil {
		return nil, err
	}
	if local && seed && slices.ContainsFunc(children, func(o selection.OccurrenceFacts) bool {
		return o.Declaration == occurrence.Declaration
	}) && !slices.Contains(root.Anchors, application.Path) {
		root.Anchors = append(root.Anchors, application.Path)
	}
	scope := &rootScope{occurrences: children, parent: visit.scope}
	if err := p.prepareScope(ctx, scope); err != nil {
		return nil, err
	}
	next := make([]rootVisit, len(children))
	for i := range children {
		next[i] = rootVisit{occurrence: &children[i], scope: scope}
	}
	return next, nil
}

func (p *entryPlanner) followTarget(
	root *clusterRoot,
	input *store.SourceArtifact,
	path string,
	seed bool,
) (string, bool) {
	if input == nil {
		return "", false
	}
	absolute := filepath.Join(input.LocalPath, path)
	if !safepath.Contains(input.LocalPath, absolute) {
		return "", false
	}
	cfg := &p.session.cfg
	local := input == cfg.Input || input.InputAlias && input.LocalRoot == cfg.Input.LocalPath
	if !seed && local && cfg.Changes != nil && p.singleAnchoredRoot(root) {
		scan, target := cfg.Input.OriginPath(cfg.Path), input.OriginPath(absolute)
		// An anchored single entry has no cluster membership to learn from disjoint workloads.
		if !safepath.Contains(scan, target) && !safepath.Contains(target, scan) {
			return "", false
		}
	}
	return absolute, local
}

func (p *entryPlanner) singleAnchoredRoot(root *clusterRoot) bool {
	return len(p.roots) == 1 && len(root.Anchors) != 0
}

func (p *entryPlanner) bootstrapRoot(
	root *clusterRoot,
	declaration *selection.OccurrenceID,
	input *store.SourceArtifact,
	application *manifest.EntryApplication,
) bool {
	file := filepath.Join(declaration.Source, declaration.File)
	packagePath := p.session.cfg.Input.OriginPath(filepath.Join(p.session.cfg.Input.LocalPath, root.Path))
	origin := p.session.cfg.Input.OriginPath(filepath.Join(p.session.cfg.Input.LocalPath, application.Path))
	entry := packagePath == p.session.cfg.Input.OriginPath(p.session.cfg.Path)
	if !entry && !safepath.Contains(origin, file) && safepath.Contains(packagePath, origin) {
		return false
	}
	if input == nil {
		binding := p.session.resolvedSources.snapshot()[application.Source]
		return binding.object != nil && !binding.synthetic
	}
	info, err := fs.Stat(input.FS, application.Path)
	return entry || err == nil && info.IsDir()
}

func (p *entryPlanner) prepareScope(ctx context.Context, scope *rootScope) error {
	for i := range scope.occurrences {
		occurrence := &scope.occurrences[i]
		kind := manifest.KindOf(occurrenceObject(occurrence))
		if !kind.Parent || !kind.Discovery || kind.Structural || len(occurrence.TemplateObjects) != 0 {
			continue
		}
		child, err := p.object(ctx, occurrence)
		if err != nil {
			return err
		}
		if child.Obj != nil {
			facts := p.session.loader.ProducerFacts(child.Obj).Occurrence
			occurrence.Targets, occurrence.TemplateObjects = facts.Targets, facts.TemplateObjects
		}
	}
	return nil
}

func (p *entryPlanner) object(ctx context.Context, occurrence *selection.OccurrenceFacts) (store.Child, error) {
	key := occurrenceIdentity(occurrence)
	key.input = p.inputs[occurrence.Declaration.Source]
	if child, ok := p.objects[key]; ok {
		return child, nil
	}
	child, err := p.session.loader.OccurrenceObject(ctx, occurrence, p.inputs[occurrence.Declaration.Source])
	if err == nil && child.Obj != nil {
		if p.objects == nil {
			p.objects = map[occurrenceKey]store.Child{}
		}
		p.objects[key] = child
		p.parsed = nil
	}
	return child, err
}

func (p *entryPlanner) source(
	ctx context.Context,
	root *clusterRoot,
	visit *rootVisit,
	reference *manifest.Reference,
) (*store.SourceArtifact, error) {
	id := reference.NamedResource
	object := occurrenceObject(visit.occurrence)
	application, _ := manifest.ApplicationFor(object)
	selected, projected, producers := visit.scope.source(id)
	if selected == nil && projected == nil && len(producers) == 0 {
		selected, projected, producers = p.catalogSource(id)
	}
	if selected != nil && selected.RequiresBuild {
		return nil, nil //nolint:nilnil // Unprepared predictions MUST NOT supply source material.
	}

	if application.Source == id && selected != nil {
		if !slices.Contains(root.sourceDeclarations, selected.Declaration) {
			root.sourceDeclarations = append(root.sourceDeclarations, selected.Declaration)
		}
	}

	group := entrySourceGroup{
		scoped: true, required: true, reference: *reference, producers: producers,
	}
	for i := range p.session.cfg.Declarations {
		declaration := &p.session.cfg.Declarations[i]
		if declaration.Obj.Named() == id {
			group.object, group.document = declaration.Obj, declaration.Doc
		}
	}
	switch {
	case group.object != nil:
	case selected != nil:
		child, err := p.object(ctx, selected)
		if err != nil {
			return nil, err
		}
		group.object, group.document = child.Obj, child.Doc
	default:
		group.project(projected)
	}
	group.entry = application.Source == id && p.appliesEntry(&application, visit.occurrence, group.object)
	admission := p.session.admission()
	before := p.session.resolvedSources.snapshot()[id]
	if _, err := p.session.resolveEntrySource(ctx, admission, id, &group); err != nil {
		return nil, err
	}
	binding := p.session.resolvedSources.snapshot()[id]
	binding.producers = producers
	return p.recordSource(root, id, &binding, before.artifact), nil
}

type occurrenceKey struct {
	declaration   selection.OccurrenceID
	identity      manifest.NamedResource
	object        manifest.BaseManifest
	witness       any
	requiresBuild bool
	input         *store.SourceArtifact
}

func occurrenceIdentity(occurrence *selection.OccurrenceFacts) occurrenceKey {
	return occurrenceKey{
		occurrence.Declaration,
		occurrence.Identity,
		occurrence.Object,
		occurrence.Witness,
		occurrence.RequiresBuild,
		occurrence.Mounted,
	}
}

func occurrenceObject(occurrence *selection.OccurrenceFacts) manifest.BaseManifest {
	object := occurrence.Object
	if object == nil {
		object = occurrence.Catalog
	}
	if object != nil && object.Named().Namespace != occurrence.Identity.Namespace {
		object = object.WithNamespace(occurrence.Identity.Namespace)
	}
	return object
}

func (s *rootScope) source(id manifest.NamedResource) (
	*selection.OccurrenceFacts,
	manifest.BaseManifest,
	[]occurrenceKey,
) {
	var selected *selection.OccurrenceFacts
	var projected manifest.BaseManifest
	var producers []occurrenceKey
	project := func(object manifest.BaseManifest) bool {
		application, valid := manifest.ApplicationFor(object)
		if !valid || application.Declaration == nil || application.Source != id {
			return false
		}
		if projected == nil {
			projected = application.Declaration
		}
		return true
	}
	for candidate := range s.objects() {
		if candidate.Identity == id {
			selected = candidate
		}
		produced := project(occurrenceObject(candidate))
		for _, object := range candidate.TemplateObjects {
			produced = project(object) || produced
		}
		targets := candidate.Targets
		if candidate.NeedsCandidates {
			targets = manifest.ProducerTargets(occurrenceObject(candidate), id)
		}
		produced = produced || slices.ContainsFunc(targets, func(target manifest.ProducerTarget) bool {
			return target.NamedResource == id
		})
		if produced {
			producers = append(producers, occurrenceIdentity(candidate))
		}
	}

	return selected, projected, producers
}

func pinnedRoot(
	claims *manifest.DiscoveryIndex,
	owners map[manifest.NamedResource]int,
	pending []clusterRoot,
	active []bool,
) int {
	first := slices.Index(active, true)
	if first < 0 {
		return -1
	}
	chain := []int{first}
	selected := first
	for {
		parent := -1
		for group := range claims.ClaimOwners(pending[selected].Path+"/", manifest.NamedResource{}, manifest.NamedResource{},
			func(id manifest.NamedResource) bool { return active[owners[id]] && owners[id] != selected }) {
			parent = owners[group[0]]
			break
		}
		if parent < 0 {
			break
		}
		if cycle := slices.Index(chain, parent); cycle >= 0 {
			selected = slices.Min(chain[cycle:])
			break
		}
		chain = append(chain, parent)
		selected = parent
	}
	return selected
}

func (p *entryPlanner) catalogSource(
	id manifest.NamedResource,
) (*selection.OccurrenceFacts, manifest.BaseManifest, []occurrenceKey) {
	catalog := &p.catalogView
	declarations := 0
	for candidate := range catalog.objects() {
		if candidate.Identity == id {
			declarations++
		}
	}
	if declarations <= 1 {
		return catalog.source(id)
	}
	return nil, nil, nil
}

func (p *entryPlanner) appliesEntry(
	application *manifest.EntryApplication,
	occurrence *selection.OccurrenceFacts,
	source manifest.BaseManifest,
) bool {
	if p.session.admission().appliesEntry(application) {
		return true
	}
	if _, capable := source.(entrySource); !capable {
		return false
	}
	if !p.coordinates[occurrence.Declaration] || occurrence.Declaration.Source != p.session.cfg.Input.LocalPath {
		return false
	}
	for candidate := range p.scope.objects() {
		other, valid := manifest.ApplicationFor(occurrenceObject(candidate))
		if valid && other.Source != application.Source && p.session.admission().appliesEntry(&other) {
			return false
		}
	}
	target := filepath.Join(p.session.cfg.Input.LocalPath, manifest.NormalizeClaimBase(application.Path))
	declaration := filepath.Join(occurrence.Declaration.Source, occurrence.Declaration.File)
	return target == p.session.cfg.Path && safepath.Contains(target, declaration)
}

func (p *entryPlanner) occurrences(
	ctx context.Context,
	input *store.SourceArtifact,
	object manifest.BaseManifest,
	absolute string,
) ([]selection.OccurrenceFacts, error) {
	key := occurrenceProjection{path: absolute, input: input}
	if manifest.KindOf(object).Parent {
		key.namespace = object.EmissionPolicy().Namespace
	}
	children, known := p.projections[key]
	if !known {
		p.parsed = nil
		var err error
		children, err = p.session.loader.Occurrences(ctx, absolute, key.namespace)
		if err != nil {
			return nil, err
		}
		if p.projections == nil {
			p.projections = map[occurrenceProjection][]selection.OccurrenceFacts{}
		}
		p.projections[key] = children
	}
	return children, nil
}

func (p *entryPlanner) recordSource(
	root *clusterRoot,
	id manifest.NamedResource,
	binding *entryResolution,
	before *store.SourceArtifact,
) *store.SourceArtifact {
	if !binding.artifact.SameMaterial(
		before,
		store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
	) {
		p.parsed = nil
	}
	if binding.artifact != nil {
		p.inputs[binding.artifact.LocalPath] = binding.artifact
	}
	if binding.viewKey == "" && !binding.projected && len(binding.producers) == 0 {
		return p.session.sourceMaterial(id, binding.artifact)
	}
	if root.sources == nil {
		root.sources = map[manifest.NamedResource]entryResolution{}
	}
	previous := root.sources[id]
	previous.merge(binding)
	root.sources[id] = previous
	if binding.viewKey == "" {
		return p.session.sourceMaterial(id, binding.artifact)
	}
	if root.views == nil {
		root.views = map[string]*store.SourceArtifact{}
	}
	root.views[binding.viewKey] = binding.artifact
	return p.session.sourceMaterial(id, binding.artifact)
}

func (p *entryPlanner) prepareRoot(ctx context.Context, root *clusterRoot) error {
	input := p.session.cfg.Input
	occurrences := root.seeds
	if !p.rootRequiresPreparation(root, input) {
		return nil
	}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root.Path, manifest.NamedResource{})
	prepared, err := p.session.prepareDocuments(ctx, owner, input, nil)
	if err != nil {
		return err
	}
	// Restricted roots need built declarations to resolve indexed producers before entry qualification.
	register := !p.session.cfg.Selection.AllRoots()
	remaining := rootProducerCount(occurrences)
	originals := make(map[manifest.NamedResource]*selection.OccurrenceFacts, len(occurrences))
	for i := range slices.Backward(occurrences) {
		originals[occurrences[i].Identity] = &occurrences[i]
	}
	fallback := filepath.ToSlash(filepath.Join(root.Path, manifest.KustomizeBuilderFilenames[0]))
	result := make([]selection.OccurrenceFacts, 0, len(prepared.children))
	for i := range prepared.children {
		child := &prepared.children[i]
		facts := p.session.loader.ProducerFacts(child.Obj).Occurrence
		original := originals[facts.Identity]
		if original != nil {
			facts.Declaration = original.Declaration
		} else {
			facts.Declaration = selection.OccurrenceID{Source: input.LocalPath, File: fallback, Document: i}
		}
		facts.Mounted = input
		if p.objects == nil {
			p.objects = map[occurrenceKey]store.Child{}
		}
		key := occurrenceIdentity(&facts)
		key.input = input
		p.objects[key] = *child
		result = append(result, facts)
		if register || !rootPreparationProducer(facts.Identity) {
			continue
		}
		remaining--
		if original == nil {
			register = true
			continue
		}
		raw, err := p.object(ctx, original)
		register = err != nil || !manifest.Equal(raw.Obj, child.Obj)
	}
	root.seeds = result
	root.packageBuild = &plannedPackage{render: prepared.declarationRender, register: register || remaining != 0}
	return ctx.Err()
}

func (p *entryPlanner) rootRequiresPreparation(root *clusterRoot, input *store.SourceArtifact) bool {
	if p.session.cfg.Trees == nil {
		return false
	}
	for i := range root.seeds {
		occurrence := &root.seeds[i]
		if !occurrence.RequiresBuild {
			continue
		}
		application, valid := manifest.ApplicationFor(occurrenceObject(occurrence))
		if !valid {
			continue
		}
		target := filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(application.Path))
		declaration := filepath.Join(occurrence.Declaration.Source, occurrence.Declaration.File)
		if safepath.Contains(target, declaration) {
			return true
		}
	}
	return false
}

func rootProducerCount(occurrences []selection.OccurrenceFacts) int {
	count := 0
	for i := range occurrences {
		if rootPreparationProducer(occurrences[i].Identity) {
			count++
		}
	}
	return count
}

func rootPreparationProducer(id manifest.NamedResource) bool {
	kind := id.KindInfo()
	return kind.Parent || kind.IsSource()
}

func (d *Session) beginRootPlan(ctx context.Context) error {
	packages, err := d.loader.Packages(ctx)
	if err != nil {
		return err
	}
	d.packages = slices.Clone(packages)
	d.planner = &entryPlanner{
		session:     d,
		inputs:      map[string]*store.SourceArtifact{d.cfg.Input.LocalPath: d.cfg.Input},
		coordinates: map[selection.OccurrenceID]bool{},
		seen:        map[occurrenceKey]bool{},
	}
	for _, pkg := range packages {
		for i := range pkg.Occurrences {
			occurrence := pkg.Occurrences[i]
			projected := occurrenceObject(&occurrence)
			if occurrence.Object == nil {
				occurrence.Catalog = projected
			} else {
				occurrence.Object = projected
			}
			d.planner.catalog.occurrences = append(d.planner.catalog.occurrences, occurrence)
		}
		if !pkg.Independent || !slices.ContainsFunc(pkg.Occurrences, func(o selection.OccurrenceFacts) bool {
			_, _, applies := o.SourcePath()
			return applies || o.Identity.KindInfo().Runnable
		}) {
			continue
		}
		d.planner.roots = append(
			d.planner.roots,
			clusterRoot{Path: pkg.Path, seeds: pkg.Occurrences},
		)
	}
	return nil
}
