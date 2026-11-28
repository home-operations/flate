package discovery

import (
	"context"
	"io/fs"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/core/kustpath"
	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type clusterRoot struct {
	Path         string
	Occurrences  []selection.OccurrenceFacts
	anchors      []bootstrapAnchor
	pinned       manifest.DiscoveryIndex
	sources      map[manifest.NamedResource]entryResolution
	views        sourceViews
	parsed       *loader.ParsedInputs
	seeds        []selection.OccurrenceFacts
	packageBuild *plannedPackage
	qualifying   []selection.OccurrenceID
	reached      map[selection.OccurrenceID]struct{}
}

type bootstrapAnchor struct {
	target string
	source manifest.NamedResource
}

type occurrenceProjection struct {
	path, namespace string
	input           *store.SourceArtifact
}

type entryPlanner struct {
	directoryInventory bool
	roots              []clusterRoot
	index              int
	queue              []rootVisit
	head               int
	parsed             *loader.ParsedInputs
	started            bool
	scope              rootScope
	catalog            rootScope
	catalogView        rootScope
	coordinates        map[selection.OccurrenceID]bool
	seen               map[occurrenceKey]rootVisitState
	routes             map[rootRoute]struct{}

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
		if len(root.anchors) > 1 {
			split := root.partitionAnchors(d.cfg.Input)
			planner.roots = slices.Replace(planner.roots, planner.index, planner.index+1, split...)
			planner.started = false
			continue
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
	p.queue = p.queue[:0]
	p.head = 0
	clear(p.coordinates)
	clear(p.seen)
	clear(p.routes)
	p.scope = rootScope{occurrences: seeds}
	boundary := ""
	input := p.session.cfg.Input
	for i := range seeds {
		seed := &seeds[i]
		application, applies := seed.EntryApplication()
		if !applies {
			continue
		}
		target, contains := configuredApplicationTarget(input, application.Path, seed)
		if contains && (boundary == "" || len(target) < len(boundary)) {
			boundary = target
		}
	}
	p.catalogView = p.catalog
	if boundary != "" {
		p.catalogView.occurrences = nil
		for candidate := range p.catalog.objects() {
			if containsDeclaration(boundary, &candidate.Declaration, candidate.Mounted, input) {
				p.catalogView.occurrences = append(p.catalogView.occurrences, *candidate)
			}
		}
	}
	if err := p.prepareScope(ctx, &p.scope); err != nil {
		return err
	}
	for i := range p.scope.occurrences {
		p.coordinates[p.scope.occurrences[i].Declaration] = true
	}
	p.enqueue(&p.scope)
	return nil
}

func (d *Session) unpinnedRoots(pending []clusterRoot) []clusterRoot {
	slices.SortFunc(pending, func(a, b clusterRoot) int { return strings.Compare(a.Path, b.Path) })
	claims, _, owners := loader.NewPackageClaimIndex(len(pending), func(i int) string { return pending[i].Path },
		func(add func(int, string)) {
			for i := range pending {
				for prefix := range pending[i].pinned.Claims {
					add(i, prefix)
				}
			}
		})
	active := make([]bool, len(pending))
	for i := range active {
		active[i] = true
	}
	claimed := func(child, owner int) bool {
		for range claims.ClaimOwners(pending[child].Path+"/", manifest.NamedResource{}, manifest.NamedResource{},
			func(id manifest.NamedResource) bool {
				return owners[id] == owner
			}) {
			return true
		}
		return false
	}
	var roots []clusterRoot
	for {
		selected := pinnedRoot(claims, owners, pending, active)
		if selected < 0 {
			break
		}
		roots = append(roots, pending[selected])
		active[selected] = false
		for i := range pending {
			if active[i] && claimed(i, selected) {
				active[i] = false
			}
		}
	}
	return roots
}

type rootVisit struct {
	occurrence *selection.OccurrenceFacts
	scope      *rootScope
	first      bool
}

type rootApplication struct {
	visit       *rootVisit
	object      manifest.BaseManifest
	application manifest.EntryApplication
	state       *rootVisitState
	origin      *selection.OccurrenceFacts
}

type rootVisitState struct {
	index    int
	initial  bool
	prepared *preparedRootRoute
}

type rootRoute struct {
	origin selection.OccurrenceID
	source scopedSource
	index  int
}

type rootTarget struct {
	source    scopedSource
	reference manifest.Reference
	path      string
	local     bool
}

type preparedRootRoute struct {
	scope  rootScope
	target rootTarget
}

type scopedSource struct {
	input       *store.SourceArtifact
	declaration selection.OccurrenceID
	entry       entryScope
}

type rootScope struct {
	occurrences []selection.OccurrenceFacts
	parent      *rootScope
	seed        *selection.OccurrenceFacts
	targetInput *store.SourceArtifact
	targets     []scopedEntryTarget
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

func (p *entryPlanner) followOutput(
	ctx context.Context, input *store.SourceArtifact, resolved *rootApplication, absolute string,
) ([]selection.OccurrenceFacts, error) {
	visit := resolved.visit
	object := resolved.object
	children, err := p.occurrences(ctx, input, object, absolute)
	key := occurrenceIdentity(visit.occurrence)
	key.input = p.inputs[visit.occurrence.Declaration.Source]
	configured := p.session.cfg.Input
	if err != nil || !p.objects[key].VisibleGeneration ||
		!configured.MatchesOrigin(p.session.cfg.Path, input.OriginPath(absolute)) {
		return children, err
	}
	if !slices.ContainsFunc(children, func(child selection.OccurrenceFacts) bool { return child.RequiresBuild }) {
		return children, nil
	}
	ks, ok := object.(*manifest.Kustomization)
	if !ok || p.session.cfg.Trees == nil || !p.session.canPrepare(ks) {
		return children, nil
	}
	if _, err := p.session.prepareProducer(ctx, ks, input, nil); err != nil {
		return nil, err
	}
	prepared := p.session.prepared[ks.Named()]
	if prepared != nil && prepared.inventoryReady() && prepared.support.Valid() {
		return p.preparedOccurrences(input, visit.occurrence.Declaration, prepared.children), nil
	}
	return children, nil
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
	local := input.WorkingTree()
	if !seed && local && cfg.Changes != nil && p.singleAnchoredRoot(root) {
		scan, target := cfg.Input.OriginPath(cfg.Path), input.OriginPath(absolute)
		// An anchored single entry has no cluster membership to learn from disjoint workloads.
		if !kustpath.RelateHost(scan, target).Overlaps() {
			return "", false
		}
	}
	return absolute, local
}

func (p *entryPlanner) singleAnchoredRoot(root *clusterRoot) bool {
	return len(p.roots) == 1 && len(root.anchors) != 0
}

func (p *entryPlanner) bootstrapRoot(
	root *clusterRoot,
	occurrence *selection.OccurrenceFacts,
	input *store.SourceArtifact,
	application *manifest.EntryApplication,
) bool {
	configured := p.session.cfg.Input
	packagePath := configured.OriginPath(filepath.Join(configured.LocalPath, root.Path))
	origin := configured.OriginPath(filepath.Join(configured.LocalPath, application.Path))
	entry := configured.MatchesOrigin(p.session.cfg.Path, packagePath)
	if !entry && safepath.Contains(packagePath, origin) &&
		!containsDeclaration(origin, &occurrence.Declaration, occurrence.Mounted, configured) {
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
	var generated []selection.OccurrenceFacts
	for i := range scope.occurrences {
		occurrence := &scope.occurrences[i]
		object := occurrence.EffectiveObject()
		kind := manifest.KindOf(object)
		sync, err := p.scopeGeneration(occurrence, object)
		if err != nil {
			return err
		}
		generated = append(generated, sync...)
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
	if len(generated) != 0 {
		scope.occurrences = append(slices.Clone(scope.occurrences), generated...)
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
	resolved *rootApplication,
	reference *manifest.Reference,
) (scopedSource, error) {
	id := reference.NamedResource
	visit := resolved.visit
	scope := visit.scope
	selected, producers := scope.source(id)
	if selected == nil && len(producers) == 0 {
		selected, producers = p.catalogSource(id)
		scope = &p.catalogView
	}
	var resolution scopedSource
	if selected != nil {
		resolution.declaration = selected.Declaration
		if selected.RequiresBuild {
			return resolution, nil
		}
	}

	admission := p.session.admission()
	before := p.session.resolvedSources.snapshot()[id]
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
			return resolution, err
		}
		group.object, group.document = child.Obj, child.Doc
		group.projected = child.VisibleGeneration
		if group.projected {
			group.producers = append(group.producers, occurrenceIdentity(selected))
			group.sync = p.scopedSyncSource(selected, &child, scope, scope == visit.scope)
		}
	}
	entry := entryUnscoped
	if resolved.application.Source == id {
		entry = p.scopedEntry(id, group.object, scope)
	}
	declaration := entryResolution{object: group.object, input: admission.input}
	priorEntry := before.entry && before.sameDeclaration(&declaration) &&
		(before.sync == nil || before.sync.equal(group.sync))
	group.entry = entry == entryAccepted || entry == entryUnscoped && priorEntry
	if entry == entryRejected {
		group.sync = nil
	}
	if _, err := p.session.resolveEntrySource(ctx, admission, id, &group); err != nil {
		return resolution, err
	}
	binding := p.session.resolvedSources.snapshot()[id]
	binding.producers = producers
	binding.entryScoped = entry != entryUnscoped
	resolution.entry = entry
	resolution.input = p.recordSource(root, id, &binding, before.artifact)
	return resolution, nil
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

func physicalDeclaration(occurrence *selection.OccurrenceFacts) selection.OccurrenceID {
	declaration := occurrence.Declaration
	declaration.Source = occurrence.Mounted.OriginPath(declaration.Source)
	return declaration
}

func (s *rootScope) source(id manifest.NamedResource) (*selection.OccurrenceFacts, []occurrenceKey) {
	var selected *selection.OccurrenceFacts
	var producers []occurrenceKey
	for candidate := range s.objects() {
		if candidate.Identity == id {
			selected = candidate
		}
		targets := candidate.Targets
		if candidate.NeedsCandidates {
			targets = append(slices.Clone(targets), manifest.ProducerTargets(candidate.EffectiveObject(), id)...)
		}
		matches := func(target manifest.ProducerTarget) bool { return target.NamedResource == id }
		if slices.ContainsFunc(targets, matches) || slices.ContainsFunc(
			candidate.TemplateObjects,
			func(object manifest.BaseManifest) bool {
				if _, applies := manifest.ApplicationFor(object); !applies {
					return false
				}
				generated, _ := manifest.GeneratedTargets(object)
				return slices.ContainsFunc(generated, matches) ||
					manifest.ProducerNeedsCandidates(object) &&
						slices.ContainsFunc(manifest.ProducerTargets(object, id), matches)
			},
		) {
			producers = append(producers, occurrenceIdentity(candidate))
		}
	}
	return selected, producers
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
) (*selection.OccurrenceFacts, []occurrenceKey) {
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
	return nil, nil
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
	if binding.viewKey.mount == "" && !binding.entryScoped && !binding.projected && len(binding.producers) == 0 {
		return p.session.sourceMaterial(id, binding.artifact)
	}
	if root.sources == nil {
		root.sources = map[manifest.NamedResource]entryResolution{}
	}
	previous := root.sources[id]
	previous.merge(binding)
	root.sources[id] = previous
	if binding.viewKey.mount == "" {
		return p.session.sourceMaterial(id, binding.artifact)
	}
	if root.views == nil {
		root.views = sourceViews{}
	}
	root.views.remember(binding.viewKey, binding.artifact)
	return p.session.sourceMaterial(id, binding.artifact)
}

func (p *entryPlanner) prepareRoot(ctx context.Context, root *clusterRoot) error {
	input := p.session.cfg.Input
	occurrences := root.seeds
	if !p.rootRequiresPreparation(root, input) {
		return nil
	}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root.Path, manifest.NamedResource{})
	prepared, err := p.session.preparePackageInventory(ctx, owner, input, nil)
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
		if register {
			continue
		}
		_, generated := generatedEntryPath(&facts)
		if !rootPreparationProducer(facts.Identity.KindInfo(), generated) {
			continue
		}
		remaining--
		if original == nil {
			register = true
			continue
		}
		raw, err := p.object(ctx, original)
		application, applies := manifest.ApplicationFor(child.Obj)
		// A downstream application still needs its built declaration to expand.
		register = err != nil || !manifest.Equal(raw.Obj, child.Obj)
		if !register && original.RequiresBuild && applies {
			register = !safepath.Contains(
				filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(application.Path)),
				filepath.Join(input.LocalPath, root.Path),
			)
		}
	}
	root.seeds = result
	root.packageBuild = &plannedPackage{render: prepared.declarationRender, register: register || remaining != 0}
	return ctx.Err()
}
