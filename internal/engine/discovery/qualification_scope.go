package discovery

import (
	"cmp"
	"context"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/core/kustpath"
	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/kustomize/patch"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type qualificationWalk struct {
	namedProjection  bool
	inventory        string
	projectedTargets []entryTarget
	q                *entryQualification
	admission        *Admission
	store            *store.Store
	reads            *store.ReadSet
	visits           map[applicationRoute]qualificationVisit
	probed           []manifest.BaseManifest
}

type entryTarget struct {
	occurrence selection.OccurrenceID
	origin     string
	coverEntry bool
}

func (t entryTarget) selected() bool {
	return t.occurrence != (selection.OccurrenceID{})
}

func (w *qualificationWalk) selectedTargetsReached() bool {
	return len(w.admission.entryTargets) != 0 && w.enoughMatches()
}

func (w *qualificationWalk) applicationContinuation(object manifest.BaseManifest) bool {
	return len(w.admission.entryTargets) == 0 || manifest.KindOf(object).Structural ||
		w.applicationProjection(object)
}

func (w *qualificationWalk) entryFrontier(ctx context.Context, input *store.SourceArtifact, path string) (bool, error) {
	return w.q.loader.EntryFrontier(ctx, input, path, w.applicationProjection)
}

func (a *Admission) applicationReach(evidence *applicationEvidence, inventory string) bool {
	evidence.reaches, evidence.descendant = nil, false
	if evidence.object.Named().ReportHidden() {
		return false
	}
	material := evidence.source.artifact
	if evidence.lookupReach && material != nil && material.Provider == store.ProviderGenerator {
		return material.Available()
	}
	origin := a.entryOrigin(&evidence.application, material)
	reach := entryReach{
		entry: a.input.OriginPath(inventory), targets: a.entryTargets,
		packageReaches: a.generatedEntryPackageReaches,
	}
	if q := a.qualification; q != nil {
		reach.winners = q.winners
	}
	return reach.includes(origin, evidence, true)
}

type entryReach struct {
	entry          string
	targets        []entryTarget
	winners        map[selection.OccurrenceID]manifest.NamedResource
	packageReaches func(*applicationEvidence, string) bool
}

func (r entryReach) includes(origin string, evidence *applicationEvidence, capture bool) bool {
	if origin == "" {
		return false
	}
	targets := r.targets
	if len(targets) == 0 {
		targets = []entryTarget{{origin: r.entry}}
	}
	for _, target := range targets {
		selected := target.selected()
		relation := kustpath.RelateHost(origin, target.origin)
		switch {
		case !selected && relation == kustpath.HostEqual:
			return true
		case !evidence.visibleGeneration, selected && r.winners[target.occurrence] != (manifest.NamedResource{}):
			continue
		case !selected && relation == kustpath.HostDescendant:
			return true
		}
		if !relation.Contains() ||
			!kustpath.RelateHost(r.entry, target.origin).Contains() || !r.packageReaches(evidence, target.origin) {
			continue
		}
		if !capture || !selected {
			evidence.descendant = !selected
			return true
		}
		evidence.reaches = append(evidence.reaches, target.occurrence)
		// A reached occurrence inside the entry proves its generated ancestor covers the entry.
		if target.coverEntry && kustpath.RelateHost(origin, r.entry) == kustpath.HostAncestor {
			evidence.descendant = true
		}
	}
	return len(evidence.reaches) != 0
}

func (a *Admission) generatedEntryPackageReaches(evidence *applicationEvidence, target string) bool {
	input := evidence.source.artifact
	if input == nil {
		return false
	}
	ks, _ := evidence.object.(*manifest.Kustomization)
	packageLoader := a.loader
	if q := a.qualification; q != nil {
		packageLoader = q.loader
	}
	return packageLoader.EntryPackageReached(input, evidence.application.Path, ks, target)
}

func (a *Admission) projectedEntry(named bool) bool {
	source := a.loader.SourceFacts(manifest.BootstrapSourceID, nil).Context
	projections := source.TemplateProjections
	if len(projections) == 0 || !named && !source.PlainEntry {
		return false
	}
	var reads store.ReadSet
	reads.Init(a.store, manifest.BootstrapSourceID, nil)
	defer a.store.Retire(&reads)
	defer reads.ReleaseCapture()
	walk := qualificationWalk{admission: a, reads: &reads, namedProjection: named}
	for _, producer := range slices.SortedFunc(maps.Keys(projections), manifest.NamedResource.Compare) {
		for _, object := range projections[producer].Objects {
			if !manifest.KindOf(object).Runnable && walk.projectedApplication(object) {
				return true
			}
		}
	}
	return false
}

func (a *Admission) projectedEntryPatches() bool {
	projections := a.loader.SourceFacts(manifest.BootstrapSourceID, nil).Context.TemplateProjections
	for _, projection := range projections {
		for _, object := range projection.Objects {
			if projectedPatch(object).patches {
				return true
			}
		}
	}
	return false
}

type projectedGeneration struct {
	patches bool
	valid   bool
}

func projectedPatch(object manifest.BaseManifest) projectedGeneration {
	if _, applies := manifest.ApplicationFor(object); !applies || manifest.KindOf(object).Runnable {
		return projectedGeneration{}
	}
	generation, err := manifest.GeneratedObjects(object)
	return projectedGeneration{patches: err != nil || len(generation.Patches) != 0, valid: err == nil}
}

func (w *qualificationWalk) changedDefinitionTargets() []entryTarget {
	a := w.admission
	changes := a.loader.Options.Changes
	if changes == nil {
		return nil
	}
	var targets []entryTarget
	for id := range a.loader.Index.Records {
		if !id.KindInfo().Structural || !changes.Contains(a.loader.Index.Records[id].File) {
			continue
		}
		occurrence, exists := a.loader.Occurrence(id)
		if !exists || !occurrence.Located || occurrence.Mounted == nil {
			continue
		}
		location := occurrence.Declaration
		origin := occurrence.Mounted.OriginPath(filepath.Join(location.Source, location.File))
		targets = append(targets, entryTarget{occurrence: location, origin: origin})
	}
	slices.SortFunc(targets, func(a, b entryTarget) int { return cmp.Compare(a.origin, b.origin) })
	return targets
}

func (w *qualificationWalk) applicationProjection(object manifest.BaseManifest) bool {
	if object == nil {
		return false
	}
	for _, projected := range w.q.loader.ProjectedObjects(object) {
		if w.projectedApplication(projected) || w.projectedValuesMayChange(object) {
			return true
		}
	}
	return false
}

// Capture all candidate ancestor edges while discovery owns loader traversal.
func (w *qualificationWalk) captureEntryReach(id manifest.NamedResource) *selection.EntryReach {
	a := w.admission
	input := a.Input()
	if input == nil {
		return nil
	}
	entry := a.input.OriginPath(a.path)
	origin := input.OriginPath(input.LocalPath)
	var targets []entryTarget
	edges := map[[2]string]bool{}
	for _, target := range a.entryTargets {
		if w.q.winners[target.occurrence] != (manifest.NamedResource{}) || !safepath.Contains(origin, target.origin) {
			continue
		}
		targets = append(targets, target)
		for dir := filepath.Dir(target.origin); safepath.Contains(origin, dir); dir = filepath.Dir(dir) {
			path, _ := filepath.Rel(origin, dir)
			edges[[2]string{dir, target.origin}] = w.q.loader.EntryPackageReached(input, path, nil, target.origin)
			if dir == origin {
				break
			}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	reachable := preparedEntryReach{
		input: input, entry: entry, targets: targets, edges: edges,
		remotes: a.remotes, sources: a.resolvedSources.snapshot(),
	}
	return &selection.EntryReach{Producer: id, Possible: reachable.possible}
}

type preparedEntryReach struct {
	input   *store.SourceArtifact
	entry   string
	targets []entryTarget
	edges   map[[2]string]bool
	remotes []string
	sources map[manifest.NamedResource]entryResolution
}

func (r *preparedEntryReach) possible(objects []manifest.BaseManifest) bool {
	for _, object := range objects {
		if manifest.KindOf(object).Runnable {
			return true
		}
		generation, err := patch.PrepareGeneration(object, manifest.WorkScope{})
		if err != nil {
			return true
		}
		for i := range generation.Children {
			child := &generation.Children[i]
			application, applies := runnableApplication(child.Obj)
			if applies && r.applicationPossible(&application, child.VisibleGeneration, generation.Children) {
				return true
			}
		}
	}
	return false
}

func (r *preparedEntryReach) applicationPossible(
	app *manifest.EntryApplication, visible bool, children []store.Child,
) bool {
	current := r.sources[app.Source]
	if current.artifact != nil {
		return !current.artifact.Reads.Valid() || r.sourceReaches(app, current.artifact, visible)
	}
	if current.object != nil && !current.projected {
		return true
	}
	if source := generationSource(app, children); source != nil {
		projected := entryResolution{object: source.Obj, input: r.input}
		if current.sync != nil && manifest.Equal(current.sync.source, source.Obj) {
			pair, valid := runnableApplication(current.sync.application)
			if valid && pair.Source == app.Source && pair.Path == app.Path {
				projected.sync = current.sync
			}
		}
		return !projected.classifyCheckout(app.Source, r.remotes, true) || projected.requiresGitResolution() ||
			r.sourceReaches(app, r.input, visible)
	}

	return true
}

func (r *preparedEntryReach) sourceReaches(
	app *manifest.EntryApplication, source *store.SourceArtifact, visible bool,
) bool {
	origin := source.ApplicationOrigin(app.Path)
	unknown := false
	reach := entryReach{
		entry: r.entry, targets: r.targets,
		packageReaches: func(_ *applicationEvidence, target string) bool {
			if source == r.input && r.edges[[2]string{origin, target}] {
				return true
			}
			// An incomplete package view cannot rule out a resource edge.
			unknown = true
			return false
		},
	}
	return reach.includes(origin, &applicationEvidence{visibleGeneration: visible}, false) || unknown
}

func (w *qualificationWalk) projectedValuesMayChange(object manifest.BaseManifest) bool {
	if w.namedProjection || len(w.admission.entryTargets) != 0 {
		for ref := range manifest.References(object, manifest.ReferenceRelations) {
			if keyed, data := ref.DataInput(); keyed && data {
				return true
			}
		}
	}
	return false
}

func (w *qualificationWalk) projectedApplication(object manifest.BaseManifest) bool {
	if _, applies := manifest.ApplicationFor(object); !applies {
		return object.Named().Name == "" && (w.namedProjection || len(w.admission.entryTargets) != 0)
	}
	if _, applies := runnableApplication(object); applies {
		return w.projectedRootReaches(object, nil, false)
	}

	output, err := w.admission.prepareGeneration(object, w.reads.WorkScope())
	if err != nil {
		return true
	}
	for i := range output.Children {
		child := &output.Children[i]
		application, applies := runnableApplication(child.Obj)
		if !applies {
			continue
		}
		source := generationSource(&application, output.Children)
		if w.projectedRootReaches(child.Obj, source, child.VisibleGeneration) {
			return true
		}
	}
	return false
}

func (w *qualificationWalk) projectedRootReaches(root manifest.BaseManifest, source *store.Child, generated bool) bool {
	application, _ := runnableApplication(root)
	targets := w.admission.entryTargets
	if w.projectedTargets != nil {
		targets = w.projectedTargets
	}
	var input *store.SourceArtifact
	switch {
	case source != nil:
		request := sourceRequest{
			object: source.Obj, reads: w.reads, scoped: true, projected: true,
			entry: w.admission.reachesEntry(&application, nil),
		}
		// A template pair can select a producer for a located application.
		// Until selection locates the application, the source needs checkout authority.
		if len(targets) != 0 {
			request.sync = &syncSource{source: source.Obj, application: root, predicted: true}
		}
		selected := w.admission.classifySource(application.Source, &request)
		input = selected.artifact
		if input == nil {
			if selected.requiresGitResolution() {
				return true
			}
			if !selected.provider.WorkingTree() {
				return false
			}
			input = w.admission.Input()
		}
	case application.Source == manifest.BootstrapSourceID:
		input = w.admission.Input()
	default:
		input, _ = w.reads.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
		if input == nil || !input.Reads.Valid() {
			return true
		}
	}
	target := input.ApplicationOrigin(application.Path)
	// Named discovery can start before selection supplies the declaration coordinates.
	if w.namedProjection && safepath.Contains(target, w.admission.input.OriginPath(w.admission.path)) {
		return true
	}
	reach := entryReach{
		entry: w.admission.input.OriginPath(w.admission.path), targets: targets,
		winners: w.admission.qualification.winners, packageReaches: w.admission.generatedEntryPackageReaches,
	}
	return reach.includes(target, &applicationEvidence{
		object: root, application: application, visibleGeneration: generated,
		source: entryResolution{artifact: input},
	}, false)
}

// CompareEntryApplications captures possible application changes from both declarations.
func (d *Session) CompareEntryApplications(
	ctx context.Context, other *Session, changes *change.Set, renderPair bool,
) error {
	if d == nil || other == nil || changes == nil {
		return nil
	}
	named := slices.ContainsFunc(d.cfg.Selection.Roots, func(id manifest.NamedResource) bool { return id.Name != "" })
	if !named && (!renderPair || !d.cfg.Selection.Changed || !d.cfg.Selection.Matches(manifest.NamedResource{})) {
		return nil
	}
	if !named && d.splitEntryPackage(other) {
		if err := d.compareEntryPackages(ctx, other); err != nil {
			return err
		}
	}
	local, peer := d.changedEntryApplications(changes), other.changedEntryApplications(changes)
	applications := slices.Concat(local, peer)
	if !named {
		patched := slices.ContainsFunc(applications, func(object manifest.BaseManifest) bool {
			return projectedPatch(object).patches
		})
		if len(local) != 0 && len(peer) != 0 && patched {
			d.loader.Options.Session.DeferEntryComparison()
			other.loader.Options.Session.DeferEntryComparison()
		}
		return nil
	}
	for _, side := range []*Session{d, other} {
		if a := side.entryAdmission; a != nil && len(applications)+len(a.changedApplications) != 0 {
			a.changedApplications = applications
			a.publishEntry(nil, manifest.NamedResource{})
		}
	}
	return nil
}

func (d *Session) splitEntryPackage(other *Session) bool {
	left, right := d.entryCoordinates, other.entryCoordinates
	return left != nil && right != nil && left.packagePath != "" && right.packagePath != "" &&
		(left.packagePath != d.cfg.Path || right.packagePath != other.cfg.Path)
}

type entryInventoryProposal struct {
	root    *manifest.Kustomization
	binding Binding
	frame   *declarationPreparation
}

func (d *Session) compareEntryPackages(ctx context.Context, other *Session) error {
	sides := [2]*Session{d, other}
	var proposals [2]entryInventoryProposal
	defer func() {
		for i := range proposals {
			proposals[i].discard(sides[i].cfg.Store)
		}
	}()
	for i, side := range sides {
		proposal, err := side.captureEntryInventory(ctx)
		proposals[i] = proposal
		if err != nil {
			return err
		}
	}
	left, right := proposals[0].frame, proposals[1].frame
	if left == nil || right == nil || left.sameEntryInventory(right) {
		return nil
	}
	for i, side := range sides {
		if err := side.installEntryInventory(ctx, &proposals[i]); err != nil {
			return err
		}
	}
	return nil
}

func (d *Session) captureEntryInventory(ctx context.Context) (entryInventoryProposal, error) {
	if d.cfg.Trees == nil {
		return entryInventoryProposal{}, nil
	}
	root, binding, err := d.admission().captureBinding(ctx, d.cfg.Store, d.entryCoordinates.packagePath)
	proposal := entryInventoryProposal{root: root, binding: binding}
	if err != nil || root == nil || binding.Reads == nil {
		return proposal, err
	}
	input, _ := kustomize.SourceInput(binding.Reads, root)
	if !input.Available() || !input.Reads.Valid() {
		return proposal, nil
	}
	frame, _ := d.preparePackageInventory(ctx, root, input, nil)
	if frame != nil && frame.complete && binding.Reads.Valid() {
		proposal.frame = frame
	}
	return proposal, ctx.Err()
}

func (p *entryInventoryProposal) discard(st *store.Store) {
	binding := &p.binding
	if binding.Reads != nil {
		binding.Reads.ReleaseCapture()
		st.Retire(binding.Reads)
		if binding.qualification != nil {
			binding.qualification.proposal = nil
		}
	}
	if binding.support != nil {
		binding.support.ReleaseCapture()
	}
}

func (p *declarationPreparation) sameEntryInventory(other *declarationPreparation) bool {
	if p.err != nil || other.err != nil || len(p.children) != len(other.children) {
		return false
	}
	i := 0
	for ; i < len(p.children); i++ {
		left, right := &p.children[i], &other.children[i]
		if left.Obj.Named() != right.Obj.Named() || !manifest.ValueEqual(left.Doc, right.Doc) {
			break
		}
	}
	if i == len(p.children) {
		return true
	}
	documents := make(map[manifest.NamedResource][]map[string]any, len(other.children)-i)
	for j := i; j < len(other.children); j++ {
		child := &other.children[j]
		id := child.Obj.Named()
		documents[id] = append(documents[id], child.Doc)
	}
	for j := i; j < len(p.children); j++ {
		child := &p.children[j]
		id := child.Obj.Named()
		docs := documents[id]
		match := slices.IndexFunc(docs, func(doc map[string]any) bool { return manifest.ValueEqual(child.Doc, doc) })
		if match == -1 {
			return false
		}
		last := len(docs) - 1
		docs[match] = docs[last]
		documents[id] = docs[:last]
	}
	return true
}

func (d *Session) installEntryInventory(ctx context.Context, proposal *entryInventoryProposal) error {
	binding := &proposal.binding
	binding.Failure = proposal.frame.err
	a := d.entryAdmission
	a.publishEntry(binding.support, binding.Producer)
	accepted, err := d.installBinding(ctx, proposal.root, binding)
	binding.Reads = nil
	if accepted {
		d.entryDemand = true
		binding.support = nil
	} else {
		a.publishEntry(nil, manifest.NamedResource{})
	}
	return err
}

func (d *Session) changedEntryApplications(changes *change.Set) []manifest.BaseManifest {
	projections := d.loader.SourceFacts(manifest.BootstrapSourceID, nil).Context.TemplateProjections
	if len(projections) == 0 {
		return nil
	}
	affected := make(map[manifest.NamedResource]bool)
	for _, file := range changes.Paths() {
		for reader := range d.loader.Index.ReadersOfFile(file) {
			affected[reader.Reader] = true
		}
	}
	var applications []manifest.BaseManifest
	for _, id := range slices.SortedFunc(maps.Keys(projections), manifest.NamedResource.Compare) {
		record, _ := d.loader.Index.Lookup(id)
		if !affected[id] && !changes.Contains(record.File) {
			continue
		}
		for _, object := range projections[id].Objects {
			if _, application := manifest.ApplicationFor(object); application && !manifest.KindOf(object).Runnable {
				applications = append(applications, object)
			}
		}
	}
	return applications
}
