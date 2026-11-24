package discovery

import (
	"cmp"
	"context"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/core/kustpath"
	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/kustomize/patch"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type qualificationWalk struct {
	namedProjection  bool
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

func (a *Admission) applicationReach(evidence *applicationEvidence) bool {
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
		entry: a.input.OriginPath(a.path), targets: a.entryTargets,
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
	for i := range children {
		child := &children[i]
		if child.Obj.Named() != app.Source {
			continue
		}
		projected := entryResolution{object: child.Obj, input: r.input}
		source, hasDescriptor := projected.object.(entrySource)
		if !projected.checkoutEligible(r.remotes, true, hasDescriptor) {
			return true
		}
		projected.classifyCheckout(source)
		return projected.requiresGitResolution() || r.sourceReaches(app, r.input, visible)
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
		var source manifest.BaseManifest
		if i := slices.IndexFunc(output.Children, func(candidate store.Child) bool {
			return candidate.Obj.Named() == application.Source
		}); i >= 0 {
			source = output.Children[i].Obj
		}
		if w.projectedRootReaches(child.Obj, source, child.VisibleGeneration) {
			return true
		}
	}
	return false
}

func (w *qualificationWalk) projectedRootReaches(root, source manifest.BaseManifest, generated bool) bool {
	application, _ := runnableApplication(root)
	var input *store.SourceArtifact
	switch {
	case source != nil:
		selected := w.admission.classifySource(application.Source, &sourceRequest{
			object: source, reads: w.reads, scoped: true, projected: true,
			entry: w.admission.reachesEntry(&application, nil),
		})
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
	targets := w.admission.entryTargets
	if w.projectedTargets != nil {
		targets = w.projectedTargets
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
func (d *Session) CompareEntryApplications(other *Session, changes *change.Set, renderPair bool) {
	if d == nil || other == nil || changes == nil {
		return
	}
	named := slices.ContainsFunc(d.cfg.Selection.Roots, func(id manifest.NamedResource) bool { return id.Name != "" })
	if !named && (!renderPair || !d.cfg.Selection.Changed || !d.cfg.Selection.Matches(manifest.NamedResource{})) {
		return
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
		return
	}
	for _, side := range []*Session{d, other} {
		if a := side.entryAdmission; a != nil && len(applications)+len(a.changedApplications) != 0 {
			a.changedApplications = applications
			a.publishEntry(nil, manifest.NamedResource{})
		}
	}
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
