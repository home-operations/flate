package discovery

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type sourceRequest struct {
	object    manifest.BaseManifest
	reference manifest.Reference
	reads     *store.ReadSet
	work      manifest.WorkScope
	scoped    bool
	producers []occurrenceKey
	entry     bool
	projected bool
}

type entrySourceGroup struct {
	reference           manifest.Reference
	producers           []occurrenceKey
	object              manifest.BaseManifest
	document            map[string]any
	refs                []manifest.NamedResource
	scoped              bool
	required            bool
	entry               bool
	projected           bool
	projectedFromParent bool
}

func entrySourceGroupFor(
	groups map[manifest.NamedResource]*entrySourceGroup,
	id manifest.NamedResource,
) *entrySourceGroup {
	group := groups[id]
	if group == nil {
		group = &entrySourceGroup{}
		groups[id] = group
	}
	return group
}

func addEntrySource(
	groups map[manifest.NamedResource]*entrySourceGroup,
	id, requester manifest.NamedResource,
	reference *manifest.Reference,
) {
	group := entrySourceGroupFor(groups, id)
	group.required = true
	if reference != nil && (reference.ReadsSourceTree() || reference.RequiresDeclaration) {
		requiresDeclaration := group.reference.RequiresDeclaration || reference.RequiresDeclaration
		group.reference = *reference
		group.reference.RequiresDeclaration = requiresDeclaration
	}
	if requester != (manifest.NamedResource{}) && !slices.Contains(group.refs, requester) {
		group.refs = append(group.refs, requester)
	}
}

func sourceReference(object manifest.BaseManifest, id manifest.NamedResource) manifest.Reference {
	if object != nil {
		for ref := range manifest.References(object, manifest.ReferenceRelations) {
			if ref.BindsSource() && ref.NamedResource == id {
				return ref
			}
		}
	}
	return manifest.Reference{}
}

func (r *sourceRequest) generatedDocument(id manifest.NamedResource, candidate, observed manifest.BaseManifest) bool {
	return r.reads != nil && candidate == observed && r.reads.GeneratedDocument(id)
}

func (a *Admission) classifySource(id manifest.NamedResource, request *sourceRequest) entryResolution {
	previous := a.resolvedSources.snapshot()[id]
	candidate := entryResolution{
		object: request.object, input: a.input, entry: request.entry, producers: request.producers,
	}
	authored := candidate.selectDeclaration(a, id, request, &previous)
	candidate.inheritEntry(&previous, request.scoped)
	if !request.scoped && candidate.projected && !candidate.entry && !candidate.entryScoped {
		candidate.entry = a.emittedEntrySource(id, candidate.object)
	}
	if candidate.synthetic {
		return candidate
	}
	if candidate.projected && !authored && previous.reusesProjection(&candidate) {
		if !request.projected && a.projectedProvider(id, request) == store.ProviderAbsent {
			return entryResolution{input: a.input, producers: previous.producers, provider: store.ProviderAbsent}
		}
		return previous
	}
	// A pending projection without a checkout descriptor MUST wait for its producer.
	pending := candidate.projected && !authored && (request.scoped || len(request.producers) == 0)
	source, hasDescriptor := candidate.object.(entrySource)
	eligible := candidate.checkoutEligible(a.remotes, pending, hasDescriptor)
	if pending && !eligible {
		candidate.provider, candidate.entry = a.projectedProvider(id, request), false
		return candidate
	}
	switch {
	case previous.ownsDeclaration(candidate.object), candidate.object == nil:
		return a.classifyMissingSource(id, request, &candidate, &previous)
	case previous.reuseClassification(&candidate, request, a):
		return previous
	}
	if eligible {
		candidate.classifyCheckout(source)
	}
	return candidate
}

func (a *Admission) emittedEntrySource(id manifest.NamedResource, declaration manifest.BaseManifest) bool {
	owner := a.store.EmissionOwner(id)
	output, _ := a.store.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
	if output == nil || !output.Complete {
		return false
	}
	// Replacing the checkout can invalidate its producer's reads before either declaration changes.
	// The two declarations MUST still belong to the complete inventory.
	source, entry := false, false
	for i := range output.Children {
		child := &output.Children[i]
		source = source || child.Obj == declaration
		if child.VisibleGeneration && !entry {
			application, applies := runnableApplication(child.Obj)
			entry = applies && application.Source == id &&
				a.input.ApplicationOrigin(application.Path) == a.input.OriginPath(a.path) &&
				a.store.GetDeclaration(child.Obj.Named()) == child.Obj
		}
		if source && entry {
			return true
		}
	}
	return false
}

func (r *entryResolution) checkoutEligible(remotes []string, pending, hasDescriptor bool) bool {
	return manifest.BootstrapSourceEligible(r.object, r.entry && (!pending || hasDescriptor), remotes)
}

func (r *entryResolution) classifyCheckout(source entrySource) {
	r.provider = store.ProviderAlias
	if source != nil {
		_, r.git = source.BootstrapAlias(r.input.LocalPath)
		if r.git.BootstrapPinned() {
			r.provider = store.ProviderCommit
		}
	}
}

func (r *entryResolution) requiresGitResolution() bool {
	return r.provider == store.ProviderCommit || r.git != nil && r.git.RecurseSubmodules
}

func (a *Admission) projectedProvider(id manifest.NamedResource, request *sourceRequest) store.SourceProvider {
	if provider, known := a.sourceProduction(id, request, a.loaderIndex()); known {
		return provider
	}
	return store.ProviderGenerator
}

func (r *entryResolution) inheritEntry(previous *entryResolution, scoped bool) {
	if scoped || previous.input != r.input {
		return
	}
	// A synthetic alias carries source-specific checkout authority.
	if r.projected && previous.synthetic && previous.provider == store.ProviderAlias &&
		previous.artifact.WorkingTree() {
		r.entry = true
		return
	}
	if !previous.sameDeclaration(r) {
		return
	}
	if previous.entryScoped {
		r.entryScoped, r.entry = true, previous.entry
		return
	}
	r.entry = r.entry || previous.entry && (!previous.projected || previous.provider != store.ProviderFetch)
}

func (r *entryResolution) sameDeclaration(candidate *entryResolution) bool {
	return r.input == candidate.input && r.object == candidate.object
}

func (r *entryResolution) reusesProjection(candidate *entryResolution) bool {
	return r.input == candidate.input && r.artifact != nil &&
		len(r.producers) != 0 && r.sameClassification(candidate)
}

func (r *entryResolution) selectDeclaration(
	a *Admission, id manifest.NamedResource, request *sourceRequest, previous *entryResolution,
) bool {
	var bootstrap manifest.BaseManifest
	if a.resolvedSources != nil {
		bootstrap = a.resolvedSources.bootstrap
	}
	object := r.authoredSource(a, id, request, previous)
	if r.object == bootstrap || previous.ownsDeclaration(r.object) {
		r.object = nil
	}
	if object == bootstrap {
		object = nil
	}
	// An unscoped declaration MUST establish authority before replacing a synthetic binding.
	unscoped := !request.scoped && !request.projected && !r.entry
	challengesSynthetic := previous.synthetic && object == nil && r.object != nil && unscoped
	if challengesSynthetic && !manifest.BootstrapSourceEligible(r.object, false, a.remotes) {
		r.object = nil
	}
	// Producer predictions MUST NOT displace the checkout that supplies their construction input.
	undeclared := r.object == nil && object == nil
	if id == manifest.BootstrapSourceID && undeclared && !previous.projected && !request.reference.RequiresDeclaration {
		r.bindCheckout(bootstrap, previous, request.scoped)
		return false
	}
	if request.reads != nil {
		observed := request.reads.GetObject(id)
		if observed == bootstrap || previous.ownsDeclaration(observed) {
			observed = nil
		}
		r.object = cmp.Or(r.object, observed, previous.object)
	}
	switch {
	case !request.scoped && object != nil:
		r.object = object
	case r.object == nil && !request.scoped && a.loader != nil && a.loader.IsFileIndexed(id):
		r.object = a.loader.CatalogObject(id)
	}
	if r.object == nil && !request.scoped && previous.projected {
		r.object = previous.object
	}
	r.projected = r.isProjected(id, request, previous, object)
	return object != nil
}

func (r *entryResolution) bindCheckout(
	bootstrap manifest.BaseManifest, previous *entryResolution, scoped bool,
) {
	if !scoped && previous.input == r.input && previous.object != nil && !previous.projected {
		*r = *previous
		return
	}
	r.object = bootstrap
	r.synthetic, r.provider, r.artifact = true, store.ProviderInput, r.input
}

func (r *entryResolution) authoredSource(
	a *Admission, id manifest.NamedResource, request *sourceRequest, previous *entryResolution,
) manifest.BaseManifest {
	var object manifest.BaseManifest
	if request.reads == nil {
		object = a.store.GetDeclaration(id)
	} else {
		object = request.reads.GetDeclaration(id)
	}
	if !previous.ownsDeclaration(object) {
		return object
	}
	r.placeholder = previous.placeholder
	if r.placeholder == nil && r.object != nil && !previous.ownsDeclaration(r.object) {
		r.placeholder = new(object)
	}
	return nil
}

func (r *entryResolution) isProjected(
	id manifest.NamedResource, request *sourceRequest, previous *entryResolution, authored manifest.BaseManifest,
) bool {
	projectedRequest := request.projected && r.object == request.object
	projectedDeclaration := previous.projected && r.object == previous.object
	return projectedRequest || projectedDeclaration || request.generatedDocument(id, r.object, authored)
}

func (r *entryResolution) sameClassification(candidate *entryResolution) bool {
	return r.object == candidate.object && r.entry == candidate.entry &&
		r.entryScoped == candidate.entryScoped && r.projected == candidate.projected
}

type entryScope uint8

const (
	entryUnscoped entryScope = iota
	entryRejected
	entryAccepted
)

func (p *entryPlanner) scopedEntry(
	id manifest.NamedResource, source manifest.BaseManifest, scope *rootScope,
) entryScope {
	if !manifest.BootstrapSourceEligible(source, true, nil) {
		return entryUnscoped
	}
	admission := p.session.admission()
	var target string
	for occurrence := range scope.objects() {
		candidate := admission.scopedCheckoutTarget(occurrence)
		if candidate.application.Source == id && len(candidate.path) > len(target) {
			target = candidate.path
		}
	}
	if target == "" {
		return entryUnscoped
	}
	// A broader target MUST NOT displace a narrower self-applying entry.
	for occurrence := range scope.objects() {
		candidate := admission.scopedCheckoutTarget(occurrence)
		other := candidate.application
		observed := admission.entryOrigin(&other, nil)
		current := cmp.Or(observed, candidate.path)
		if other.Source == id || current == "" || !safepath.Contains(target, current) {
			continue
		}
		narrower := current != target
		if !narrower && manifest.BootstrapSourceEligible(source, false, admission.remotes) {
			continue
		}
		if observed != "" {
			return entryRejected
		}
		selected, _ := scope.source(other.Source)
		if selected != nil && !selected.RequiresBuild &&
			manifest.BootstrapSourceEligible(occurrenceObject(selected), narrower, admission.remotes) {
			return entryRejected
		}
	}
	return entryAccepted
}

type scopedEntryTarget struct {
	application manifest.EntryApplication
	path        string
}

func (a *Admission) scopedCheckoutTarget(occurrence *selection.OccurrenceFacts) scopedEntryTarget {
	object := occurrenceObject(occurrence)
	application, valid := runnableApplication(object)
	if !valid || !object.EmissionPolicy().MatchesNamespace(occurrence.Identity) {
		return scopedEntryTarget{}
	}
	return scopedEntryTarget{application: application, path: a.checkoutTarget(&application, occurrence)}
}

func (a *Admission) reachesEntry(
	application *manifest.EntryApplication, occurrence *selection.OccurrenceFacts,
) bool {
	if a.input == nil || a.path == "" {
		return false
	}
	entry := a.input.OriginPath(a.path)
	if a.entryOrigin(application, nil) == entry {
		return true
	}
	return occurrence != nil && a.checkoutTarget(application, occurrence) == entry
}

func (a *Admission) checkoutTarget(
	application *manifest.EntryApplication, occurrence *selection.OccurrenceFacts,
) string {
	if a.input == nil || occurrence.RequiresBuild || occurrence.Declaration.File == "" {
		return ""
	}
	if object := occurrenceObject(occurrence); object == nil ||
		!object.EmissionPolicy().MatchesNamespace(occurrence.Identity) {
		return ""
	}
	target := a.input.ApplicationOrigin(application.Path)
	if target == "" || application.Path == "" && target != a.input.OriginPath(a.path) {
		return ""
	}
	mount := occurrence.Mounted
	if occurrence.Declaration.Source == a.input.LocalPath {
		mount = a.input
	}
	if !mount.WorkingTree() || mount.LocalPath != occurrence.Declaration.Source {
		return ""
	}
	declaration := mount.OriginPath(filepath.Join(mount.LocalPath, occurrence.Declaration.File))
	if !safepath.Contains(target, declaration) {
		return ""
	}
	return target
}

func (a *Admission) entryOrigin(application *manifest.EntryApplication, observed *store.SourceArtifact) string {
	if application.Source == (manifest.NamedResource{}) {
		return ""
	}
	if observed == nil && a.store != nil {
		observed, _ = a.store.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	}
	if observed == nil || !observed.Reads.Valid() {
		selected := a.resolvedSources.snapshot()[application.Source]
		if selected.input == a.input {
			observed = selected.artifact
		}
	}
	return observed.ApplicationOrigin(application.Path)
}

// A source MUST NOT replace its construction input with its own rendered declaration.
func (r *entryResolution) reuseClassification(
	candidate *entryResolution,
	request *sourceRequest,
	a *Admission,
) bool {
	if candidate.object != nil && r.sameClassification(candidate) {
		switch {
		case r.input == candidate.input:
			return true
		case r.artifact.WorkingTree():
			r.input, r.artifact = candidate.input, nil
			return true
		}
	}
	if request.scoped || r.synthetic || r.artifact == nil {
		return false
	}
	if r.input != candidate.input || r.object == candidate.object {
		return false
	}
	id := r.object.Named()
	index := a.loaderIndex()
	owner := a.store.EmissionOwner(id)
	if owner == (manifest.NamedResource{}) {
		record, _ := index.Lookup(id)
		if record.Builder == "" || record.Declaration != candidate.object {
			return false
		}
		owner = record.BuilderOwner
	}
	if owner == (manifest.NamedResource{}) {
		return false
	}
	ownsInput := func(production manifest.Production) bool {
		return production.Source == id && production.Owner == owner
	}
	if request.reads != nil {
		return slices.ContainsFunc(request.reads.Productions(id, index), ownsInput)
	}
	return slices.ContainsFunc(a.store.Productions(id, index), ownsInput)
}

func (a *Admission) classifyMissingSource(
	id manifest.NamedResource,
	request *sourceRequest,
	candidate *entryResolution,
	previous *entryResolution,
) entryResolution {
	*candidate = entryResolution{input: a.input, producers: candidate.producers, provider: store.ProviderAbsent}
	if !request.scoped && previous.input == a.input && len(candidate.producers) == 0 {
		candidate.producers = previous.producers
	}
	var observed store.Artifact
	if request.reads != nil && request.reads.ID() != id && id != manifest.BootstrapSourceID {
		observed = request.reads.GetArtifact(id, store.ArtifactContent)
	} else {
		observed = a.store.GetArtifact(id, store.ArtifactContent)
	}
	if owned, ok := observed.(*store.SourceArtifact); ok && owned.Reads.Valid() &&
		!owned.SameMaterial(previous.artifact, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin) &&
		owned.Provider != store.ProviderInput {
		candidate.provider = owned.Provider
		return *candidate
	}
	if provider, known := a.sourceProduction(id, request, a.loaderIndex()); known {
		candidate.provider = provider
		return *candidate
	}
	for i := range candidate.producers {
		if a.sourceOutput(candidate.producers[i].identity, nil) == nil {
			candidate.provider = store.ProviderGenerator
			return *candidate
		}
	}
	switch {
	case len(candidate.producers) != 0, request.reference.RequiresDeclaration:
		return *candidate
	case id == manifest.BootstrapSourceID:
		candidate.bindCheckout(a.resolvedSources.bootstrap, previous, request.scoped)
		return *candidate
	case previous.synthetic:
		if previous.input != a.input {
			previous.input, previous.artifact = a.input, nil
		}
		return *previous
	case request.reference.ReadsSourceTree() && id.CanBindLocal():
		candidate.synthetic, candidate.provider = true, store.ProviderAlias
	}
	return *candidate
}

// Provider captures the same source decision for diagnostics before material exists.
func (a *Admission) Provider(
	id manifest.NamedResource,
	reads *store.ReadSet,
	index *manifest.DiscoveryIndex,
) store.SourceProvider {
	var objects interface {
		GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
		GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	} = reads
	if reads == nil {
		objects = a.store
	}
	artifact, _ := objects.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	resolution, accepted := a.resolvedSources.snapshot()[id]
	composes := resolution.object != nil && len(resolution.object.Composition().Includes) != 0
	if artifact.Available() && (!composes || artifact.Extent == store.ExtentComposed) {
		return artifact.Provider
	}
	// A local checkout can supply the base of a Git include, but acquisition supplies the composed source.
	if composes {
		return store.ProviderFetch
	}
	if accepted && resolution.object != nil {
		return resolution.provider
	}
	declaration := objects.GetDeclaration(id)
	indexed, _ := index.Lookup(id)
	if id.KindInfo().Runnable && (declaration != nil || indexed.Declaration != nil) {
		return store.ProviderFetch
	}
	if provider, known := a.sourceProduction(id, &sourceRequest{reads: reads, scoped: true}, index); known {
		return provider
	}
	if accepted {
		return resolution.provider
	}
	if id == manifest.BootstrapSourceID && declaration == nil {
		return store.ProviderInput
	}
	if declaration != nil || id.KindInfo().SourceArtifact {
		return store.ProviderFetch
	}
	return store.ProviderAbsent
}

func (a *Admission) sourceProduction(
	id manifest.NamedResource,
	request *sourceRequest,
	index *manifest.DiscoveryIndex,
) (store.SourceProvider, bool) {
	var productions []manifest.Production
	if request.reads != nil {
		productions = request.reads.Productions(id, index)
	} else {
		productions = a.store.Productions(id, index)
	}
	local := func(p manifest.Production) bool {
		return p.Declaration != id &&
			(p.Declaration != (manifest.NamedResource{}) || p.Owner != (manifest.NamedResource{}))
	}
	produced := false
	for i := range productions {
		production := &productions[i]
		if !local(*production) {
			continue
		}
		produced = true
		owner := production.Owner
		if production.Declaration.KindInfo().Runnable {
			owner = production.Declaration
		}
		output := a.sourceOutput(owner, request.reads)
		if output == nil ||
			slices.ContainsFunc(output.Children, func(child store.Child) bool {
				return child.Obj != nil && child.Obj.Named() == id
			}) {
			return store.ProviderGenerator, true
		}
	}
	if record, _ := index.Lookup(id); produced || slices.ContainsFunc(record.Productions, local) {
		return store.ProviderAbsent, true
	}
	if request.scoped || a.loader == nil {
		return store.ProviderAbsent, false
	}
	projections := a.loader.SourceFacts(manifest.BootstrapSourceID, request.reads).Context.TemplateProjections
	for i := range projections {
		if slices.ContainsFunc(projections[i].Targets, func(target manifest.ProducerTarget) bool {
			return target.NamedResource == id
		}) {
			return store.ProviderGenerator, true
		}
	}
	return store.ProviderAbsent, false
}

func (a *Admission) sourceOutput(owner manifest.NamedResource, reads *store.ReadSet) *store.Emission {
	var observed store.Artifact
	if reads == nil {
		observed = a.store.GetArtifact(owner, store.ArtifactContent)
	} else {
		observed = reads.GetArtifact(owner, store.ArtifactContent)
	}
	output, _ := observed.(*store.Emission)
	if output == nil || !output.Complete || !output.OutputReads.Valid() {
		return nil
	}
	return output
}

func (a *Admission) loaderIndex() *manifest.DiscoveryIndex {
	if a.loader != nil {
		return a.loader.Index
	}
	return nil
}

func (d *Session) entrySourceGroups(
	ctx context.Context,
	allowMissing bool,
) (map[manifest.NamedResource]*entrySourceGroup, error) {
	// Sparse group maps MUST hold pointers to avoid reserving inline storage for empty slots.
	groups := d.sourceGroups
	if groups == nil {
		groups = map[manifest.NamedResource]*entrySourceGroup{}
		d.sourceGroups = groups
	}
	for _, group := range groups {
		*group = entrySourceGroup{refs: group.refs[:0]}
	}
	addEntrySource(groups, manifest.BootstrapSourceID, manifest.NamedResource{}, nil)
	for kind := range manifest.Kinds() {
		if !kind.IsSource() {
			continue
		}
		for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
			if obj.Named().CanBindLocal() {
				addEntrySource(groups, obj.Named(), manifest.NamedResource{}, nil)
			}
		}
	}
	if d.loader != nil {
		for id := range d.loader.Index.Files() {
			application, valid := manifest.ApplicationFor(d.loader.CatalogObject(id))
			if valid && d.admission().reachesEntry(&application, nil) {
				entrySourceGroupFor(groups, application.Source).entry = true
			}
		}
		if err := d.addLoadedSources(ctx, groups, allowMissing); err != nil {
			return nil, err
		}
	}
	if allowMissing {
		d.addMissingSources(groups)
	}
	return groups, nil
}

func (d *Session) addLoadedSources(
	ctx context.Context,
	groups map[manifest.NamedResource]*entrySourceGroup,
	allowMissing bool,
) error {
	restricted := !d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{})
	for child, err := range d.loader.Sources(ctx) {
		if err != nil {
			return err
		}
		id := child.Obj.Named()
		evidence := selection.CaptureRoot(d.loader.RootEvidence(id, nil), d.loader.Store, d.loader.Index)
		if evidence.Occurrence.RequiresBuild && evidence.Record.Builder == "" {
			continue
		}
		root := selection.Root(evidence)
		if root.Owner != (manifest.NamedResource{}) && d.resolvedSources.snapshot()[id].synthetic &&
			d.cfg.Store.GetDeclaration(id) == nil {
			continue
		}
		group := entrySourceGroupFor(groups, id)
		group.object, group.document = child.Obj, child.Doc
		group.projectedFromParent = false
		group.projected = !root.Admitted && evidence.Occurrence.Declaration == (selection.OccurrenceID{}) &&
			evidence.Record.Builder == "" && d.cfg.Store.GetDeclaration(id) == nil
		for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
			if ref.BindsSource() && ref.RequiresDeclaration {
				addEntrySource(groups, ref.NamedResource, id, &ref)
			}
		}
		group.required = group.required || restricted && root.Admitted
	}
	return d.addPathSources(groups, allowMissing)
}

func (d *Session) addMissingSources(groups map[manifest.NamedResource]*entrySourceGroup) {
	for kind := range manifest.Kinds() {
		if !kind.Runnable {
			continue
		}
		for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				if ref.IsSource() && ref.NamedResource.KindInfo().IsSource() {
					addEntrySource(groups, ref.NamedResource, obj.Named(), &ref)
				}
			}
		}
	}
	d.addMissingIndexedSources(groups)
}

func (d *Session) addMissingIndexedSources(groups map[manifest.NamedResource]*entrySourceGroup) {
	if d.loader == nil {
		return
	}
	for id := range d.loader.Index.Files() {
		if !id.KindInfo().Runnable || d.cfg.Store.GetObject(id) != nil ||
			!slices.ContainsFunc(d.loader.Index.Records[id].Refs, func(ref manifest.NamedResource) bool {
				return ref.KindInfo().IsSource()
			}) {
			continue
		}
		evidence := selection.CaptureRoot(d.loader.RootEvidence(id, nil), d.loader.Store, d.loader.Index)
		prepared := !evidence.Occurrence.RequiresBuild || evidence.Record.Builder != ""
		eager := d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{}) &&
			evidence.Scope.EntryKustomize && evidence.Scope.Eligibility != selection.RootInactive && prepared
		if !prepared || !selection.Root(evidence).Admitted && !eager {
			continue
		}
		for ref := range d.loader.References(id) {
			if ref.IsSource() && ref.NamedResource.KindInfo().IsSource() {
				addEntrySource(groups, ref.NamedResource, id, &ref)
			}
		}
	}
}

func (d *Session) addPathSources(groups map[manifest.NamedResource]*entrySourceGroup, allowMissing bool) error {
	childValues := d.loader.PathObjects()
	for childIndex := range childValues {
		child := &childValues[childIndex]
		if child.RequiresBuild {
			continue
		}
		object := occurrenceObject(child)
		if !child.Identity.KindInfo().Runnable {
			if _, applies := manifest.ApplicationFor(object); applies {
				if err := d.addGenerationSources(groups, object, child); err != nil {
					return err
				}
			}
			continue
		}

		application, valid := manifest.ApplicationFor(object)
		if valid {
			group := entrySourceGroupFor(groups, application.Source)
			group.entry = group.entry || d.occurrenceReachesEntry(&application, child)
		}
		ref, _, _ := child.SourcePath()
		group := groups[ref]
		declared := group != nil && group.object != nil
		if allowMissing || declared || d.cfg.Store.GetObject(ref) != nil {
			reference := sourceReference(object, ref)
			addEntrySource(groups, ref, child.Identity, &reference)
		}
	}
	return nil
}

func (d *Session) addGenerationSources(
	groups map[manifest.NamedResource]*entrySourceGroup, object manifest.BaseManifest,
	occurrence *selection.OccurrenceFacts,
) error {
	output, err := d.admission().prepareGeneration(object, d.cfg.Work.Source(object.Named()))
	if err != nil {
		return err
	}
	evidence := selection.CaptureRoot(d.loader.RootEvidence(object.Named(), nil), d.loader.Store, d.loader.Index)
	owner := selection.Root(evidence).Owner
	for i := range output.Children {
		child := &output.Children[i]
		if !manifest.KindOf(child.Obj).IsSource() {
			continue
		}
		id := child.Obj.Named()
		group := entrySourceGroupFor(groups, id)
		if group.object == nil {
			group.object, group.document, group.projected = child.Obj, child.Doc, true
			group.projectedFromParent = owner != (manifest.NamedResource{}) && !owner.ReportHidden()
		}
		group.required = true
		for j := range output.Children {
			if application, valid := runnableApplication(output.Children[j].Obj); valid && application.Source == id {
				group.entry = group.entry || d.occurrenceReachesEntry(&application, occurrence)
			}
		}
	}
	return nil
}

func (d *Session) occurrenceReachesEntry(
	application *manifest.EntryApplication, occurrence *selection.OccurrenceFacts,
) bool {
	admission := d.admission()
	// An occurrence outside a planned scope MUST belong to an entry package.
	if !d.hasEntryBuilder() {
		return admission.reachesEntry(application, nil)
	}
	if occurrence.Declaration == (selection.OccurrenceID{}) {
		evidence := d.loader.RootEvidence(occurrence.Identity, nil)
		occurrence = &evidence.Occurrence
		if evidence.Record.Builder != "" && evidence.PreparedRoot != "" {
			prepared := *occurrence
			prepared.RequiresBuild = false
			occurrence = &prepared
		}
	}
	return admission.reachesEntry(application, occurrence)
}
