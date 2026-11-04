package store

import (
	"iter"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

// ReadSet retains the immutable inputs consumed by one execution.
// Its storage MUST remain owned by that execution until retirement is swept.
type ReadSet struct {
	warnings     []manifest.Warning
	advisories   AdvisoryEvidence
	produced     []admission
	invalid      atomic.Bool
	accepted     atomic.Bool
	retired      atomic.Bool
	store        *Store
	id           manifest.NamedResource
	inputs       *Input
	last         *Input
	local        Input
	spare        *Input
	capacity     int
	localDetails inputDetails
	spareDetails []inputDetails
	byID         *[]*Input
}

// Input co-owns a captured dependency and its authority registrations.
// Its reference MUST remain immutable until retirement is swept.
type Input struct {
	Ref           manifest.DependencyRef
	next          *Input
	object        registration
	view          capture
	details       atomic.Pointer[inputDetails]
	producer      uint64
	settled       bool
	watchProducer bool
	gate          bool
	materializer  bool
	declaration   bool
	required      bool
	hasStatus     bool
	derived       uint8
}

type inputDetails struct {
	cells        [6]registration
	gates        *readinessGate
	productions  []manifest.Production
	objects      []manifest.BaseManifest
	artifact     Artifact
	artifactRead ArtifactRead
	conditions   []Condition
	status       StatusInfo
}

type inputBlock struct {
	input   Input
	details inputDetails
}

type readinessGate struct {
	expr                                *string
	artifact, artifactOnly, chartSource bool
	materializer                        bool
	next                                *readinessGate
}

// Gate retains a readiness expression and its dependency purpose.
type Gate struct {
	manifest.DependencyRef

	Materializer bool
}

// Init starts an execution using caller-owned dependency storage.
// Reused storage MUST have no live or detached registration or output borrower.
func (r *ReadSet) Init(s *Store, id manifest.NamedResource, inputs []Input) {
	r.invalid.Store(false)
	r.accepted.Store(false)
	r.retired.Store(false)
	r.local = Input{}
	r.localDetails = inputDetails{}
	r.local.details.Store(&r.localDetails)
	clear(inputs)
	r.store, r.id, r.capacity = s, id, len(inputs)
	r.inputs, r.last, r.spare, r.warnings, r.produced = nil, nil, nil, nil, nil
	r.advisories = AdvisoryEvidence{}
	r.spareDetails, r.byID = nil, nil
	for i := range inputs {
		inputs[i].next, r.spare = r.spare, &inputs[i]
	}
}

func (r *ReadSet) input(id manifest.NamedResource) *Input {
	if r.last != nil && r.last.Ref.NamedResource == id {
		return r.last
	}
	index := 0
	if r.byID != nil {
		var found bool
		index, found = slices.BinarySearchFunc(
			*r.byID,
			id,
			func(input *Input, id manifest.NamedResource) int { return input.Ref.Compare(id) },
		)
		if found {
			r.last = (*r.byID)[index]
			return r.last
		}
	} else {
		for input := r.inputs; input != nil; input = input.next {
			if input.Ref.NamedResource == id {
				r.last = input
				return input
			}
		}
	}
	return r.allocateInput(id, index)
}

func (r *ReadSet) details(input *Input) *inputDetails {
	if details := input.details.Load(); details != nil {
		return details
	}
	if len(r.spareDetails) == 0 {
		r.spareDetails = make([]inputDetails, max(1, allocationGrowthFactor*cap(r.spareDetails)))
	}
	i := len(r.spareDetails) - 1
	details := &r.spareDetails[i]
	r.spareDetails = r.spareDetails[:i]
	input.details.Store(details)
	return details
}

func (in *Input) cell(kind facet) *registration {
	if kind == objectFacet {
		return &in.object
	}
	if details := in.details.Load(); details != nil {
		return &details.cells[kind-conditionFacet]
	}
	return nil
}

func (r *ReadSet) reserve(count int) {
	for spare := r.spare; spare != nil && count > 0; spare = spare.next {
		count--
	}
	remaining, capacity, allocations := count, r.capacity, 0
	for remaining > 0 {
		capacity = max(1, allocationGrowthFactor*capacity)
		remaining -= capacity
		allocations++
	}
	if allocations <= 1 {
		return
	}
	inputs := make([]Input, count)
	for i := range inputs {
		inputs[i].next, r.spare = r.spare, &inputs[i]
	}
	if r.byID == nil && allocations > maxLinearInputBlocks {
		r.indexInputs(count)
	}
}

func (r *ReadSet) indexInputs(spare int) {
	length := 0
	for input := r.inputs; input != nil; input = input.next {
		length++
	}
	index := make([]*Input, 0, length+spare)
	for input := r.inputs; input != nil; input = input.next {
		index = append(index, input)
	}
	slices.SortStableFunc(index, func(a, b *Input) int { return a.Ref.Compare(b.Ref.NamedResource) })
	index = slices.CompactFunc(index, func(a, b *Input) bool { return a.Ref.NamedResource == b.Ref.NamedResource })
	r.byID = &index
}

func (r *ReadSet) observe(input *Input, kind facet, generation uint64) {
	if kind != objectFacet {
		r.details(input)
	}
	cell := input.cell(kind)
	if cell.head != nil {
		if kind != producerFacet && cell.generation.Load() != generation {
			r.invalid.Store(true)
		}
		return
	}
	r.activate(input, kind, generation)
}

func (r *ReadSet) sample(id manifest.NamedResource, slot *producerSlot, record *Emission, watch bool) {
	if id == r.id && slot.id == r.id {
		return
	}
	input := r.input(slot.id)
	input.watchProducer = input.watchProducer || watch
	if input.derived&capturedSettlement != 0 {
		if (!input.settled || watch) && input.producer != record.publication() {
			r.invalid.Store(true)
		}
		return
	}
	input.derived |= capturedSettlement
	input.producer = record.publication()
	input.settled = record != nil && record.Complete
}

func (r *ReadSet) activateProducers() {
	for input := r.inputs; input != nil; input = input.next {
		if input.derived&capturedSettlement != 0 && (!input.settled || input.watchProducer) &&
			(input.cell(producerFacet) == nil || input.cell(producerFacet).head == nil) {
			r.observe(input, producerFacet, 0)
		}
	}
}

// GetObject captures a named object, including its absence and supplying producer.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *ReadSet) GetObject(id manifest.NamedResource) manifest.BaseManifest {
	return r.Capture(r.input(id))
}

// GetDeclaration captures the effective input authorized by static or accepted support.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *ReadSet) GetDeclaration(id manifest.NamedResource) manifest.BaseManifest {
	input := r.input(id)
	r.Capture(input)
	input.declaration = true
	return new(input.view).declaration()
}

// Capture records a caller-owned named input without reconstructing its identity index.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *ReadSet) Capture(input *Input) manifest.BaseManifest {
	r.attachInput(input)
	id := input.Ref.NamedResource
	if input.derived&capturedObject == 0 {
		{
			childValues := r.produced
			for childIndex := range childValues {
				child := childValues[childIndex]
				if child.child.Obj.Named() == id {
					input.view.child, input.view.independent, input.derived = child.child, child.independent, input.derived|capturedObject
					break
				}
			}
		}
	}
	if input.derived&capturedObject != 0 {
		return input.view.child.Obj
	}
	sh := r.store.shardFor(id)
	sh.mu.RLock()
	var view capture
	cell, present := sh.cells[id]
	sh.captureLocked(id, cell, &view)
	registration := &input.object
	registration.generation.Store(view.generation)
	registration.owner = r
	if !present {
		registration.absent = input
		registration.head = &sh.absence
		registration.link()
	} else if cell.links != nil {
		registration.head = cell.links
		if cell.links.first.Load() == nil && !cell.links.detached.Load() {
			cell.links.generation.Store(view.generation)
		}
		registration.link()
	}
	sh.mu.RUnlock()
	if registration.head == nil {
		r.activate(input, objectFacet, view.generation)
	}
	input.view, input.derived = view, input.derived|capturedObject
	r.captureSourceAuthorities(&view)
	input.view.record = nil
	input.view.payloadInput = nil
	return input.view.child.Obj
}

// Get returns the captured object asserted to T.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *ReadSet) Get[T manifest.BaseManifest](id manifest.NamedResource) (T, bool) {
	obj, ok := r.GetObject(id).(T)
	return obj, ok
}

// Snapshot captures the declaration and requested readiness facts.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *ReadSet) Snapshot(id manifest.NamedResource, conditions bool) (manifest.BaseManifest, Observation) {
	input := r.input(id)
	obj := r.Capture(input)
	var observed Observation
	observed.Statusful = obj != nil && manifest.KindOf(obj).Status || obj == nil && id.KindInfo().Status
	if observed.Statusful || obj == nil || conditions {
		details := r.details(input)
		if conditions && input.derived&capturedConditions == 0 {
			sh := r.store.shardFor(id)
			sh.mu.RLock()
			record := sh.conditions[id]
			generation := record.stamp().generation
			if !sh.cells[id].hidden {
				details.conditions = slices.Clone(record.conditions)
				details.status, input.hasStatus = record.statusInfo()
			}
			sh.mu.RUnlock()
			record.project(details.conditions)
			input.derived |= capturedConditions | capturedStatus
			r.observe(input, conditionFacet, generation)
		}
		observed.StatusInfo, _ = r.status(input)
		if conditions {
			observed.Conditions = details.conditions
		}
	}
	return obj, observed
}

// GetStatus captures the readiness condition used by an assessment.
func (r *ReadSet) GetStatus(id manifest.NamedResource) (StatusInfo, bool) {
	return r.status(r.input(id))
}

func (r *ReadSet) status(input *Input) (StatusInfo, bool) {
	details := r.details(input)
	if input.derived&capturedStatus == 0 {
		r.Capture(input)
		id := input.Ref.NamedResource
		sh := r.store.shardFor(id)
		sh.mu.RLock()
		record := sh.conditions[id]
		generation := record.stamp().generation
		if !sh.cells[id].hidden {
			details.status, input.hasStatus = record.statusInfo()
		}
		sh.mu.RUnlock()
		input.derived |= capturedStatus
		r.observe(input, conditionFacet, generation)
	}
	return details.status, input.hasStatus
}

// GetArtifact captures fetched content or accepted output for a dependency.
//
//nolint:ireturn // The store contract supports multiple artifact implementations.
func (r *ReadSet) GetArtifact(id manifest.NamedResource, read ArtifactRead) Artifact {
	input := r.input(id)
	details := r.details(input)
	if input.derived&capturedArtifact == 0 {
		r.Capture(input)
		if input.object.head == nil {
			return r.store.GetArtifact(id, read)
		}
		sh := r.store.shardFor(id)
		sh.mu.RLock()
		record := sh.artifacts[id]
		if !sh.cells[id].hidden {
			details.artifact = record.effective
		}
		for i, generation := range [3]uint64{record.generation, record.revisionHead.generation, record.originHead.generation} {
			input.cell(contentFacet + facet(i)).generation.Store(generation)
		}
		sh.mu.RUnlock()
		input.derived |= capturedArtifact
		if e, ok := details.artifact.(*Emission); ok && !emissionCurrent(e, new(input.view)) {
			details.artifact = nil
		}
	}
	added := read &^ details.artifactRead
	details.artifactRead |= read
	for i := range 3 {
		if added&(1<<i) != 0 {
			kind := contentFacet + facet(i)
			r.observe(input, kind, input.cell(kind).generation.Load())
		}
	}
	return details.artifact
}

// Productions captures all contributors and discovery evidence for a named input.
func (r *ReadSet) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	input := r.input(id)
	input.derived |= capturedProductions
	return r.productions(input, index)
}

// IncomingProductions captures prospective source and contributor evidence without reading the target.
func (r *ReadSet) IncomingProductions(child Child, index *manifest.DiscoveryIndex) []manifest.Production {
	result := productionResult{reads: r, incoming: &child, watch: true}
	r.store.visitProductions(child.Obj.Named(), index, &result)
	return result.out
}

func (r *ReadSet) productions(input *Input, index *manifest.DiscoveryIndex) []manifest.Production {
	details := r.details(input)
	if details.productions == nil {
		result := productionResult{reads: r, captureTarget: true, watch: true}
		r.store.visitProductions(input.Ref.NamedResource, index, &result)
		details.productions = result.out
		if details.productions == nil {
			details.productions = []manifest.Production{}
		}
	}
	return details.productions
}

// ReferenceFacts captures visible data and its accepted source-qualified authority.
func (r *ReadSet) ReferenceFacts(id manifest.NamedResource, index *manifest.DiscoveryIndex) ReferenceFacts {
	input := r.input(id)
	data, _ := r.Capture(input).(manifest.Data)
	supplied, path := r.store.suppliedObject(id)
	input.derived |= capturedProductions
	productions := r.productions(input, index)
	var candidates []manifest.ReferenceData
	if len(productions) != 0 {
		candidates = r.store.referenceData(id, index, productions, r)
	}
	{
		productionValues := productions
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			if production.Source.Name != "" {
				r.GetArtifact(production.Source, ArtifactContent)
			}
		}
	}
	return ReferenceFacts{
		Data:          data,
		Accepted:      input.view.accepted || input.view.owner != nil,
		Productions:   productions,
		ReferenceData: candidates,
		Supplied:      supplied,
		SupplyPath:    path,
	}
}

// Materializer selects and captures the runnable declaration supplying a production.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (r *ReadSet) Materializer(id manifest.NamedResource, production manifest.Production) manifest.NamedResource {
	owner := production.Owner
	if declaration := production.Declaration; declaration != (manifest.NamedResource{}) &&
		manifest.KindOf(r.GetObject(declaration)).Runnable {
		owner = declaration
		sh := r.store.shardFor(owner)
		sh.mu.RLock()
		slot := sh.slots[owner]
		if slot != nil {
			r.sample(id, slot, slot.current.Load(), false)
		}
		sh.mu.RUnlock()
		r.activateProducers()
	}
	return owner
}

// UnsettledProducers captures contributors needed to supply an unpublished declaration.
// Availability MUST NOT become a content projection for artifact reuse.
func (r *ReadSet) UnsettledProducers(id manifest.NamedResource) []manifest.NamedResource {
	input := r.input(id)
	productions := r.productions(input, nil)
	if new(input.view).declaration() != nil {
		return nil
	}
	var pending []manifest.NamedResource
	{
		productionValues2 := productions
		for productionIndex2 := range productionValues2 {
			production := productionValues2[productionIndex2]
			owner := r.Materializer(id, production)
			if owner != (manifest.NamedResource{}) && owner != id && !r.input(owner).settled &&
				!slices.Contains(pending, owner) {
				pending = append(pending, owner)
			}
		}
	}
	return pending
}

// ListObjects captures the kind bucket and every candidate consulted by selection.
func (r *ReadSet) ListObjects(group, kind string) []manifest.BaseManifest {
	key := manifest.NamedResource{Group: group, Kind: kind}
	input := r.input(key)
	details := r.details(input)
	if input.derived&capturedObjects != 0 {
		return details.objects
	}
	sh := r.store.shardFor(key)
	sh.mu.RLock()
	bucket := sh.byName[kindID{group, kind}]
	ids := make([]manifest.NamedResource, 0, len(bucket.names))
	for name := range bucket.names {
		ids = append(ids, manifest.NamedResource{Group: group, Kind: kind, Namespace: name.namespace, Name: name.name})
	}
	generation := bucket.generation
	sh.mu.RUnlock()
	r.observe(input, bucketFacet, generation)
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	missing := len(ids)
	for captured := r.inputs; captured != nil; captured = captured.next {
		if _, found := slices.BinarySearchFunc(ids, captured.Ref.NamedResource, manifest.NamedResource.Compare); found {
			missing--
		}
	}
	r.reserve(max(0, missing))
	out := make([]manifest.BaseManifest, 0, len(ids))
	for _, id := range ids {
		if obj := r.GetObject(id); obj != nil {
			out = append(out, obj)
		}
	}
	details.objects, input.derived = out, input.derived|capturedObjects
	return out
}

// GetEmission binds candidate output to this execution's captured declaration and source.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (r *ReadSet) GetEmission(id manifest.NamedResource) (manifest.BaseManifest, map[string]any, Emission) {
	return r.store.getEmission(r, id)
}

// ID identifies the execution that owns these inputs.
func (r *ReadSet) ID() manifest.NamedResource { return r.id }

// Inputs yields the concrete identities captured by the completed execution.
// The owning output MUST keep the read set immutable throughout iteration.
func (r *ReadSet) Inputs() iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		if r == nil {
			return
		}
		for input := r.inputs; input != nil; input = input.next {
			if input.Ref.Name != "" && !yield(input.Ref.NamedResource) {
				return
			}
		}
	}
}

// Accept replaces the current assessment and advisory bundle for this origin.
func (r *ReadSet) Accept() {
	if !r.Valid() {
		return
	}
	r.accepted.Store(true)
	input := r.input(r.id)
	if head := input.object.head; head != nil && input.object.absent == nil {
		head.assessment.Store(r)
	}
}

// ReplacesInvalid reports whether this attempt supersedes a stale accepted assessment.
func (r *ReadSet) ReplacesInvalid() bool {
	r.GetObject(r.id)
	head := r.input(r.id).object.head
	if head == nil {
		return false
	}
	previous := head.assessment.Load()
	return previous != nil && (!previous.Valid() || previous.retired.Load() && !r.Matches(previous))
}

// Admitted reports the captured changed-only disposition.
func (r *ReadSet) Admitted() bool { r.GetObject(r.id); return !r.input(r.id).view.excluded }

// Preflight returns the captured structural admission failure.
func (r *ReadSet) Preflight() string { r.GetObject(r.id); return r.input(r.id).view.preflight }

// RequiredInputs enumerates a completed origin's accepted required footprint.
func (s *Store) RequiredInputs(id manifest.NamedResource) iter.Seq[manifest.NamedResource] {
	sh := s.shardFor(id)
	sh.mu.RLock()
	head := sh.cells[id].links
	sh.mu.RUnlock()
	var reads *ReadSet
	if head != nil {
		reads = head.assessment.Load()
	}
	if reads == nil || !reads.Valid() {
		return func(func(manifest.NamedResource) bool) {}
	}
	return reads.Required()
}

// SetIncluded publishes invocation-monotone admission and resets stale unchanged readiness.
// Once included, an object cannot be excluded within this Store invocation.
func (s *Store) SetIncluded(id manifest.NamedResource, included bool) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	cell := sh.cells[id]
	if !included && (cell.included || cell.excluded) {
		sh.mu.Unlock()
		return
	}
	var condition authority
	var reset bool
	if included {
		record := sh.conditions[id]
		info, _ := record.statusInfo()
		reset = info.Outcome() == OutcomeUnchanged
		if reset {
			condition = record.stamp()
		}
	}
	if cell.included && !reset {
		sh.mu.Unlock()
		return
	}
	if cell.excluded == included || reset {
		sh.requireCapacityLocked(cell.authority, condition)
		cell.authority = sh.advanceLocked(cell.current())
	}
	cell.excluded, cell.included = !included, included
	sh.cells[id] = cell
	var dispatch eventDispatch
	if reset {
		dispatch = sh.resetInclusion(s, id, &cell)
	} else if included && !cell.hidden {
		if obj := sh.objectLocked(id); obj != nil {
			dispatch = fireUnderLock(s, EventObjectAdded, id)
			dispatch.payload = obj
		}
	}
	sh.mu.Unlock()
	dispatch.run()
}

// SetPreflight publishes a changed structural admission decision.
func (s *Store) SetPreflight(id manifest.NamedResource, message string) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	cell := sh.cells[id]
	if cell.preflight == message {
		sh.mu.Unlock()
		return
	}
	sh.requireCapacityLocked(cell.authority)
	cell.preflight = message
	cell.authority = sh.advanceLocked(cell.current())
	sh.cells[id] = cell
	sh.mu.Unlock()
}

// Admit binds raw-file admission to the captured source that supplied its bytes.
func (r *ReadSet) Admit(obj manifest.BaseManifest, source, owner manifest.NamedResource, docs ...map[string]any) {
	r.GetArtifact(source, ArtifactContent)
	var support *Input
	if owner != (manifest.NamedResource{}) && owner != obj.Named() {
		support = r.input(owner)
	}
	r.store.addObject(obj, r.input(source), support, docs...)
}

// Promotions captures source-family evidence before conditional raw-file admission.
// The target itself is a mutation precondition until admission has completed.
func (r *ReadSet) Promotions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	result := productionResult{reads: r}
	r.store.visitProductions(id, index, &result)
	return result.out
}

// Bind attaches consumed provenance and retains prospective children before publication.
func (r *ReadSet) Bind(emission *Emission) {
	previous := emission.previous
	if emission.Reads != r {
		_, _, captured := r.GetEmission(r.id)
		emission.input, emission.inputDoc, emission.inputHead = captured.input, captured.inputDoc, captured.inputHead
		emission.binding, emission.coverage = captured.binding, captured.coverage
		previous = captured.previous
		captured.Finish()
	}
	emission.Reads = r
	emission.entriesOwned = true
	if previous != nil {
		shared := len(emission.Children) == len(previous.Children) &&
			(len(emission.Children) == 0 || &emission.Children[0] == &previous.Children[0])
		emission.entriesOwned = !shared && !entriesMatch(emission.Children, previous.entries)
	}
	if emission.entriesOwned {
		emission.entries = r.store.prepareChildren(emission.Children, r.store.index.Load())
	} else {
		emission.entries = previous.entries
	}
	r.produced = emission.entries
}

// Matches recaptures a predecessor's consumed projections in a fresh execution.
func (r *ReadSet) Matches(previous *ReadSet) bool {
	if previous == nil {
		return false
	}
	for input := previous.inputs; input != nil; input = input.next {
		if !r.matchesInput(input) {
			return false
		}
	}
	return r.Valid()
}

// Close retires captured assessment cells and the origin's borrowed output.
// Callers MUST stop work for the origin before closing.
func (r *ReadSet) Close() {
	r.Retire()
	sh := r.store.shardFor(r.id)
	sh.mu.RLock()
	source := sh.artifacts[r.id].value
	var inventory *Emission
	if slot := sh.slots[r.id]; slot != nil {
		inventory = slot.current.Load()
	}
	sh.mu.RUnlock()
	if source != nil {
		source.Reads.Retire()
	}
	if inventory != nil {
		inventory.Reads.Retire()
		inventory.OutputReads.Retire()
		inventory.payloadReads.Retire()
		for borrowed := range inventory.borrowed {
			borrowed.Retire()
		}
	}
}

func sameReadinessGate(aValue *manifest.DependencyRef, bValue *manifest.DependencyRef) bool {
	a := *aValue
	b := *bValue
	if a.ReadyExpr != b.ReadyExpr {
		if a.ReadyExpr == nil || b.ReadyExpr == nil || *a.ReadyExpr != *b.ReadyExpr {
			return false
		}
		a.ReadyExpr = b.ReadyExpr
	}
	return a == b
}

const (
	capturedObject uint8 = 1 << iota
	capturedConditions
	capturedArtifact
	capturedSettlement
	capturedStatus
	initializedInput
	capturedProductions
	capturedObjects
)

const (
	allocationGrowthFactor = 2
	maxLinearInputBlocks   = 2
)

func (r *ReadSet) allocateInput(id manifest.NamedResource, index int) *Input {
	var input *Input
	if r.local.derived == 0 {
		input = &r.local
	} else {
		if r.spare == nil {
			r.capacity = max(1, allocationGrowthFactor*r.capacity)
			block := make([]inputBlock, r.capacity)
			for i := range block {
				block[i].input.details.Store(&block[i].details)
				block[i].input.next, r.spare = r.spare, &block[i].input
			}
			if r.byID == nil && r.capacity >= 64 {
				r.indexInputs(r.capacity)
				index, _ = slices.BinarySearchFunc(
					*r.byID,
					id,
					func(input *Input, id manifest.NamedResource) int { return input.Ref.Compare(id) },
				)
			}
		}
		input, r.spare = r.spare, r.spare.next
	}
	input.Ref.NamedResource, input.next, input.derived = id, r.inputs, initializedInput
	r.inputs = input
	if r.byID != nil {
		*r.byID = slices.Insert(*r.byID, index, input)
	}
	r.last = input
	return input
}

func (r *ReadSet) attachInput(input *Input) {
	if input.derived&initializedInput != 0 {
		return
	}
	if r.last != nil && r.last.Ref.NamedResource == input.Ref.NamedResource {
		r.last = input
	}
	input.next, r.inputs = r.inputs, input
	input.derived |= initializedInput
	if r.byID != nil {
		index, found := slices.BinarySearchFunc(
			*r.byID,
			input.Ref.NamedResource,
			func(captured *Input, id manifest.NamedResource) int { return captured.Ref.Compare(id) },
		)
		if found {
			(*r.byID)[index] = input
		} else {
			*r.byID = slices.Insert(*r.byID, index, input)
		}
	}
}

func (r *ReadSet) captureSourceAuthorities(view *capture) {
	if view.binding != nil {
		r.GetObject(view.binding.Ref.NamedResource)
		r.GetArtifact(view.binding.Ref.NamedResource, ArtifactContent)
		if view.bound && !view.binding.current() {
			r.invalid.Store(true)
		}
	}
	if view.support != nil {
		r.GetObject(view.support.Ref.NamedResource)
		r.GetArtifact(view.support.Ref.NamedResource, ArtifactContent)
		if view.bound && !view.support.productionCurrent() {
			r.invalid.Store(true)
		}
	}
}

func (r *ReadSet) matchesInput(input *Input) bool {
	id := input.Ref.NamedResource
	details := input.details.Load()
	if input.derived&capturedObject != 0 &&
		!reconcileEqual(Child{Obj: r.GetObject(id), Doc: r.input(id).view.child.Doc}, input.view.child) {
		return false
	}
	if input.declaration && (r.GetDeclaration(id) == nil) != (new(input.view).declaration() == nil) {
		return false
	}
	if !r.matchesReadiness(id, input, details) {
		return false
	}
	if input.derived&capturedArtifact != 0 &&
		!artifactReadEqual(details.artifact, r.GetArtifact(id, details.artifactRead), details.artifactRead) {
		return false
	}
	if input.derived&capturedProductions != 0 {
		if !slices.Equal(details.productions, r.Productions(id, nil)) {
			return false
		}
	} else if details != nil && details.productions != nil {
		r.UnsettledProducers(id)
	}
	if input.derived&capturedObjects != 0 &&
		!slices.EqualFunc(details.objects, r.ListObjects(id.Group, id.Kind), manifest.Equal) {
		return false
	}
	return true
}

func (r *ReadSet) matchesReadiness(id manifest.NamedResource, input *Input, details *inputDetails) bool {
	if input.derived&capturedConditions != 0 {
		var observed Observation
		_, observed = r.Snapshot(id, true)
		conditions := observed.Conditions
		if !slices.EqualFunc(conditions, details.conditions, conditionEqual) {
			return false
		}
	} else if input.derived&capturedStatus != 0 {
		status, ok := r.GetStatus(id)
		if ok != input.hasStatus || status.Status != details.status.Status ||
			status.Reason != details.status.Reason ||
			!sameMessage(
				status.Message,
				status.applyingObjects,
				details.status.Message,
				details.status.applyingObjects,
			) ||
			!slices.Equal(status.BlockedBy(), details.status.BlockedBy()) {
			return false
		}
	}
	return true
}

func (sh *shard) resetInclusion(s *Store, id manifest.NamedResource, cell *objectCell) eventDispatch {
	var dispatch eventDispatch
	updated, _ := sh.setConditionLocked(id, new(readyCondition(StatusPending, "", "")), 0, nil)
	if !sh.derived || !cell.hidden {
		dispatch = fireUnderLock(s, EventStatusUpdated, id)
		if dispatch.listeners != nil {
			dispatch.payload, _ = updated.statusInfo()
		}
	}
	return dispatch
}
