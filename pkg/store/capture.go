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
	produced     []admission
	invalid      atomic.Bool
	retired      atomic.Bool
	store        *Store
	id           manifest.NamedResource
	inputs       *Input
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
	Ref          manifest.DependencyRef
	next         *Input
	object       registration
	view         capture
	details      atomic.Pointer[inputDetails]
	producer     uint64
	settled      bool
	gate         bool
	materializer bool
	declaration  bool
	required     bool
	hasStatus    bool
	derived      uint8
}

type inputDetails struct {
	cells       [4]registration
	gates       *readinessGate
	productions []manifest.Production
	objects     []manifest.BaseManifest
	artifact    Artifact
	conditions  []Condition
	status      StatusInfo
}

type inputBlock struct {
	input   Input
	details inputDetails
}

type readinessGate struct {
	expr                                                     *string
	artifact, artifactOnly, unknownArtifactRoot, chartSource bool
	materializer                                             bool
	next                                                     *readinessGate
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
	r.retired.Store(false)
	r.local = Input{}
	r.localDetails = inputDetails{}
	r.local.details.Store(&r.localDetails)
	clear(inputs)
	r.store, r.id, r.capacity = s, id, len(inputs)
	r.inputs, r.spare, r.warnings, r.produced = nil, nil, nil, nil
	r.spareDetails, r.byID = nil, nil
	for i := range inputs {
		inputs[i].next, r.spare = r.spare, &inputs[i]
	}
}

func (r *ReadSet) input(id manifest.NamedResource) *Input {
	index := 0
	if r.byID != nil {
		var found bool
		index, found = slices.BinarySearchFunc(*r.byID, id, func(input *Input, id manifest.NamedResource) int { return input.Ref.Compare(id) })
		if found {
			return (*r.byID)[index]
		}
	} else {
		for input := r.inputs; input != nil; input = input.next {
			if input.Ref.NamedResource == id {
				return input
			}
		}
	}
	var input *Input
	if r.local.derived == 0 {
		input = &r.local
	} else {
		if r.spare == nil {
			r.capacity = max(1, 2*r.capacity)
			block := make([]inputBlock, r.capacity)
			for i := range block {
				block[i].input.details.Store(&block[i].details)
				block[i].input.next, r.spare = r.spare, &block[i].input
			}
		}
		input, r.spare = r.spare, r.spare.next
	}
	input.Ref.NamedResource, input.next, input.derived = id, r.inputs, 32
	r.inputs = input
	if r.byID != nil {
		*r.byID = slices.Insert(*r.byID, index, input)
	}
	return input
}

func (r *ReadSet) details(input *Input) *inputDetails {
	if details := input.details.Load(); details != nil {
		return details
	}
	if len(r.spareDetails) == 0 {
		r.spareDetails = make([]inputDetails, max(1, 2*cap(r.spareDetails)))
	}
	i := len(r.spareDetails) - 1
	details := &r.spareDetails[i]
	r.spareDetails = r.spareDetails[:i]
	input.details.Store(details)
	return details
}

func (input *Input) cell(kind facet) *registration {
	if kind == objectFacet {
		return &input.object
	}
	if details := input.details.Load(); details != nil {
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
		capacity = max(1, 2*capacity)
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
	if r.byID == nil && allocations > 2 {
		length := 0
		for input := r.inputs; input != nil; input = input.next {
			length++
		}
		index := make([]*Input, 0, length+count)
		for input := r.inputs; input != nil; input = input.next {
			index = append(index, input)
		}
		slices.SortStableFunc(index, func(a, b *Input) int { return a.Ref.Compare(b.Ref.NamedResource) })
		index = slices.CompactFunc(index, func(a, b *Input) bool { return a.Ref.NamedResource == b.Ref.NamedResource })
		r.byID = &index
	}
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

func (r *ReadSet) sample(id manifest.NamedResource, slot *producerSlot, record *Emission) {
	if id == r.id && slot.id == r.id {
		return
	}
	input := r.input(slot.id)
	if input.derived&8 != 0 {
		if input.producer != record.publication() {
			r.invalid.Store(true)
		}
		return
	}
	input.derived |= 8
	input.producer = record.publication()
	input.settled = record != nil && record.Complete
}

func (r *ReadSet) activateProducers() {
	for input := r.inputs; input != nil; input = input.next {
		if input.derived&8 != 0 && (input.cell(producerFacet) == nil || input.cell(producerFacet).head == nil) {
			r.observe(input, producerFacet, 0)
		}
	}
}

// GetObject captures a named object, including its absence and supplying producer.
func (r *ReadSet) GetObject(id manifest.NamedResource) manifest.BaseManifest {
	return r.Capture(r.input(id))
}

// GetDeclaration captures the effective input authorized by static or accepted support.
func (r *ReadSet) GetDeclaration(id manifest.NamedResource) manifest.BaseManifest {
	input := r.input(id)
	r.Capture(input)
	input.declaration = true
	return input.view.declaration()
}

// Capture records a caller-owned named input without reconstructing its identity index.
func (r *ReadSet) Capture(input *Input) manifest.BaseManifest {
	if input.derived&32 == 0 {
		input.next, r.inputs = r.inputs, input
		input.derived |= 32
		if r.byID != nil {
			index, found := slices.BinarySearchFunc(*r.byID, input.Ref.NamedResource, func(captured *Input, id manifest.NamedResource) int { return captured.Ref.Compare(id) })
			if found {
				(*r.byID)[index] = input
			} else {
				*r.byID = slices.Insert(*r.byID, index, input)
			}
		}
	}
	id := input.Ref.NamedResource
	if input.derived&1 == 0 {
		for _, child := range r.produced {
			if child.child.Obj.Named() == id {
				input.view.child, input.view.independent, input.derived = child.child, child.independent, input.derived|1
				break
			}
		}
	}
	if input.derived&1 != 0 {
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
	input.view, input.derived = view, input.derived|1
	if view.binding != nil {
		r.GetObject(view.binding.Ref.NamedResource)
		r.GetArtifact(view.binding.Ref.NamedResource)
		if view.bound && !view.binding.current() {
			r.invalid.Store(true)
		}
	}
	if view.support != nil {
		r.GetObject(view.support.Ref.NamedResource)
		r.GetArtifact(view.support.Ref.NamedResource)
		if view.bound && !view.support.productionCurrent() {
			r.invalid.Store(true)
		}
	}
	if view.payloadOwner != nil {
		r.sample(id, view.payloadOwner, view.payloadRecord)
		r.activateProducers()
		input.view.payloadRecord = nil
	}
	if view.owner != nil {
		r.sample(id, view.owner, view.record)
		r.activateProducers()
		input.view.record = nil
	}
	return input.view.child.Obj
}

// Get returns the captured object asserted to T.
func (r *ReadSet) Get[T manifest.BaseManifest](id manifest.NamedResource) (T, bool) {
	obj, ok := r.GetObject(id).(T)
	return obj, ok
}

// Snapshot captures the declaration and requested readiness facts.
func (r *ReadSet) Snapshot(id manifest.NamedResource, conditions bool) (manifest.BaseManifest, Observation) {
	input := r.input(id)
	obj := r.Capture(input)
	var observed Observation
	observed.Statusful = obj != nil && manifest.KindOf(obj).Status || obj == nil && id.KindInfo().Status
	if observed.Statusful || obj == nil || conditions {
		details := r.details(input)
		if conditions && input.derived&2 == 0 {
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
			input.derived |= 2 | 16
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
	if input.derived&16 == 0 {
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
		input.derived |= 16
		r.observe(input, conditionFacet, generation)
	}
	return details.status, input.hasStatus
}

// GetArtifact captures fetched content or accepted output for a dependency.
func (r *ReadSet) GetArtifact(id manifest.NamedResource) Artifact {
	input := r.input(id)
	details := r.details(input)
	if input.derived&4 != 0 {
		return details.artifact
	}
	r.GetObject(id)
	sh := r.store.shardFor(id)
	sh.mu.RLock()
	record := sh.artifacts[id]
	if !sh.cells[id].hidden {
		details.artifact = record.value
	}
	sh.mu.RUnlock()
	input.derived |= 4
	r.observe(input, artifactFacet, record.generation)
	if input.view.payload != nil {
		details.artifact = input.view.payload
	}
	if e, ok := details.artifact.(*Emission); ok && !emissionCurrent(e, input.view) {
		details.artifact = nil
	}
	return details.artifact
}

// Productions captures all contributors and discovery evidence for a named input.
func (r *ReadSet) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	input := r.input(id)
	input.derived |= 64
	return r.productions(input, index)
}

func (r *ReadSet) productions(input *Input, index *manifest.DiscoveryIndex) []manifest.Production {
	details := r.details(input)
	if details.productions == nil {
		result := productionResult{reads: r, captureTarget: true}
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
	return ReferenceFacts{Data: data, Accepted: input.view.accepted || input.view.owner != nil, Productions: r.Productions(id, index)}
}

// Materializer selects and captures the runnable declaration supplying a production.
func (r *ReadSet) Materializer(id manifest.NamedResource, production manifest.Production) manifest.NamedResource {
	owner := production.Owner
	if declaration := production.Declaration; declaration != (manifest.NamedResource{}) && manifest.KindOf(r.GetObject(declaration)).Runnable {
		owner = declaration
		sh := r.store.shardFor(owner)
		sh.mu.RLock()
		slot := sh.slots[owner]
		if slot != nil {
			r.sample(id, slot, slot.current.Load())
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
	if input.view.declaration() != nil {
		return nil
	}
	var pending []manifest.NamedResource
	for _, production := range productions {
		owner := r.Materializer(id, production)
		if owner != (manifest.NamedResource{}) && owner != id && !r.input(owner).settled && !slices.Contains(pending, owner) {
			pending = append(pending, owner)
		}
	}
	return pending
}

// ListObjects captures the kind bucket and every candidate consulted by selection.
func (r *ReadSet) ListObjects(group, kind string) []manifest.BaseManifest {
	key := manifest.NamedResource{Group: group, Kind: kind}
	input := r.input(key)
	details := r.details(input)
	if input.derived&128 != 0 {
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
	details.objects, input.derived = out, input.derived|128
	return out
}

// GetEmission binds candidate output to this execution's captured declaration and source.
func (r *ReadSet) GetEmission(id manifest.NamedResource) (manifest.BaseManifest, map[string]any, Emission) {
	return r.store.getEmission(r, id)
}

// ID identifies the execution that owns these inputs.
func (r *ReadSet) ID() manifest.NamedResource { return r.id }

// Require retains each distinct readiness gate in the execution footprint.
func (r *ReadSet) Require(ref manifest.DependencyRef) {
	r.require(ref, false)
}

// RequireMaterializer retains a supplier's status-aware readiness assessment.
func (r *ReadSet) RequireMaterializer(id manifest.NamedResource) {
	r.require(manifest.DependencyRef{NamedResource: id}, true)
}

func (r *ReadSet) require(ref manifest.DependencyRef, materializer bool) {
	input := r.input(ref.NamedResource)
	input.required = true
	if !input.gate {
		input.Ref.ReadyExpr, input.gate = ref.ReadyExpr, true
		input.Ref.Artifact, input.Ref.ArtifactOnly, input.Ref.UnknownArtifactRoot, input.Ref.ChartSource = ref.Artifact, ref.ArtifactOnly, ref.UnknownArtifactRoot, ref.ChartSource
		input.materializer = materializer
		return
	}
	if sameReadinessGate(input.Ref, ref) && input.materializer == materializer {
		return
	}
	for gate := &r.details(input).gates; ; gate = &(*gate).next {
		if *gate == nil {
			*gate = &readinessGate{expr: ref.ReadyExpr, artifact: ref.Artifact, artifactOnly: ref.ArtifactOnly, unknownArtifactRoot: ref.UnknownArtifactRoot, chartSource: ref.ChartSource, materializer: materializer}
			return
		}
		if sameReadinessGate(manifest.DependencyRef{NamedResource: ref.NamedResource, ReadyExpr: (*gate).expr, Artifact: (*gate).artifact, ArtifactOnly: (*gate).artifactOnly, UnknownArtifactRoot: (*gate).unknownArtifactRoot, ChartSource: (*gate).chartSource}, ref) && (*gate).materializer == materializer {
			return
		}
	}
}

// Gates enumerates the retained readiness assessments.
func (r *ReadSet) Gates() iter.Seq[Gate] {
	return func(yield func(Gate) bool) {
		for input := r.inputs; input != nil; input = input.next {
			if !input.gate {
				continue
			}
			if !yield(Gate{DependencyRef: input.Ref, Materializer: input.materializer}) {
				return
			}
			details := input.details.Load()
			if details == nil {
				continue
			}
			for gate := details.gates; gate != nil; gate = gate.next {
				if !yield(Gate{NamedResource: input.Ref.NamedResource, ReadyExpr: gate.expr, Artifact: gate.artifact, ArtifactOnly: gate.artifactOnly, UnknownArtifactRoot: gate.unknownArtifactRoot, ChartSource: gate.chartSource, Materializer: gate.materializer}) {
					return
				}
			}
		}
	}
}

// Required enumerates the required inputs of the accepted execution.
func (r *ReadSet) Required() iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		for input := r.inputs; input != nil; input = input.next {
			if input.required && !yield(input.Ref.NamedResource) {
				return
			}
		}
	}
}

// RequireData records a required semantic input without imposing readiness.
func (r *ReadSet) RequireData(id manifest.NamedResource) { r.input(id).required = true }

// CarryWarnings reuses advisories after the caller verifies an equal consumed projection.
func (r *ReadSet) CarryWarnings(previous RenderedOutput) {
	r.warnings = append(r.warnings, previous.warnings...)
}

// AddWarning retains a render advisory in this execution's accepted bundle.
func (r *ReadSet) AddWarning(_ manifest.NamedResource, warning manifest.Warning) {
	warning.Detail = slices.Clone(warning.Detail)
	r.warnings = append(r.warnings, warning)
}

// Accept replaces the current assessment and advisory bundle for this origin.
func (r *ReadSet) Accept() {
	if !r.Valid() {
		return
	}
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

// SetIncluded publishes a changed-only admission transition through the object head.
func (s *Store) SetIncluded(id manifest.NamedResource, included bool) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	cell := sh.cells[id]
	if cell.excluded != included {
		sh.mu.Unlock()
		return
	}
	sh.requireCapacityLocked(cell.authority)
	cell.excluded = !included
	cell.authority = sh.advanceLocked(cell.authority)
	sh.cells[id] = cell
	sh.mu.Unlock()
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
	cell.authority = sh.advanceLocked(cell.authority)
	sh.cells[id] = cell
	sh.mu.Unlock()
}

// Admit binds raw-file admission to the captured source that supplied its bytes.
func (r *ReadSet) Admit(obj manifest.BaseManifest, source, owner manifest.NamedResource, docs ...map[string]any) {
	r.GetArtifact(source)
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
		shared := len(emission.Children) == len(previous.Children) && (len(emission.Children) == 0 || &emission.Children[0] == &previous.Children[0])
		emission.entriesOwned = !shared && !entriesMatch(emission.Children, previous.entries)
	}
	if emission.entriesOwned {
		emission.entries = r.store.prepareChildren(emission.Children)
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
		id := input.Ref.NamedResource
		details := input.details.Load()
		if input.derived&1 != 0 && !semanticEqual(Child{Obj: r.GetObject(id), Doc: r.input(id).view.child.Doc}, input.view.child) {
			return false
		}
		if input.declaration && (r.GetDeclaration(id) == nil) != (input.view.declaration() == nil) {
			return false
		}
		if input.derived&2 != 0 {
			var observed Observation
			_, observed = r.Snapshot(id, true)
			conditions := observed.Conditions
			if !slices.EqualFunc(conditions, details.conditions, conditionEqual) {
				return false
			}
		} else if input.derived&16 != 0 {
			status, ok := r.GetStatus(id)
			if ok != input.hasStatus || status.Status != details.status.Status || status.Reason != details.status.Reason || !sameMessage(status.Message, status.applyingObjects, details.status.Message, details.status.applyingObjects) || !slices.Equal(status.BlockedBy(), details.status.BlockedBy()) {
				return false
			}
		}
		if input.derived&4 != 0 && !sameArtifact(details.artifact, r.GetArtifact(id)) {
			return false
		}
		if input.derived&64 != 0 && !slices.Equal(details.productions, r.Productions(id, nil)) {
			return false
		}
		if input.derived&128 != 0 && !slices.EqualFunc(details.objects, r.ListObjects(id.Group, id.Kind), objectEqual) {
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
	artifact := sh.artifacts[r.id].value
	if slot := sh.slots[r.id]; slot != nil {
		if current := slot.current.Load(); current != nil {
			artifact = current
		}
	}
	var reads *ReadSet
	switch output := artifact.(type) {
	case *Emission:
		reads = output.OutputReads
		output.payloadReads.Retire()
		for borrowed := range output.borrowed {
			borrowed.Retire()
		}
	case *SourceArtifact:
		reads = output.Reads
	}
	sh.mu.RUnlock()
	reads.Retire()
}

func sameReadinessGate(a, b manifest.DependencyRef) bool {
	if a.ReadyExpr != b.ReadyExpr {
		if a.ReadyExpr == nil || b.ReadyExpr == nil || *a.ReadyExpr != *b.ReadyExpr {
			return false
		}
		a.ReadyExpr = b.ReadyExpr
	}
	return a == b
}
