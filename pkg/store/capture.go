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
	warnings []manifest.Warning
	produced []Child
	invalid  atomic.Bool
	retired  atomic.Bool
	store    *Store
	id       manifest.NamedResource
	inputs   *Input
	local    Input
	spare    []Input
	capacity int
}

// Input co-owns a captured dependency and its authority registrations.
// Its reference MUST remain immutable until retirement is swept.
type Input struct {
	Ref          manifest.DependencyRef
	gates        *readinessGate
	next         *Input
	cells        [5]registration
	view         capture
	producer     uint64
	settled      bool
	gate         bool
	materializer bool
	declaration  bool
	required     bool
	productions  []manifest.Production
	objects      []manifest.BaseManifest
	artifact     Artifact
	conditions   []Condition
	status       StatusInfo
	hasStatus    bool
	derived      uint8
}

type readinessGate struct {
	expr                                       *string
	artifact, unknownArtifactRoot, chartSource bool
	materializer                               bool
	next                                       *readinessGate
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
	if r.local.derived != 0 {
		r.local = Input{}
	}
	clear(inputs)
	r.store, r.id, r.spare, r.capacity = s, id, inputs, len(inputs)
	r.inputs, r.warnings, r.produced = nil, nil, nil
}

func (r *ReadSet) input(id manifest.NamedResource) *Input {
	for input := r.inputs; input != nil; input = input.next {
		if input.Ref.NamedResource == id {
			return input
		}
	}
	if r.local.derived == 0 {
		r.local.Ref.NamedResource, r.local.derived, r.local.next = id, 32, r.inputs
		r.inputs = &r.local
		return &r.local
	}
	if len(r.spare) == 0 {
		r.capacity = max(1, 2*r.capacity)
		r.spare = make([]Input, r.capacity)
	}
	input := &r.spare[0]
	r.spare = r.spare[1:]
	input.Ref.NamedResource, input.next, input.derived = id, r.inputs, 32
	r.inputs = input
	return input
}

func (r *ReadSet) observe(input *Input, kind facet, generation uint64) {
	cell := &input.cells[kind-1]
	if cell.head != nil {
		if kind != producerFacet && cell.generation.Load() != generation {
			r.invalid.Store(true)
		}
		return
	}
	r.activate(input, kind, generation)
}

func (r *ReadSet) sample(slot *producerSlot, record *Emission) {
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
		if input.derived&8 != 0 && input.cells[producerFacet-1].head == nil {
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
	return r.store.declaration(id, input.view)
}

// Capture records a caller-owned named input without reconstructing its identity index.
func (r *ReadSet) Capture(input *Input) manifest.BaseManifest {
	if input.derived&32 == 0 {
		input.next, r.inputs = r.inputs, input
		input.derived |= 32
	}
	id := input.Ref.NamedResource
	if input.derived&1 == 0 {
		for _, child := range r.produced {
			if child.Obj.Named() == id {
				input.view.child, input.derived = child, input.derived|1
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
	registration := &input.cells[objectFacet-1]
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
	if view.owner != nil {
		r.sample(view.owner, view.record)
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
	obj := r.GetObject(id)
	var observed Observation
	observed.Statusful = obj != nil && manifest.KindOf(obj).Status || obj == nil && id.KindInfo().Status
	if observed.Statusful || obj == nil || conditions {
		input := r.input(id)
		if conditions && input.derived&2 == 0 {
			sh := r.store.shardFor(id)
			sh.mu.RLock()
			record := sh.conditions[id]
			generation := record.stamp().generation
			if !sh.cells[id].hidden {
				input.conditions = slices.Clone(record.conditions)
				input.status, input.hasStatus = record.statusInfo()
			}
			sh.mu.RUnlock()
			record.project(input.conditions)
			input.derived |= 2 | 16
			r.observe(input, conditionFacet, generation)
		}
		observed.StatusInfo, _ = r.GetStatus(id)
		if conditions {
			observed.Conditions = input.conditions
		}
	}
	return obj, observed
}

// GetStatus captures the readiness condition used by an assessment.
func (r *ReadSet) GetStatus(id manifest.NamedResource) (StatusInfo, bool) {
	input := r.input(id)
	if input.derived&16 == 0 {
		r.GetObject(id)
		sh := r.store.shardFor(id)
		sh.mu.RLock()
		record := sh.conditions[id]
		generation := record.stamp().generation
		if !sh.cells[id].hidden {
			input.status, input.hasStatus = record.statusInfo()
		}
		sh.mu.RUnlock()
		input.derived |= 16
		r.observe(input, conditionFacet, generation)
	}
	return input.status, input.hasStatus
}

// GetArtifact captures fetched content or accepted output for a dependency.
func (r *ReadSet) GetArtifact(id manifest.NamedResource) Artifact {
	input := r.input(id)
	if input.derived&4 != 0 {
		return input.artifact
	}
	obj := r.GetObject(id)
	sh := r.store.shardFor(id)
	sh.mu.RLock()
	record := sh.artifacts[id]
	if !sh.cells[id].hidden {
		input.artifact = record.value
	}
	sh.mu.RUnlock()
	input.derived |= 4
	r.observe(input, artifactFacet, record.generation)
	if e, ok := input.artifact.(*Emission); ok && !emissionCurrent(e, Child{Obj: obj, Doc: input.view.child.Doc}) {
		input.artifact = nil
	}
	return input.artifact
}

// Productions captures all contributors and discovery evidence for a named input.
func (r *ReadSet) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	input := r.input(id)
	input.derived |= 64
	return r.productions(input, index)
}

func (r *ReadSet) productions(input *Input, index *manifest.DiscoveryIndex) []manifest.Production {
	if input.productions == nil {
		result := productionResult{reads: r, captureTarget: true}
		r.store.visitProductions(input.Ref.NamedResource, index, &result)
		input.productions = result.out
		if input.productions == nil {
			input.productions = []manifest.Production{}
		}
	}
	return input.productions
}

// ReferenceFacts captures visible data and its accepted source-qualified authority.
func (r *ReadSet) ReferenceFacts(id manifest.NamedResource, index *manifest.DiscoveryIndex) ReferenceFacts {
	input := r.input(id)
	data, _ := r.Capture(input).(manifest.Data)
	return ReferenceFacts{Data: data, Accepted: input.view.accepted || input.view.owner != nil, Productions: r.Productions(id, index)}
}

// UnsettledProducers captures declaration contributors that can still supply settled bytes.
// Availability MUST NOT become a content projection for artifact reuse.
func (r *ReadSet) UnsettledProducers(id manifest.NamedResource) []manifest.NamedResource {
	var pending []manifest.NamedResource
	for _, production := range r.productions(r.input(id), nil) {
		owner := production.Owner
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
	if input.derived&128 != 0 {
		return input.objects
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
	out := make([]manifest.BaseManifest, 0, len(ids))
	for _, id := range ids {
		if obj := r.GetObject(id); obj != nil {
			out = append(out, obj)
		}
	}
	input.objects, input.derived = out, input.derived|128
	return out
}

// GetEmission binds candidate output to this execution's captured declaration and source.
func (r *ReadSet) GetEmission(id manifest.NamedResource) (manifest.BaseManifest, Emission) {
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
		input.Ref.Artifact, input.Ref.UnknownArtifactRoot, input.Ref.ChartSource = ref.Artifact, ref.UnknownArtifactRoot, ref.ChartSource
		input.materializer = materializer
		return
	}
	if sameReadinessGate(input.Ref, ref) && input.materializer == materializer {
		return
	}
	for gate := &input.gates; ; gate = &(*gate).next {
		if *gate == nil {
			*gate = &readinessGate{expr: ref.ReadyExpr, artifact: ref.Artifact, unknownArtifactRoot: ref.UnknownArtifactRoot, chartSource: ref.ChartSource, materializer: materializer}
			return
		}
		if sameReadinessGate(manifest.DependencyRef{NamedResource: ref.NamedResource, ReadyExpr: (*gate).expr, Artifact: (*gate).artifact, UnknownArtifactRoot: (*gate).unknownArtifactRoot, ChartSource: (*gate).chartSource}, ref) && (*gate).materializer == materializer {
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
			for gate := input.gates; gate != nil; gate = gate.next {
				if !yield(Gate{NamedResource: input.Ref.NamedResource, ReadyExpr: gate.expr, Artifact: gate.artifact, UnknownArtifactRoot: gate.unknownArtifactRoot, ChartSource: gate.chartSource, Materializer: gate.materializer}) {
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
	if head := input.cells[objectFacet-1].head; head != nil && input.cells[objectFacet-1].absent == nil {
		head.assessment.Store(r)
	}
}

// ReplacesInvalid reports whether this attempt supersedes a stale accepted assessment.
func (r *ReadSet) ReplacesInvalid() bool {
	r.GetObject(r.id)
	head := r.input(r.id).cells[objectFacet-1].head
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
func (r *ReadSet) Admit(obj manifest.BaseManifest, source, owner manifest.NamedResource) {
	r.GetArtifact(source)
	var support *Input
	if owner != (manifest.NamedResource{}) && owner != obj.Named() {
		support = r.input(owner)
	}
	r.store.addObject(obj, r.input(source), support)
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
	if emission.Reads != r {
		_, captured := r.GetEmission(r.id)
		emission.input, emission.inputDoc, emission.inputHead = captured.input, captured.inputDoc, captured.inputHead
		emission.binding, emission.coverage = captured.binding, captured.coverage
		captured.Finish()
	}
	emission.Reads = r
	r.produced = emission.Children
}

// Matches recaptures a predecessor's consumed projections in a fresh execution.
func (r *ReadSet) Matches(previous *ReadSet) bool {
	if previous == nil {
		return false
	}
	for input := previous.inputs; input != nil; input = input.next {
		id := input.Ref.NamedResource
		if input.derived&1 != 0 && !semanticEqual(Child{Obj: r.GetObject(id), Doc: r.input(id).view.child.Doc}, input.view.child) {
			return false
		}
		if input.declaration && (r.GetDeclaration(id) == nil) != (r.store.declaration(id, input.view) == nil) {
			return false
		}
		if input.derived&2 != 0 {
			var observed Observation
			_, observed = r.Snapshot(id, true)
			conditions := observed.Conditions
			if !slices.EqualFunc(conditions, input.conditions, conditionEqual) {
				return false
			}
		} else if input.derived&16 != 0 {
			status, ok := r.GetStatus(id)
			if ok != input.hasStatus || status.Status != input.status.Status || status.Reason != input.status.Reason || !sameMessage(status.Message, status.applyingObjects, input.status.Message, input.status.applyingObjects) || !slices.Equal(status.BlockedBy(), input.status.BlockedBy()) {
				return false
			}
		}
		if input.derived&4 != 0 && !sameArtifact(input.artifact, r.GetArtifact(id)) {
			return false
		}
		if input.derived&64 != 0 && !slices.Equal(input.productions, r.Productions(id, nil)) {
			return false
		}
		if input.derived&128 != 0 && !slices.EqualFunc(input.objects, r.ListObjects(id.Group, id.Kind), objectEqual) {
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
