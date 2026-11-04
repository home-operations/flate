package manifest

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
)

// WorkRecorder binds an execution identity to its run-owned counters.
type WorkRecorder struct {
	Key        WorkKey
	counters   *workCounters
	concurrent *uint64
}

// Scope returns attribution for this recorder's operation.
func (w *WorkRecorder) Scope() WorkScope {
	if w == nil {
		return WorkScope{}
	}
	if w.counters == nil {
		w.Init(&w.Key, WorkScope{})
	}
	if w.counters.closed.Load() {
		return WorkScope{}
	}
	return w.counters.scope(&w.Key, w.concurrent)
}

type workCounters struct {
	records    sync.Map
	identities sync.Map
	active     uint64
	initial    []WorkRecord
	used       atomic.Uint64
	closed     atomic.Bool
}

// AcquisitionWork retains one acquisition's counters in its caller's operation storage.
type AcquisitionWork struct {
	Scope WorkScope
}

// Lookup returns the cache-service cell for this acquisition.
func (w *AcquisitionWork) Lookup(input string) *WorkRecord {
	if w == nil {
		return nil
	}
	return w.Scope.Record(WorkCache, input)
}

// WorkScope carries attribution into the run-owned counter store.
type WorkScope struct {
	counters   *workCounters
	identity   *workIdentity
	concurrent *uint64
	current    string
}

// Recorder returns the run-owned recorder for an execution identity.
func (s WorkScope) Recorder(key *WorkKey) *WorkRecorder {
	if !s.enabled() {
		return nil
	}
	return &s.counters.scope(key, s.concurrent).identity.WorkRecorder
}

// Prepare records the acquisition identity derived by its cache owner.
func (w *AcquisitionWork) Prepare(input string, render bool) {
	if w == nil || !w.Scope.enabled() {
		return
	}
	w.Scope.Record(WorkPreparation, input).Add(&WorkCounts{RawKeyPrepares: 1})
	if !render {
		w.Scope.current = input
	}
}

// RenderWork retains one prepared render's physical stages in its execution owner.
type RenderWork struct {
	Scope   WorkScope
	pending AcquisitionWork
	history AcquisitionWork
}

// Attribution returns the render's caller-qualified attribution.
func (w *RenderWork) Attribution() WorkScope {
	if w == nil {
		return WorkScope{}
	}
	return w.Scope
}

// Admission returns this owner's declaration-preparation counters.
func (s WorkScope) Admission() *WorkRecord { return s.Record(WorkPreparation, "admission") }

// WorkSummary is an immutable, sorted snapshot after all work has joined.
type WorkSummary []WorkRecord

// Normalize orders and combines records with the same qualified identity.
func (s WorkSummary) Normalize() WorkSummary {
	slices.SortFunc(s, func(a, b WorkRecord) int { return a.Key.Compare(&b.Key) })
	n := 0
	for i := range s {
		record := &s[i]
		if record.Counts == (WorkCounts{}) {
			continue
		}
		if n != 0 && s[n-1].Key == record.Key {
			s[n-1].Counts.Add(&record.Counts)
			continue
		}
		s[n] = *record
		n++
	}
	return s[:n]
}

// Current returns the acquisition carried by this operation.
func (s WorkScope) Current() *WorkRecord {
	if s.current == "" {
		return nil
	}
	return s.Record(WorkAcquisition, s.current)
}

// StartConcurrent counts physical entry across the invocation.
func (s WorkScope) StartConcurrent() uint64 {
	if !s.enabled() {
		return 0
	}
	return atomic.AddUint64(s.concurrent, 1)
}

func (w *workCounters) record(key *WorkKey) *WorkRecord {
	if record, ok := w.records.Load(*key); ok {
		typed, _ := record.(*WorkRecord)
		return typed
	}
	var record *WorkRecord
	slot := w.used.Add(1) - 1
	if slot < uint64(len(w.initial)) {
		record = &w.initial[slot]
		record.Key = *key
	} else {
		record = &WorkRecord{Key: *key}
	}
	actual, _ := w.records.LoadOrStore(*key, record)
	typed, _ := actual.(*WorkRecord)
	return typed
}

// RenderWork returns run-owned state for this render authority.
func (s WorkScope) RenderWork() *RenderWork {
	if !s.enabled() {
		return nil
	}
	if work := s.identity.render.Load(); work != nil {
		return work
	}
	acquisition := s
	acquisition.current = ""
	work := &RenderWork{
		Scope:   s,
		pending: AcquisitionWork{Scope: acquisition},
		history: AcquisitionWork{Scope: acquisition},
	}
	s.identity.render.CompareAndSwap(nil, work)
	return s.identity.render.Load()
}

const (
	// WorkAdmission records declarations admitted during the invocation.
	WorkAdmission WorkPhase = "admission"
	// WorkPreparation records discovery and declaration preparation.
	WorkPreparation WorkPhase = "preparation"
	// WorkKustomize records actual Kustomize engine calls.
	WorkKustomize WorkPhase = "kustomize"
	// WorkHelm records actual Helm engine calls.
	WorkHelm WorkPhase = "helm"
	// WorkPostRender records Helm post-render operations.
	WorkPostRender WorkPhase = "post-render"
	// WorkSubstitution records post-build substitution.
	WorkSubstitution WorkPhase = "substitution"
	// WorkEvaluation records generated resource evaluation.
	WorkEvaluation WorkPhase = "evaluation"
	// WorkAcquisition records source and chart production.
	WorkAcquisition WorkPhase = "acquisition"
	// WorkValidation records cache and input validation.
	WorkValidation WorkPhase = "validation"
	// WorkCache records service by an existing content generation.
	WorkCache WorkPhase = "cache"
	// WorkIndexDecode records actual Helm index decoding.
	WorkIndexDecode WorkPhase = "index-decode"
	// WorkChartDecode records actual Helm chart decoding.
	WorkChartDecode WorkPhase = "chart-decode"
)

// Attribution returns this acquisition's caller-qualified attribution.
func (w *AcquisitionWork) Attribution() WorkScope {
	if w == nil {
		return WorkScope{}
	}
	return w.Scope
}

// Snapshot allocates only the final detached output and its requested extra records.
// All writers MUST join before the counters are detached.
func (s WorkScope) Snapshot(extra int) WorkSummary {
	if !s.enabled() {
		return nil
	}
	counters := s.counters
	counters.closed.Store(true)
	count := extra
	counters.records.Range(func(_, value any) bool {
		record, _ := value.(*WorkRecord)
		if record.Counts != (WorkCounts{}) {
			count++
		}
		return true
	})
	var summary WorkSummary
	if count != 0 {
		summary = make(WorkSummary, 0, count)
	}
	counters.records.Range(func(_, value any) bool {
		record, _ := value.(*WorkRecord)
		if record.Counts != (WorkCounts{}) {
			summary = append(summary, *record)
		}
		return true
	})
	counters.identities.Range(func(_, value any) bool {
		identity, _ := value.(*workIdentity)
		identity.render.Store(nil)
		return true
	})
	counters.records, counters.identities = sync.Map{}, sync.Map{}
	counters.initial = nil
	return summary.Normalize()
}

// WorkKey qualifies work by execution, authority, owner and owner-derived input.
type WorkKey struct {
	Tree   TreeID
	Owner  NamedResource
	Source NamedResource
	Phase  WorkPhase
	Input  string
}

// Compare orders detached work identities deterministically.
func (k *WorkKey) Compare(other *WorkKey) int {
	return cmp.Or(k.Tree.Compare(other.Tree), k.Owner.Compare(other.Owner), k.Source.Compare(other.Source),
		cmp.Compare(k.Phase, other.Phase), cmp.Compare(k.Input, other.Input))
}

// FinishConcurrent releases one physical operation's concurrency count.
func (s WorkScope) FinishConcurrent() {
	if s.enabled() {
		atomic.AddUint64(s.concurrent, ^uint64(0))
	}
}

type workIdentity struct {
	WorkRecorder

	render atomic.Pointer[RenderWork]
}

func (s WorkScope) enabled() bool { return s.counters != nil && !s.counters.closed.Load() }

func (w *workCounters) scope(key *WorkKey, concurrent *uint64) WorkScope {
	if identity, ok := w.identities.Load(*key); ok {
		typed, _ := identity.(*workIdentity)
		return WorkScope{counters: w, identity: typed, concurrent: concurrent}
	}
	identity := &workIdentity{Key: *key, counters: w, concurrent: concurrent}
	actual, _ := w.identities.LoadOrStore(*key, identity)
	typed, _ := actual.(*workIdentity)
	return WorkScope{counters: w, identity: typed, concurrent: concurrent}
}

// Traversal returns discovery traversal counters without assigning an execution owner.
func (s WorkScope) Traversal(source NamedResource) *WorkRecord {
	if !s.enabled() {
		return nil
	}
	key := s.identity.Key
	key.Source, key.Phase = source, WorkPreparation
	return s.counters.record(&key)
}

// Substitution returns the post-build substitution counter cell.
func (w *RenderWork) Substitution() *WorkRecord {
	if w == nil {
		return nil
	}
	return w.Scope.Record(WorkSubstitution, "")
}

// WorkCounts distinguishes attempted actions, service and structural traversal.
type WorkCounts struct {
	Attempts          uint64
	Completed         uint64
	Failures          uint64
	CacheService      uint64
	RemoteTransfers   uint64
	LocalAcquisitions uint64
	FileEntries       uint64
	ReaderEdges       uint64
	OwnerPrepares     uint64
	RawKeyPrepares    uint64
	// PeakConcurrent is a maximum, never an additive count.
	PeakConcurrent uint64
}

// Add combines detached counts and concurrency maxima.
func (c *WorkCounts) Add(other *WorkCounts) {
	c.Attempts += other.Attempts
	c.Completed += other.Completed
	c.Failures += other.Failures
	c.CacheService += other.CacheService
	c.RemoteTransfers += other.RemoteTransfers
	c.LocalAcquisitions += other.LocalAcquisitions
	c.FileEntries += other.FileEntries
	c.ReaderEdges += other.ReaderEdges
	c.OwnerPrepares += other.OwnerPrepares
	c.RawKeyPrepares += other.RawKeyPrepares
	c.PeakConcurrent = max(c.PeakConcurrent, other.PeakConcurrent)
}

// Record returns the stable counters for one qualified work identity.
func (s WorkScope) Record(phase WorkPhase, input string) *WorkRecord {
	if !s.enabled() {
		return nil
	}
	key := s.identity.Key
	key.Phase, key.Input = phase, input
	return s.counters.record(&key)
}

// WorkRecord contains only detached identity and scalar work counts.
type WorkRecord struct {
	Key    WorkKey
	Counts WorkCounts
}

// Add records work in a pre-established cell shared by nested operations.
func (r *WorkRecord) Add(c *WorkCounts) {
	if r == nil {
		return
	}
	if c.PeakConcurrent != 0 {
		peak := atomic.LoadUint64(&r.Counts.PeakConcurrent)
		for ; c.PeakConcurrent > peak; peak = atomic.LoadUint64(&r.Counts.PeakConcurrent) {
			if atomic.CompareAndSwapUint64(&r.Counts.PeakConcurrent, peak, c.PeakConcurrent) {
				break
			}
		}
	}
	for _, field := range [...]struct {
		target *uint64
		value  uint64
	}{
		{&r.Counts.Attempts, c.Attempts},
		{&r.Counts.Completed, c.Completed},
		{&r.Counts.Failures, c.Failures},
		{&r.Counts.CacheService, c.CacheService},
		{&r.Counts.RemoteTransfers, c.RemoteTransfers},
		{&r.Counts.LocalAcquisitions, c.LocalAcquisitions},
		{&r.Counts.FileEntries, c.FileEntries},
		{&r.Counts.ReaderEdges, c.ReaderEdges},
		{&r.Counts.OwnerPrepares, c.OwnerPrepares},
		{&r.Counts.RawKeyPrepares, c.RawKeyPrepares},
	} {
		if field.value != 0 {
			atomic.AddUint64(field.target, field.value)
		}
	}
}

// Init binds an owner to its tree's counters and the invocation's concurrency count.
func (w *WorkRecorder) Init(key *WorkKey, parent WorkScope) {
	w.Key = *key
	if parent.counters != nil && (parent.Identity().Tree == key.Tree || parent.Identity().Tree.Cluster == "") {
		w.counters = parent.counters
	} else {
		w.counters = new(workCounters)
	}
	w.concurrent = &w.counters.active
	if parent.counters != nil {
		w.concurrent = parent.concurrent
	}
}

// Pending returns the acquisition cells for this pending render.
func (w *RenderWork) Pending() *AcquisitionWork {
	if w == nil {
		return nil
	}
	return &w.pending
}

// PostRender returns the Helm post-render counter cell.
func (w *RenderWork) PostRender(input string) *WorkRecord {
	if w == nil {
		return nil
	}
	return w.Scope.Record(WorkPostRender, input)
}

// History returns the acquisition cells for this accepted render history.
func (w *RenderWork) History() *AcquisitionWork {
	if w == nil {
		return nil
	}
	return &w.history
}

// Validate returns this acquisition's validation cell.
func (w *AcquisitionWork) Validate() *WorkRecord {
	if w == nil {
		return nil
	}
	input := w.Scope.current
	return w.Scope.Record(WorkValidation, input)
}

// Reserve sizes the initial counter storage from known discovery identities.
// This method MUST run before concurrent recording starts.
func (s WorkScope) Reserve(identities int) {
	if s.enabled() && len(s.counters.initial) == 0 && identities > 0 {
		s.counters.initial = make([]WorkRecord, identities)
		s.counters.used.Store(0)
	}
}

// Source returns attribution for a declared source authority.
func (s WorkScope) Source(source NamedResource) WorkScope {
	if !s.enabled() {
		return WorkScope{}
	}
	key := s.identity.Key
	if key.Source == source {
		return s
	}
	key.Source = source
	if key.Owner == (NamedResource{}) {
		key.Owner = source
	}
	scoped := s.counters.scope(&key, s.concurrent)
	scoped.current = s.current
	return scoped
}

// WorkPhase identifies an operation at its owning implementation boundary.
type WorkPhase string

// Fingerprint records a raw render key derived by its cache owner.
func (w *RenderWork) Fingerprint(input string) {
	if w != nil {
		w.Scope.Record(WorkPreparation, input).Add(&WorkCounts{RawKeyPrepares: 1})
	}
}

// Engine returns the upstream renderer's counter cell.
func (w *RenderWork) Engine(phase WorkPhase, input string) *WorkRecord {
	if w == nil {
		return nil
	}
	return w.Scope.Record(phase, input)
}

// Preparation returns the cell for physical render preparation.
func (w *RenderWork) Preparation() *WorkRecord {
	if w == nil {
		return nil
	}
	return w.Scope.Record(WorkPreparation, "")
}

// Identity returns the operation's tree, owner and source authority.
func (s WorkScope) Identity() WorkKey {
	if !s.enabled() {
		return WorkKey{}
	}
	return s.identity.Key
}

// Finish records one attempted operation's completion or failure.
func (r *WorkRecord) Finish(err error) {
	if err != nil {
		r.Add(&WorkCounts{Failures: 1})
	} else {
		r.Add(&WorkCounts{Completed: 1})
	}
}

// Physical returns the acquisition's actual-production cell.
func (w *AcquisitionWork) Physical() *WorkRecord {
	if w == nil {
		return nil
	}
	return w.Scope.Current()
}

// Decode returns the physical chart-decoding counter cell.
func (w *RenderWork) Decode(input string) *WorkRecord {
	if w == nil {
		return nil
	}
	return w.Scope.Record(WorkChartDecode, input)
}
