package store

import (
	"errors"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// Status is the processing state of a resource as projected from its
// Ready condition. Kept as a high-level rollup for callers (depwait,
// `test` reporting) that don't need the full condition slice.
type Status string

// Possible Status values.
const (
	StatusPending Status = "Pending"
	StatusReady   Status = "Ready"
	StatusFailed  Status = "Failed"
)

// StatusInfo projects readiness, reason, message and failure provenance from a Ready condition.
type StatusInfo struct {
	Status  Status
	Reason  StatusReason
	Message string
	failure *dependencyFailure
}

// StatusReason identifies a Ready condition's machine-readable reason.
type StatusReason string

// Special Ready condition reasons.
const (
	ReasonSkipped   StatusReason = "Skipped"
	ReasonUnchanged StatusReason = "Unchanged"
	ReasonSuspended StatusReason = "Suspended"
)

// Outcome classifies a resource's readiness and failure provenance.
type Outcome string

// Resource outcomes.
const (
	OutcomePending   Outcome = "Pending"
	OutcomePassed    Outcome = "Passed"
	OutcomeFailed    Outcome = "Failed"
	OutcomeBlocked   Outcome = "Blocked"
	OutcomeSkipped   Outcome = "Skipped"
	OutcomeUnchanged Outcome = "Unchanged"
	OutcomeSuspended Outcome = "Suspended"
)

// Outcome returns the semantic outcome independently of diagnostic text.
func (i StatusInfo) Outcome() Outcome {
	switch i.Status {
	case StatusFailed:
		if i.IsDependencyFailure() {
			return OutcomeBlocked
		}
		return OutcomeFailed
	case StatusReady:
		switch i.Reason {
		case ReasonSkipped:
			return OutcomeSkipped
		case ReasonUnchanged:
			return OutcomeUnchanged
		case ReasonSuspended:
			return OutcomeSuspended
		default:
			return OutcomePassed
		}
	default:
		return OutcomePending
	}
}

// IsUnavailable reports whether a producer's output cannot be compared.
func (i StatusInfo) IsUnavailable() bool {
	switch i.Outcome() {
	case OutcomeFailed, OutcomeBlocked, OutcomeSkipped, OutcomeSuspended:
		return true
	default:
		return false
	}
}

// IsDependencyFailure reports whether the Failed status came from failed dependencies.
func (i StatusInfo) IsDependencyFailure() bool {
	return i.failure != nil
}

// BlockedBy returns a copy of the immediate failed dependencies in this snapshot.
func (i StatusInfo) BlockedBy() []manifest.NamedResource {
	if i.failure == nil {
		return nil
	}
	return slices.Clone(i.failure.ids)
}

// Failure IDs are immutable so StatusInfo can retain them after the shard unlocks.
type conditionRecord struct {
	state      *conditionState
	conditions []Condition
	failure    *dependencyFailure
}

type conditionState struct {
	authority
	first [1]Condition
}

func (r conditionRecord) stamp() authority {
	if r.state == nil {
		return authority{}
	}
	return r.state.authority
}

func (r conditionRecord) advanceLocked(sh *shard) {
	if r.state.generation == ^uint64(0) {
		sh.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	r.state.authority = sh.advanceLocked(r.state.authority)
}

type dependencyFailure struct {
	ids []manifest.NamedResource
}

func newDependencyFailure(err error) *dependencyFailure {
	dep, ok := errors.AsType[*manifest.DependencyFailedError](err)
	if !ok || len(dep.Failed) == 0 {
		return nil
	}
	ids := slices.Clone(dep.Failed)
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return &dependencyFailure{ids: ids}
}

// same reports whether f and o record the same failed dependencies; nil means none.
func (f *dependencyFailure) same(o *dependencyFailure) bool {
	return f == o || f != nil && o != nil && slices.Equal(f.ids, o.ids)
}

// Condition is an alias for k8s metav1.Condition. flate stores
// per-resource conditions so SOPS-encrypted-secret detection,
// health-check rollups, and Flux's dependsOn ReadyExpr CEL evaluation
// can interoperate with the rest of the K8s ecosystem.
type Condition = metav1.Condition

// Condition type identifiers used by flate. These mirror Flux's
// conventions so a future watch-mode could publish to a real cluster
// without translating.
const (
	ConditionReady   = "Ready"
	ConditionHealthy = "Healthy"
)

// Condition reasons attached to the Ready condition.
const (
	ReasonSucceeded   = "Succeeded"
	ReasonFailed      = "Failed"
	ReasonReconciling = "Reconciling"
)

func readyCondition(status Status, reason StatusReason, message string) Condition {
	c := Condition{
		Type:    ConditionReady,
		Message: message,
	}
	switch status {
	case StatusReady:
		c.Status = metav1.ConditionTrue
		c.Reason = ReasonSucceeded
	case StatusFailed:
		c.Status = metav1.ConditionFalse
		c.Reason = ReasonFailed
	default: // StatusPending or unknown
		c.Status = metav1.ConditionUnknown
		c.Reason = ReasonReconciling
	}
	if reason != "" {
		c.Reason = string(reason)
	}
	return c
}

// statusInfo projects the rollup StatusInfo from the
// Ready condition. Returns (zero, false) when no Ready condition is
// present.
func (r conditionRecord) statusInfo() (StatusInfo, bool) {
	for i := range r.conditions {
		c := &r.conditions[i]
		if c.Type != ConditionReady {
			continue
		}
		switch c.Status {
		case metav1.ConditionTrue:
			return StatusInfo{Status: StatusReady, Reason: StatusReason(c.Reason), Message: c.Message}, true
		case metav1.ConditionFalse:
			return StatusInfo{Status: StatusFailed, Reason: StatusReason(c.Reason), Message: c.Message, failure: r.failure}, true
		default:
			return StatusInfo{Status: StatusPending, Reason: StatusReason(c.Reason), Message: c.Message}, true
		}
	}
	return StatusInfo{}, false
}

// UpdateStatus records a Ready-condition transition and dispatches a
// StatusUpdated event when the StatusInfo rollup changes. Snapshot
// exposes the full condition state. An empty reason derives the normal reason from status.
func (s *Store) UpdateStatus(id manifest.NamedResource, status Status, reason StatusReason, message string) {
	s.SetCondition(id, readyCondition(status, reason, message))
}

// ReportProgress dispatches transient phase text without changing Ready conditions.
func (s *Store) ReportProgress(id manifest.NamedResource, message string) {
	dispatch := fireUnderLock(s, EventStatusUpdated, id)
	if dispatch.listeners != nil {
		dispatch.payload = StatusInfo{Status: StatusPending, Reason: ReasonReconciling, Message: message}
	}
	dispatch.run()
}

// UpdateFailure atomically records a terminal error and its immediate failed dependencies.
func (s *Store) UpdateFailure(id manifest.NamedResource, err error) {
	s.setCondition(id, readyCondition(StatusFailed, "", err.Error()), newDependencyFailure(err))
}

// SetCondition upserts cond into the resource's condition list keyed
// by cond.Type. Dispatches a StatusUpdated event with the StatusInfo
// rollup (derived from Ready) on every observable condition change,
// not just Ready transitions. Listeners that only care about Ready
// can filter on the StatusInfo payload; CEL-based ReadyExpr watchers
// need the broader notification so a Healthy condition flip (for
// example) wakes them.
//
// An identical re-write of the same condition is a no-op (no event).
//
// SetCondition does NOT require the object to be in the store —
// callers may legitimately establish initial state before AddObject
// lands (e.g. tests). The FailedResources rollup filters phantoms
// by intersecting against the object map at read time.
func (s *Store) SetCondition(id manifest.NamedResource, cond Condition) {
	s.setCondition(id, cond, nil)
}

func (s *Store) setCondition(id manifest.NamedResource, cond Condition, failure *dependencyFailure) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	updated, changed := sh.setConditionLocked(id, cond, failure)
	if !changed {
		sh.mu.Unlock()
		return
	}
	if sh.derived && sh.cells[id].hidden {
		sh.mu.Unlock()
		return
	}
	dispatch := fireUnderLock(s, EventStatusUpdated, id)
	if dispatch.listeners != nil {
		newInfo, _ := updated.statusInfo()
		dispatch.payload = newInfo
	}
	sh.mu.Unlock()
	dispatch.run()
}

// Caller MUST hold sh.mu and project conditions before unlocking: the returned
// record aliases the live condition slice. Failure IDs remain immutable.
// Existing conditions with unchanged provenance MUST be overwritten in place
// without reassigning the map entry, preserving the allocation-free hot path.
func (sh *shard) setConditionLocked(id manifest.NamedResource, cond Condition, failure *dependencyFailure) (conditionRecord, bool) {
	record := sh.conditions[id]
	failureChanged := cond.Type == ConditionReady && !record.failure.same(failure)
	for i := range record.conditions {
		if record.conditions[i].Type != cond.Type {
			continue
		}
		changed := !conditionEqual(record.conditions[i], cond)
		if changed || failureChanged {
			record.advanceLocked(sh)
		}
		if changed {
			record.conditions[i] = cond
		}
		if failureChanged {
			record.failure = failure
			sh.conditions[id] = record
		}
		return record, changed || failureChanged
	}
	if record.state == nil {
		if !sh.canAdmit(1) {
			sh.mu.Unlock()
			panic("store: authority generation exhausted")
		}
		sh.admitAbsenceLocked(id, conditionFacet)
		record.state = &conditionState{}
		record.conditions = record.state.first[:0]
	}
	record.advanceLocked(sh)
	if failureChanged {
		record.failure = failure
	}
	record.conditions = append(record.conditions, cond)
	sh.conditions[id] = record
	return record, true
}

// GetStatus returns the Ready-derived StatusInfo for id and whether
// a Ready condition was present.
func (s *Store) GetStatus(id manifest.NamedResource) (StatusInfo, bool) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if sh.derived && sh.cells[id].hidden {
		return StatusInfo{}, false
	}
	return sh.conditions[id].statusInfo()
}

// conditionEqual reports whether two conditions carry the same
// observable state. LastTransitionTime is intentionally ignored — it
// is reset on every transition by the controller-runtime libraries
// and would otherwise prevent the no-op short-circuit from firing.
func conditionEqual(a, b Condition) bool {
	return a.Type == b.Type &&
		a.Status == b.Status &&
		a.Reason == b.Reason &&
		a.Message == b.Message &&
		a.ObservedGeneration == b.ObservedGeneration
}

// FailedResources returns every (id, info) currently in Failed state
// for an object that is also present in the store. The "also present"
// gate filters phantom entries: DeleteObject wipes the conditions
// map, but a SetCondition that lands AFTER the delete (e.g. a slow
// reconcile goroutine writing back its terminal status) would
// otherwise resurrect a failure entry for an id that no longer exists.
// Conditioning on s.objects ensures FailedResources never reports a
// failure for a resource the orchestrator has explicitly removed.
//
// Iterating conditions rather than objects is faster when most objects
// don't have conditions yet (common during bootstrap) — avoids the
// secondary map lookup for every un-reconciled object.
//
// Cross-shard read: walks every shard's conditions/objects. Each
// shard is RLocked independently in canonical (ascending) order via
// rLockAll because conditions and objects for a single id always
// share the same shard, so per-shard reads suffice to detect the
// phantom — no global lock needed beyond the canonical-order ordering
// itself, which prevents lockAll-vs-rLockAll deadlocks.
func (s *Store) FailedResources() map[manifest.NamedResource]StatusInfo {
	s.rLockAll()
	defer s.rUnlockAll()
	out := make(map[manifest.NamedResource]StatusInfo)
	for _, sh := range s.shards {
		for id, record := range sh.conditions {
			if sh.objectLocked(id) == nil {
				continue
			}
			if info, ok := record.statusInfo(); ok && info.Status == StatusFailed {
				out[id] = info
			}
		}
	}
	return out
}
