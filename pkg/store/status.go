package store

import (
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// Canonical StatusReady message vocabulary. flate overloads
// StatusReady with several "ready but…" sub-states encoded in the
// message text — depwait treats them uniformly as ready (so deps
// unblock), while consumers branch on the message to decide UI /
// propagation.
//
// Three sub-states exist today:
//
//   - SkippedPrefix — a resource soft-skipped by --allow-missing-
//     secrets (or by a consumer propagating a source-skip). Detect
//     via IsSkipped. Message format: "skipped: <reason>".
//   - MsgUnchanged — the change-filter excluded this resource
//     because its sources weren't in the diff. Detect via
//     IsUnchanged. Used by `flate test` for SKIPPED outcomes.
//   - MsgSuspended — spec.suspend was honored. Detect via
//     IsSuspended.
//
// All three are kept in one place so the literal-vs-helper drift
// (testrunner's `info.Message == "unchanged"` direct compare, etc.)
// has exactly one source of truth and a future rephrasing in
// controllers/base only requires updating the constant here.
const (
	SkippedPrefix = "skipped:"
	MsgUnchanged  = "unchanged"
	MsgSuspended  = "suspended"
	// MsgRefetching is the Pending-message Refire writes before
	// re-dispatching EventObjectAdded. Surfaced as a distinct sentinel
	// (vs. the source controller's own "fetching" mid-reconcile) so log
	// scraping can distinguish a Refire-triggered re-pull from a
	// first-time fetch.
	MsgRefetching = "re-fetching"
)

// IsSkipped reports whether info represents a soft-skip — i.e. a
// StatusReady whose message starts with SkippedPrefix.
func IsSkipped(info StatusInfo) bool {
	return info.Status == StatusReady && strings.HasPrefix(info.Message, SkippedPrefix)
}

// IsUnchanged reports whether info represents a change-filter
// exclusion (StatusReady + MsgUnchanged).
func IsUnchanged(info StatusInfo) bool {
	return info.Status == StatusReady && info.Message == MsgUnchanged
}

// IsSuspended reports whether info represents a spec.suspend honor
// (StatusReady + MsgSuspended).
func IsSuspended(info StatusInfo) bool {
	return info.Status == StatusReady && info.Message == MsgSuspended
}

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

// StatusInfo bundles a status with an optional descriptive message.
// Derived from the Ready condition; see Store.GetStatus.
type StatusInfo struct {
	Status  Status
	Message string
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

// SupportsStatus reports whether the given kind participates in the
// status pipeline. Kinds outside this set (ConfigMap, Secret, ...) are
// considered "ready" simply by existing.
func SupportsStatus(kind string) bool {
	switch kind {
	case manifest.KindKustomization,
		manifest.KindGitRepository,
		manifest.KindHelmRelease,
		manifest.KindHelmRepository,
		manifest.KindOCIRepository,
		manifest.KindHelmChart,
		manifest.KindExternalArtifact,
		manifest.KindBucket:
		return true
	}
	return false
}

// readyCondition builds the Ready condition that corresponds to the
// (status, message) pair UpdateStatus accepts. Reason is derived from
// Status; Message is passed through verbatim.
func readyCondition(status Status, message string) Condition {
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
	return c
}

// statusInfoFromConditions projects the rollup StatusInfo from the
// Ready condition. Returns (zero, false) when no Ready condition is
// present.
func statusInfoFromConditions(conds []Condition) (StatusInfo, bool) {
	for _, c := range conds {
		if c.Type != ConditionReady {
			continue
		}
		info := StatusInfo{Message: c.Message}
		switch c.Status {
		case metav1.ConditionTrue:
			info.Status = StatusReady
		case metav1.ConditionFalse:
			info.Status = StatusFailed
		default:
			info.Status = StatusPending
		}
		return info, true
	}
	return StatusInfo{}, false
}

// UpdateStatus records a Ready-condition transition and dispatches a
// StatusUpdated event when the StatusInfo rollup changes. Internally
// the (status, message) pair is stored as a metav1.Condition so future
// callers (ReadyExpr CEL, SOPS detection, healthChecks) can see the
// rich state via GetConditions.
func (s *Store) UpdateStatus(id manifest.NamedResource, status Status, message string) {
	s.SetCondition(id, readyCondition(status, message))
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
	sh := s.shardFor(id)
	sh.mu.Lock()
	updated, changed := sh.setConditionLocked(id, cond)
	if !changed {
		sh.mu.Unlock()
		return
	}
	newInfo, _ := statusInfoFromConditions(updated)
	dispatch := s.fireUnderLock(EventStatusUpdated, id, newInfo)
	sh.mu.Unlock()
	dispatch()
}

// setConditionLocked upserts cond into id's condition list and
// returns the updated list plus whether it actually changed (an
// identical re-write is a no-op). Caller MUST hold sh.mu — used both
// by SetCondition (which takes the lock itself) and by Refire (which
// already holds the lock for an atomic check-and-act).
//
// Mutates the existing slice in place when cond.Type already exists.
// The hot path is "same condition type, new status/message" (every
// reconcile transition flips the Ready condition); the prior
// implementation rebuilt the full slice on each hit, allocating an
// O(len(prev)) backing array per update. In-place overwrite drops
// that allocation entirely while preserving the no-op fast path
// (identical re-write returns the original slice with changed=false).
//
// Appending a never-seen type still allocates — that's the normal
// slice-growth path and unavoidable. The Conditions slice is short
// (Ready + occasional Healthy) so the steady state hits the
// overwrite branch on every reconcile.
//
// The returned slice ALIASES sh.conditions[id] when an existing entry
// is overwritten (it IS the live backing array). Listeners that read
// the slice MUST do so before the next write under sh.mu — current
// callers project a StatusInfo immediately under the same lock, so
// no aliasing hazard exists today. A future caller that holds onto
// the returned slice past sh.mu's release would observe further
// mutations and MUST copy first; statusInfoFromConditions matches
// this contract by reading values out of the slice in-place.
func (sh *shard) setConditionLocked(id manifest.NamedResource, cond Condition) (updated []Condition, changed bool) {
	prev := sh.conditions[id]
	for i := range prev {
		if prev[i].Type != cond.Type {
			continue
		}
		if conditionEqual(prev[i], cond) {
			return prev, false
		}
		prev[i] = cond
		// sh.conditions[id] already references this backing array —
		// no reassignment needed. Returning prev (now mutated) keeps
		// the original allocation alive instead of replacing it.
		return prev, true
	}
	// New condition type: extend the slice. This branch is rare
	// (Ready + Healthy is the only multi-type combination flate
	// emits today) so the per-call allocation cost is amortized
	// across the lifetime of the resource.
	updated = append(prev, cond)
	sh.conditions[id] = updated
	return updated, true
}

// GetStatus returns the Ready-derived StatusInfo for id and whether
// a Ready condition was present.
func (s *Store) GetStatus(id manifest.NamedResource) (StatusInfo, bool) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return statusInfoFromConditions(sh.conditions[id])
}

// GetConditions returns a copy of id's condition list. Empty for
// unknown ids.
func (s *Store) GetConditions(id manifest.NamedResource) []Condition {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	conds := sh.conditions[id]
	if len(conds) == 0 {
		return nil
	}
	return slices.Clone(conds)
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
		for id, conds := range sh.conditions {
			if _, inStore := sh.objects[id]; !inStore {
				continue
			}
			if info, ok := statusInfoFromConditions(conds); ok && info.Status == StatusFailed {
				out[id] = info
			}
		}
	}
	return out
}
