package store

import (
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
		manifest.KindExternalArtifact,
		manifest.KindBucket:
		return true
	}
	return false
}

// readyCondition builds the Ready condition that corresponds to the
// (status, message) pair UpdateStatus accepts. Reason is derived from
// Status; Message is passed through verbatim.
func readyCondition(status Status, message string) metav1.Condition {
	c := metav1.Condition{
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
func statusInfoFromConditions(conds []metav1.Condition) (StatusInfo, bool) {
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
func (s *Store) SetCondition(id manifest.NamedResource, cond Condition) {
	s.mu.Lock()
	prev := s.conditions[id]

	updated := make([]Condition, 0, len(prev)+1)
	replaced := false
	for _, c := range prev {
		if c.Type == cond.Type {
			if conditionEqual(c, cond) {
				// Identical condition — nothing to do, including no event.
				s.mu.Unlock()
				return
			}
			updated = append(updated, cond)
			replaced = true
			continue
		}
		updated = append(updated, c)
	}
	if !replaced {
		updated = append(updated, cond)
	}
	s.conditions[id] = updated
	newInfo, _ := statusInfoFromConditions(updated)
	s.mu.Unlock()

	s.fire(EventStatusUpdated, id, newInfo)
}

// GetStatus returns the Ready-derived StatusInfo for id and whether
// a Ready condition was present.
func (s *Store) GetStatus(id manifest.NamedResource) (StatusInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return statusInfoFromConditions(s.conditions[id])
}

// GetConditions returns a copy of id's condition list. Empty for
// unknown ids.
func (s *Store) GetConditions(id manifest.NamedResource) []Condition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	conds := s.conditions[id]
	if len(conds) == 0 {
		return nil
	}
	out := make([]Condition, len(conds))
	copy(out, conds)
	return out
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

// FailedResources returns every (id, info) currently in Failed state.
func (s *Store) FailedResources() map[manifest.NamedResource]StatusInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[manifest.NamedResource]StatusInfo)
	for id, conds := range s.conditions {
		if info, ok := statusInfoFromConditions(conds); ok && info.Status == StatusFailed {
			out[id] = info
		}
	}
	return out
}
