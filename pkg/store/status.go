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
	ConditionReady       = "Ready"
	ConditionReconciling = "Reconciling"
	ConditionHealthy     = "Healthy"
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
		manifest.KindOCIRepository:
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
