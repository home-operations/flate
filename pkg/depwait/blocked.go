package depwait

import "github.com/home-operations/flate/pkg/manifest"

// ErrBlocked is the control-flow sentinel a dag-engine Require returns when a
// reconcile body cannot proceed because one or more dependencies are not yet
// satisfied — absent (not in the store), present-but-not-Ready, or carrying a
// ReadyExpr that has not yet evaluated true — AND none is terminally Failed.
// The scheduler parks the node keyed on Deps and re-runs the body when any of
// them advances; at the structural fixpoint a draining re-run terminalizes the
// node with the canonical failure status.
//
// It deliberately has NO Unwrap and wraps no sentinel: it must never satisfy an
// errors.Is chain for a user-facing failure (manifest.ErrInput / ErrFlux /
// ErrObjectNotFound) or be mistaken for a reconcile error. RunWithStatusOutcome
// intercepts it via errors.As before the generic Failed-status write, so a
// blocked node keeps the Pending status its Require wrote and stays re-runnable.
//
// Deps carries only the dependency identities, not a per-dep reason: the
// terminal failure message for a dependency that never becomes producible is
// re-derived structurally at the draining sweep (Classify re-reads the final
// store state) — so a stale park-time reason would be both redundant and, if
// the dep's state changed, wrong.
type ErrBlocked struct {
	Deps []manifest.NamedResource
}

func (e *ErrBlocked) Error() string { return "blocked on dependencies" }
