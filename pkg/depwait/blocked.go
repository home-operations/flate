package depwait

import "github.com/home-operations/flate/pkg/manifest"

// ErrBlocked carries parking identities without turning content suppliers into causes.
// It MUST remain outside the user-facing error sentinel chain.
type ErrBlocked struct {
	manifest.ReconcileWait
}

func (e *ErrBlocked) Error() string { return "blocked on dependencies" }
