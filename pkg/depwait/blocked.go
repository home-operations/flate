package depwait

import "github.com/home-operations/flate/pkg/manifest"

// ErrBlocked retains unresolved dependency identities for structural classification.
// It MUST remain separate from user-facing failure sentinels.
type ErrBlocked struct {
	Deps []manifest.NamedResource
}

func (e *ErrBlocked) Error() string { return "blocked on dependencies" }
