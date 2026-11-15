package depwait

import "github.com/home-operations/flate/pkg/manifest"

// BlockedError MUST remain separate from user-facing failure sentinels.
type BlockedError struct {
	Deps []manifest.NamedResource
}

func (e *BlockedError) Error() string { return "blocked on dependencies" }
