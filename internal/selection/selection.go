// Package selection shares status and failure-count rules for completed renders.
package selection

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// IsUnavailable reports whether a producer's output cannot be compared.
func IsUnavailable(info store.StatusInfo) bool {
	return info.Status == store.StatusFailed || store.IsSkipped(info) || store.IsSuspended(info)
}

// BlockedCount excludes dependency-cycle roots promoted to primary failures.
func BlockedCount(
	failed map[manifest.NamedResource]store.StatusInfo,
	blocked, causes map[manifest.NamedResource][]manifest.NamedResource,
) int {
	count := len(blocked)
	for root := range causes {
		if _, isFailed := failed[root]; isFailed {
			if _, promoted := blocked[root]; promoted {
				count--
			}
		}
	}
	return count
}
