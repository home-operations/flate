// Package selection holds scoped projections of completed renders.
package selection

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Selection contains the objects and diagnostics projected onto requested roots.
// Borrowed manifests and document batches MUST remain immutable in use.
type Selection struct {
	Manifests []manifest.DocumentBatch
	Failed    map[manifest.NamedResource]store.StatusInfo
	Blocked   map[manifest.NamedResource][]manifest.NamedResource
	Warnings  []manifest.Warning
	// Objects contains selected roots after metadata filtering, grouped by requested kind.
	Objects []manifest.BaseManifest
	// Counts records matched roots per requested kind before label filtering.
	Counts map[string]int
	// Statuses contains roster statuses and eligible diagnostic failures. Blocked
	// messages are prepared for test reports without changing dependency identity.
	Statuses map[manifest.NamedResource]store.StatusInfo
	// ChartVersions contains versions resolved from selected HR chartRef sources.
	ChartVersions map[manifest.NamedResource]string
	// Causes maps prepared failure roots to sorted distinct blocked victims.
	// Missing roots have no Failed entry; closed cycle roots carry prepared Failed messages.
	Causes map[manifest.NamedResource][]manifest.NamedResource
	// Unavailable contains canonical failure, soft-skip or suspension evidence for
	// paired producer output, independently of fatal namespace scope.
	Unavailable map[manifest.NamedResource]store.StatusInfo
	// Unrelated contains prepared advisories for failures outside named fatal scope.
	Unrelated []manifest.Warning
	// External contains paired external-source skip disclosures, honoring explicit namespace scope.
	External []manifest.Warning
}

// BlockedCount excludes dependency-cycle roots promoted to primary failures.
func (r *Selection) BlockedCount() int {
	count := len(r.Blocked)
	for root := range r.Causes {
		if _, isFailed := r.Failed[root]; isFailed {
			if _, promoted := r.Blocked[root]; promoted {
				count--
			}
		}
	}
	return count
}
