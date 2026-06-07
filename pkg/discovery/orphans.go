package discovery

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/loader"
)

// promoteOrphans materializes Existence entries that no Kustomization
// will ever render. An "orphan" here is a file-indexed object whose
// source file is not under any loaded KS's spec.path — render-driven
// discovery would leave it stranded otherwise. Common shapes:
//
//   - A loose HelmRelease/source at repo root that `flate build`
//     should still render even with no enclosing KS.
//   - flux-system bootstrap CRs that exist beside the bootstrap KS
//     itself but aren't inside its spec.path tree.
//
// Entries whose path IS under a KS's spec.path stay in Existence —
// they'll be emitted by that KS's render via emitRenderedChildren
// (or resolved on-demand by depwait's lazy-promotion fallback when a
// substituteFrom edge fires before the producing KS reconciles).
//
// Shares the parent-path-prefix predicate with the orchestrator's
// detectOrphans via loader.LongestParent — the two run in
// complementary phases (pre-reconcile materialization here,
// post-reconcile failure demotion there) and must agree on the
// same "under a KS path" predicate so an object can't be classified
// orphan by one and parented by the other.
func (d *discoverer) promoteOrphans(prefixes []loader.KSPathPrefix) {
	if d.loader.Existence == nil {
		return
	}
	// prefixes is the same KS spec.path list discovery.Run computed for
	// the parent-index passes (identical repoRoot + shared component
	// cache). Reusing it keeps this and the parent index in lockstep on
	// the "under a KS path" predicate and avoids a third rebuild.
	for id := range d.loader.Existence.All() {
		if d.cfg.Store.GetObject(id) != nil {
			continue
		}
		file, hasFile := d.sourceFiles[id]
		if hasFile {
			if _, covered := loader.LongestParent(prefixes, file, id); covered {
				continue
			}
		}
		if !d.loader.Existence.Promote(d.cfg.Store, id, d.cfg.WipeSecrets) {
			slog.Debug("discovery: orphan promotion failed", "id", id.String())
		}
	}
}
