package orchestrator

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// detectOrphans returns the subset of failed resources that are
// "orphans" — Kustomizations/HelmReleases whose source files sit
// under another Kustomization's spec.path but were never emitted by
// that parent's render output. Such files exist on disk but Flux
// would never see them, so flate downgrades the failure to a
// warning rather than gating the test on stale local files.
func (o *Orchestrator) detectOrphans(failed map[manifest.NamedResource]store.StatusInfo) map[manifest.NamedResource]struct{} {
	out := make(map[manifest.NamedResource]struct{})
	prefixes := loader.KSPathPrefixes(o.store, o.cfg.Path)
	for id := range failed {
		if id.Kind != manifest.KindKustomization && id.Kind != manifest.KindHelmRelease {
			continue
		}
		// A resource that any parent's render also emitted is by
		// definition not orphaned — kustomize-controller saw it.
		if o.rendered.has(id) {
			continue
		}
		file, ok := o.sourceFiles[id]
		if !ok {
			continue
		}
		if _, ok := loader.LongestParent(prefixes, file, id); ok {
			out[id] = struct{}{}
		}
	}
	return out
}

// finalize is the post-BlockTillDone reporting phase: demote orphans
// from Failed → Ready, log per-resource warnings, and assemble the
// aggregated error string. Pulled out of Run so the lifecycle entry
// point reads as start → drain → finalize.
func (o *Orchestrator) finalize() error {
	failed := o.store.FailedResources()
	o.demoteOrphans(failed)
	o.logSummary(failed)
	o.logResourceFailures(failed)

	if len(failed) == 0 {
		// Controllers attribute panics by marking the resource StatusFailed
		// (see kustomization/helmrelease/source controllers). This catches
		// any panic that escaped attribution — e.g. inside a future task
		// dispatched outside the per-resource recover.
		if n := o.tasks.Failures(); n > 0 {
			return fmt.Errorf("%d task(s) panicked without per-resource attribution; check logs", n)
		}
		return nil
	}
	return o.aggregateFailures(failed)
}

// demoteOrphans filters out resources whose source files sit under
// another Kustomization's spec.path but were never emitted by that
// parent's render. Real Flux wouldn't reconcile them either — the
// file walker only loaded them because flate scans the whole tree.
// Surface as warnings instead of failures so the test isn't gated on
// stale on-disk files the user has not wired into their kustomize
// tree. Mutates the failed map in place; the demoted ids land in
// o.orphans for Render() to surface.
func (o *Orchestrator) demoteOrphans(failed map[manifest.NamedResource]store.StatusInfo) {
	o.orphans = map[manifest.NamedResource]string{}
	for id := range o.detectOrphans(failed) {
		info := failed[id]
		o.orphans[id] = info.Message
		o.store.UpdateStatus(id, store.StatusReady, "orphaned (not referenced by any parent kustomization.yaml)")
		slog.Warn("resource orphaned", "id", id.String(),
			"file", o.sourceFiles[id],
			"reason", info.Message)
		delete(failed, id)
	}
}

func (o *Orchestrator) logSummary(failed map[manifest.NamedResource]store.StatusInfo) {
	ksCount := len(o.store.ListObjects(manifest.KindKustomization))
	hrCount := len(o.store.ListObjects(manifest.KindHelmRelease))
	slog.Info("reconcile complete",
		"kustomizations", ksCount,
		"helmReleases", hrCount,
		"failed", len(failed))
	// Surface a clear warning when the scan turned up nothing — covers
	// the "typo'd --path that happens to be an empty directory" case
	// where flate would otherwise look like a silent success.
	if ksCount == 0 && hrCount == 0 {
		slog.Warn("no Flux Kustomization or HelmRelease objects found under --path; check the path is correct")
	}
}

func (o *Orchestrator) logResourceFailures(failed map[manifest.NamedResource]store.StatusInfo) {
	for id, info := range sanitizeFailed(failed) {
		// Demoted to Debug: the same failure list is surfaced as a
		// structured error by aggregateFailures (with file paths
		// included) AND echoed by the test runner / build CLI's own
		// per-resource output. Logging at Warn here double-emits to
		// stderr alongside the user-facing report and reads as
		// "flate had an internal error" when it's just normal
		// per-resource Flux failures the user expects to see.
		args := []any{"id", id.String(), "reason", info.Message}
		if f := o.sourceFiles[id]; f != "" {
			args = append(args, "file", f)
		}
		slog.Debug("resource failed", args...)
	}
}

func (o *Orchestrator) aggregateFailures(failed map[manifest.NamedResource]store.StatusInfo) error {
	msgs := make([]string, 0, len(failed))
	for id, info := range sanitizeFailed(failed) {
		if f := o.sourceFiles[id]; f != "" {
			msgs = append(msgs, fmt.Sprintf("%s (%s): %s", id.String(), f, info.Message))
		} else {
			msgs = append(msgs, fmt.Sprintf("%s: %s", id.String(), info.Message))
		}
	}
	slices.Sort(msgs) // deterministic ordering across runs
	return fmt.Errorf("reconcile completed with %d failure(s):\n  %s",
		len(msgs), strings.Join(msgs, "\n  "))
}

// sanitizeFailed returns a copy of the failure map with each entry's
// Message stripped of the `flux error: …: ` sentinel chain. Three
// callers (logResourceFailures, aggregateFailures, Result.Failed)
// previously inlined the same manifest.TrimSentinelPrefix call; if a
// fourth reader appears the strip rule must be applied once at
// snapshot time rather than re-derived at every consumer.
//
// The Store keeps the raw, sentinel-prefixed messages so errors.Is
// chains on user-facing strings still work — only the projection
// the orchestrator hands out is sanitized.
func sanitizeFailed(failed map[manifest.NamedResource]store.StatusInfo) map[manifest.NamedResource]store.StatusInfo {
	out := make(map[manifest.NamedResource]store.StatusInfo, len(failed))
	for id, info := range failed {
		info.Message = manifest.TrimSentinelPrefix(info.Message)
		out[id] = info
	}
	return out
}
