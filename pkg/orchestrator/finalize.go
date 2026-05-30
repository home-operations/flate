package orchestrator

import (
	"errors"
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
func (o *Orchestrator) detectOrphans(failed map[manifest.NamedResource]store.StatusInfo) map[manifest.NamedResource]string {
	out := make(map[manifest.NamedResource]string)
	// Use repoRoot (not cfg.Path) so KSPathPrefixes' component
	// lookups read from the actual repo root rather than a
	// potentially-subdir --path. Same fix as PR #358 applied to
	// the orchestrator side. Route through the shared component
	// cache populated during Bootstrap so the per-KS component file
	// reads are served from memory rather than re-stat'd here.
	prefixes := loader.KSPathPrefixesWithCache(o.store, o.repoRoot, o.componentCache)
	for id, info := range failed {
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
			out[id] = info.Message
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
	o.cascadeParentFailures(failed)
	o.demoteOrphans(failed)
	o.logSummary(failed)
	o.logResourceFailures(failed)

	// Compose any unattributed panic count with the aggregated
	// per-resource failures so the panic signal isn't dropped when
	// other resources also failed. Pre-fix, the panic counter was
	// only consulted on the clean-aggregate path; a panicked task
	// running alongside any normal failure looked indistinguishable
	// from a clean reconcile to Service.Failures()-driven callers.
	var panicErr error
	if n := o.tasks.Failures(); n > 0 {
		panicErr = fmt.Errorf("%d task(s) panicked without per-resource attribution; check logs", n)
	}
	if len(failed) == 0 {
		return panicErr
	}
	if panicErr != nil {
		return errors.Join(o.aggregateFailures(failed), panicErr)
	}
	return o.aggregateFailures(failed)
}

// cascadeParentFailures downgrades render-emitted children whose
// ancestor (via renderedSet.ParentOf) ended up Failed. Closes a race
// window where a parent KS's first reconcile sets Ready, emits
// children, and then a later re-emission of that parent fails. Without
// this cascade, dependents that already passed their parent gate stay
// Ready in the final report even though the parent is Failed.
//
// Walks the ParentOf chain per child so a deep render tree (grand-
// parent → parent → child) propagates failures all the way down in
// one pass. Cycle-guard via a visited set keeps the walk bounded.
func (o *Orchestrator) cascadeParentFailures(failed map[manifest.NamedResource]store.StatusInfo) {
	ancestorFailure := func(child manifest.NamedResource) (manifest.NamedResource, store.StatusInfo, bool) {
		visited := map[manifest.NamedResource]struct{}{child: {}}
		cur := child
		for {
			parent, ok := o.rendered.ParentOf(cur)
			if !ok {
				return manifest.NamedResource{}, store.StatusInfo{}, false
			}
			if _, dup := visited[parent]; dup {
				return manifest.NamedResource{}, store.StatusInfo{}, false
			}
			visited[parent] = struct{}{}
			if info, isFailed := failed[parent]; isFailed {
				return parent, info, true
			}
			cur = parent
		}
	}
	cascade := func(child manifest.NamedResource) {
		if _, alreadyFailed := failed[child]; alreadyFailed {
			return
		}
		info, ok := o.store.GetStatus(child)
		if !ok || info.Status != store.StatusReady {
			return
		}
		parent, parentInfo, hasFailed := ancestorFailure(child)
		if !hasFailed {
			return
		}
		msg := fmt.Sprintf("parent %s failed: %s", parent.String(), parentInfo.Message)
		o.store.UpdateStatus(child, store.StatusFailed, msg)
		failed[child] = store.StatusInfo{Status: store.StatusFailed, Message: msg}
		slog.Debug("cascaded parent failure to child",
			"child", child.String(),
			"parent", parent.String(),
			"reason", parentInfo.Message)
	}
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, obj := range o.store.ListObjects(kind) {
			cascade(obj.Named())
		}
	}
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
	o.orphans = o.detectOrphans(failed)
	for id, msg := range o.orphans {
		o.store.UpdateStatus(id, store.StatusReady, "orphaned (not referenced by any parent kustomization.yaml)")
		slog.Warn("resource orphaned", "id", id.String(),
			"file", o.sourceFiles[id],
			"reason", msg)
		delete(failed, id)
	}
}

func (o *Orchestrator) logSummary(failed map[manifest.NamedResource]store.StatusInfo) {
	ksCount := len(o.store.ListObjects(manifest.KindKustomization))
	hrCount := len(o.store.ListObjects(manifest.KindHelmRelease))
	slog.Debug("reconcile complete",
		"kustomizations", ksCount,
		"helm_releases", hrCount,
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
		if f := o.sourceFiles[id]; f != "" {
			slog.Debug("resource failed", "id", id.String(), "reason", info.Message, "file", f)
		} else {
			slog.Debug("resource failed", "id", id.String(), "reason", info.Message)
		}
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
