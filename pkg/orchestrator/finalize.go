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
// that parent's render output, AND whose covering parent itself
// succeeded. Such files exist on disk but Flux would never see them,
// so flate downgrades the failure to a warning rather than gating the
// test on stale local files. A child under a parent that FAILED is a
// blocked cascade victim, not an orphan — it stays failed (see below).
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
		if !isReconcilableKind(id.Kind) {
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
		parent, ok := loader.LongestParent(prefixes, file, id)
		if !ok {
			continue
		}
		// A covering parent that itself FAILED emitted nothing, so every child
		// under its spec.path looks unreferenced — but these are blocked
		// cascade victims, not stale unwired files. Leave them Failed (with the
		// BlockedBy the dependency gate recorded) so the report folds them under
		// the root cause instead of demoting each to a quiet "orphaned" warning.
		// A child under a parent that SUCCEEDED but still didn't emit it is the
		// genuine orphan this detection exists for.
		if _, parentFailed := failed[parent]; parentFailed {
			continue
		}
		out[id] = info.Message
	}
	return out
}

// finalize is the post-BlockTillDone reporting phase: demote orphans
// from Failed → Ready, log per-resource warnings, and assemble the
// aggregated error string. Pulled out of Run so the lifecycle entry
// point reads as start → drain → finalize.
func (o *Orchestrator) finalize() error {
	failed := o.store.FailedResources()
	ksCount, hrCount := o.cascadeParentFailures(failed)
	o.demoteOrphans(failed)
	o.logSummary(failed, ksCount, hrCount)
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
func (o *Orchestrator) cascadeParentFailures(failed map[manifest.NamedResource]store.StatusInfo) (ksCount, hrCount int) {
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
		o.store.SetBlocked(child, []manifest.NamedResource{parent})
		failed[child] = store.StatusInfo{Status: store.StatusFailed, Message: msg}
		slog.Debug("cascaded parent failure to child",
			"child", child.String(),
			"parent", parent.String(),
			"reason", parentInfo.Message)
	}
	// Count KS/HR while we already hold the listings, so logSummary
	// doesn't re-scan the store for the same two counts. cascade only
	// mutates status (never adds/removes objects), so these counts equal
	// what a later ListObjects would report.
	for _, kind := range reconcilableKinds {
		objs := o.store.ListObjects(kind)
		switch kind {
		case manifest.KindKustomization:
			ksCount = len(objs)
		case manifest.KindHelmRelease:
			hrCount = len(objs)
		}
		for _, obj := range objs {
			cascade(obj.Named())
		}
	}
	return ksCount, hrCount
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

func (o *Orchestrator) logSummary(failed map[manifest.NamedResource]store.StatusInfo, ksCount, hrCount int) {
	// ksCount/hrCount are passed in from cascadeParentFailures, which
	// already listed both kinds — no redundant re-scan here.
	slog.Debug("reconcile complete",
		"kustomizations", ksCount,
		"helm_releases", hrCount,
		"failed", len(failed))
	// Surface a clear advisory when the scan turned up nothing — covers
	// the "typo'd --path that happens to be an empty directory" case
	// where flate would otherwise look like a silent success.
	if ksCount == 0 && hrCount == 0 {
		o.store.AddWarning(manifest.Warning{
			Category: manifest.WarnEmptyScan,
			Message:  "no Flux Kustomization or HelmRelease objects found under --path; check the path is correct",
		})
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

// FailuresError aggregates per-resource reconcile failures into one error. Its
// identity (recovered with errors.As) — not its message text — is how the CLI
// tells the resource-failure aggregate apart from incidental run errors (a
// panic, a cancellation) when it re-scopes failures to a namespace filter and
// re-renders them.
type FailuresError struct{ Message string }

func (e *FailuresError) Error() string { return e.Message }

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
	return &FailuresError{Message: fmt.Sprintf("reconcile completed with %d failure(s):\n  %s",
		len(msgs), strings.Join(msgs, "\n  "))}
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
