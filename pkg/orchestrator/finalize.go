package orchestrator

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
func (o *Orchestrator) detectOrphans(failed map[manifest.NamedResource]store.StatusInfo, prefixes []manifest.KSClaim) map[manifest.NamedResource]string {
	out := make(map[manifest.NamedResource]string)
	// External sources own decode errors but cannot parent local-tree orphans.
	prefixes = slices.DeleteFunc(slices.Clone(prefixes), func(claim manifest.KSClaim) bool {
		ks, _ := o.store.Get[*manifest.Kustomization](claim.ID)
		_, _, external := loader.SourceView(o.store, ks, o.repoRoot)
		return external
	})
	for id, info := range failed {
		if !isReconcilableKind(id.Kind) {
			continue
		}
		// A resource that any parent's render also emitted is by
		// definition not orphaned — kustomize-controller saw it.
		if _, emitted := o.store.EmittedParent(id); emitted {
			continue
		}
		record, _ := o.index.Lookup(id)
		file := record.File
		if file == "" {
			continue
		}
		parent, ok := manifest.StrictParent(prefixes, file, id, nil)
		if !ok {
			// No covering spec.path parent. A Flux KS that is a stray manifest its
			// own kustomize base excludes (apps/base/app-X/ks.yaml not listed in the
			// dir's kustomization.yaml) is one real Flux never applies — demote it
			// rather than hard-failing a file that isn't wired into any tree (#777).
			if record.Excluded {
				out[id] = info.Message
			}
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
	// Final ownership includes every Kustomization present after the drain.
	var prefixes []manifest.KSClaim
	if len(failed) != 0 || (o.index != nil && len(o.index.Errors) != 0) {
		prefixes = loader.Claims(o.store, o.repoRoot)
	}
	o.orphans = o.detectOrphans(failed, prefixes)
	for _, id := range slices.SortedFunc(maps.Keys(o.orphans), manifest.NamedResource.Compare) {
		o.store.UpdateStatus(id, store.StatusReady, "orphaned (not referenced by any parent kustomization.yaml)")
		slog.Warn("resource orphaned", "id", id.String(),
			"file", o.fileOf(id),
			"reason", o.orphans[id])
		delete(failed, id)
	}
	o.warnMissingCRDs()
	o.logSummary(failed, ksCount, hrCount)
	o.logResourceFailures(failed)

	var errs []error
	if n := o.tasks.Failures(); n > 0 {
		errs = append(errs, fmt.Errorf("%d task(s) panicked without per-resource attribution; check logs", n))
	}
	if len(failed) != 0 {
		errs = append(errs, o.aggregateFailures(failed))
	}
	if o.index != nil {
		for _, file := range slices.Sorted(maps.Keys(o.index.Errors)) {
			_, owned := manifest.StrictParent(prefixes, file, manifest.NamedResource{}, nil)
			if !owned {
				for range o.index.ReadersOfFile(file) {
					owned = true
					break
				}
			}
			if !owned {
				errs = append(errs, o.index.Errors[file])
			}
		}
	}

	return errors.Join(errs...)
}

// warnMissingCRDs uses only final canonical references and Ready/artifact
// eligibility; retained output does not establish which revision passed a gate.
func (o *Orchestrator) warnMissingCRDs() {
	if !o.cfg.AllowMissingCRDs {
		return
	}
	missing := make(map[manifest.NamedResource]map[manifest.NamedResource]struct{})
	for _, obj := range o.store.ListObjects(manifest.KindResourceSet) {
		rs, ok := obj.(*manifest.ResourceSet)
		if !ok {
			continue
		}
		id := rs.Named()
		info, ok := o.store.GetStatus(id)
		if !ok || info.Status != store.StatusReady {
			continue
		}
		if _, ok := o.store.GetArtifact(id).(*store.ResourceSetArtifact); !ok {
			continue
		}
		for _, dep := range rs.DependsOn {
			if dep.Kind != manifest.KindCustomResourceDefinition {
				continue
			}
			crd := manifest.NamedResource{Kind: dep.Kind, Name: dep.Name}
			if o.store.GetObject(crd) != nil {
				continue
			}
			if _, ok := o.store.GetStatus(crd); ok {
				continue
			}
			if missing[crd] == nil {
				missing[crd] = make(map[manifest.NamedResource]struct{})
			}
			missing[crd][id] = struct{}{}
		}
	}
	for _, crd := range slices.SortedFunc(maps.Keys(missing), manifest.NamedResource.Compare) {
		dependents := slices.SortedFunc(maps.Keys(missing[crd]), manifest.NamedResource.Compare)
		names := make([]string, len(dependents))
		for i, id := range dependents {
			names[i] = id.String()
		}
		o.store.AddWarning(manifest.Warning{
			Resource: crd,
			Category: manifest.WarnMissingCRD,
			Count:    1,
			Message:  fmt.Sprintf("CRD %s absent from offline inputs; accepted for %s", crd.String(), strings.Join(names, ", ")),
		})
	}
}

// cascadeParentFailures downgrades render-emitted children whose
// ancestor (via Store.EmittedParent) ended up Failed. Closes a race
// window where a parent KS's first reconcile sets Ready, emits
// children, and then a later re-emission of that parent fails. Without
// this cascade, dependents that already passed their parent gate stay
// Ready in the final report even though the parent is Failed.
//
// Walks the parent ownership chain per child so a deep render tree (grand-
// parent → parent → child) propagates failures all the way down in
// one pass. Cycle-guard via a visited set keeps the walk bounded.
func (o *Orchestrator) cascadeParentFailures(failed map[manifest.NamedResource]store.StatusInfo) (ksCount, hrCount int) {
	ancestorFailure := func(child manifest.NamedResource) (manifest.NamedResource, store.StatusInfo, bool) {
		visited := map[manifest.NamedResource]struct{}{child: {}}
		cur := child
		for {
			parent, ok := o.store.EmittedParent(cur)
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
		o.store.UpdateFailure(child, &manifest.DependencyFailedError{
			Parent: child, Failed: []manifest.NamedResource{parent},
			Reasons: map[manifest.NamedResource]string{parent: parentInfo.Message},
		})
		failed[child], _ = o.store.GetStatus(child)
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
		if f := o.fileOf(id); f != "" {
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
		if f := o.fileOf(id); f != "" {
			msgs = append(msgs, fmt.Sprintf("%s (%s): %s", id.String(), f, info.Message))
		} else {
			msgs = append(msgs, fmt.Sprintf("%s: %s", id.String(), info.Message))
		}
	}
	slices.Sort(msgs) // deterministic ordering across runs
	return &FailuresError{Message: fmt.Sprintf("reconcile completed with %d failure(s):\n  %s",
		len(msgs), strings.Join(msgs, "\n  "))}
}

func (o *Orchestrator) fileOf(id manifest.NamedResource) string {
	r, _ := o.index.Lookup(id)
	return r.File
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
