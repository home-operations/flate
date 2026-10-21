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
		_, _, external := loader.SourceView(o.store, ks.SourceID(), o.repoRoot)
		return external
	})
	for id, info := range failed {
		if !id.KindInfo().Render {
			continue
		}
		// A resource that any parent's render also emitted is by
		// definition not orphaned — kustomize-controller saw it.
		if slices.ContainsFunc(slices.Collect(o.store.Productions(id, o.index)), func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Authored == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{})
		}) {
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
	var prefixes []manifest.KSClaim
	var inputErrs []error
	if o.index != nil && len(o.index.Errors) != 0 {
		prefixes = loader.Claims(o.store, o.repoRoot)
		for _, file := range slices.Sorted(maps.Keys(o.index.Errors)) {
			_, owned := manifest.StrictParent(prefixes, file, manifest.NamedResource{}, nil)
			for reader := range o.index.ReadersOfFile(file) {
				owned = true
				if !o.filter.ShouldReconcile(reader) {
					continue
				}
				if info, _ := o.store.GetStatus(reader); info.Status != store.StatusFailed && !store.IsSuspended(info) {
					o.store.UpdateFailure(reader, o.index.Errors[file])
				}
			}
			if !owned {
				inputErrs = append(inputErrs, o.index.Errors[file])
			}
		}
	}
	failed := o.store.FailedResources()
	ksCount := len(o.store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
	hrCount := len(o.store.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease))
	// Final ownership includes every Kustomization present after the drain.
	if len(failed) != 0 && prefixes == nil {
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
	return errors.Join(append(errs, inputErrs...)...)
}

// warnMissingCRDs uses only final canonical references and Ready/artifact
// eligibility; retained output does not establish which revision passed a gate.
func (o *Orchestrator) warnMissingCRDs() {
	if !o.cfg.AllowMissingCRDs {
		return
	}
	missing := make(map[manifest.NamedResource]map[manifest.NamedResource]struct{})
	for _, obj := range o.store.ListObjects(manifest.FluxOperatorDomain, manifest.KindResourceSet) {
		rs, ok := obj.(*manifest.ResourceSet)
		if !ok {
			continue
		}
		id := rs.Named()
		info, ok := o.store.GetStatus(id)
		if !ok || info.Status != store.StatusReady {
			continue
		}
		if _, ok := o.store.GetArtifact(id).(*store.Emission); !ok {
			continue
		}
		for dep := range manifest.References(rs) {
			if dep.Use != "dependsOn" || !dep.IsCRD() {
				continue
			}
			crd := dep.NamedResource
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
		o.store.AddWarning(crd, manifest.Warning{
			Resource: crd,
			Category: manifest.WarnMissingCRD,
			Message:  fmt.Sprintf("CRD %s absent from offline inputs; accepted for %s", crd.String(), strings.Join(names, ", ")),
		})
	}
}

func (o *Orchestrator) logSummary(failed map[manifest.NamedResource]store.StatusInfo, ksCount, hrCount int) {
	// Counts describe the final supported inventory.
	slog.Debug("reconcile complete",
		"kustomizations", ksCount,
		"helm_releases", hrCount,
		"failed", len(failed))
	// Surface a clear advisory when the scan turned up nothing — covers
	// the "typo'd --path that happens to be an empty directory" case
	// where flate would otherwise look like a silent success.
	if ksCount == 0 && hrCount == 0 {
		o.store.AddWarning(manifest.NamedResource{}, manifest.Warning{
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
