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
		if slices.ContainsFunc(o.store.Productions(id, o.index), func(p manifest.Production) bool {
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
		if status, _ := o.store.GetStatus(parent); status.Status == store.StatusReady {
			out[id] = info.Message
		}
	}
	return out
}

const orphanedMessage = "orphaned (not referenced by any parent kustomization.yaml)"

func (o *Orchestrator) decodeFailures(prefixes []manifest.KSClaim) (map[manifest.NamedResource]error, []error) {
	var unowned []error
	attributed := make(map[manifest.NamedResource]error)
	if o.index == nil {
		return attributed, unowned
	}
	for _, file := range slices.Sorted(maps.Keys(o.index.Errors)) {
		_, owned := manifest.StrictParent(prefixes, file, manifest.NamedResource{}, nil)
		for reader := range o.index.ReadersOfFile(file) {
			owned = true
			if o.filter.ShouldReconcile(reader) && attributed[reader] == nil {
				attributed[reader] = o.index.Errors[file]
			}
		}
		if !owned {
			unowned = append(unowned, o.index.Errors[file])
		}
	}
	return attributed, unowned
}

func (o *Orchestrator) classifyCompleted() {
	var prefixes []manifest.KSClaim
	if o.index != nil && len(o.index.Errors) != 0 {
		prefixes = loader.Claims(o.store, o.repoRoot)
		attributed, _ := o.decodeFailures(prefixes)
		for _, id := range slices.SortedFunc(maps.Keys(attributed), manifest.NamedResource.Compare) {
			if info, _ := o.store.GetStatus(id); info.Status != store.StatusFailed && info.Outcome() != store.OutcomeSuspended &&
				(info.Message != orphanedMessage || o.orphans[id] != attributed[id].Error()) {
				o.store.UpdateFailure(id, attributed[id])
			}
		}
	}
	for id := range o.orphans {
		if info, _ := o.store.GetStatus(id); info.Status != store.StatusReady || info.Message != orphanedMessage {
			delete(o.orphans, id)
		}
	}
	failed := o.store.FailedResources()
	if len(failed) == 0 {
		return
	}
	if prefixes == nil {
		prefixes = loader.Claims(o.store, o.repoRoot)
	}
	orphans := o.detectOrphans(failed, prefixes)
	if len(orphans) != 0 && o.orphans == nil {
		o.orphans = make(map[manifest.NamedResource]string)
	}
	for _, id := range slices.SortedFunc(maps.Keys(orphans), manifest.NamedResource.Compare) {
		o.orphans[id] = orphans[id]
		o.store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: orphanedMessage})
	}
}

func (o *Orchestrator) finalize() error {
	var inputErrs []error
	if o.index != nil && len(o.index.Errors) != 0 {
		_, inputErrs = o.decodeFailures(loader.Claims(o.store, o.repoRoot))
	}
	failed := o.store.FailedResources()
	ksCount := len(o.store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
	hrCount := len(o.store.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease))
	for _, id := range slices.SortedFunc(maps.Keys(o.orphans), manifest.NamedResource.Compare) {
		o.store.AddWarning(id, manifest.Warning{Resource: id, Category: manifest.WarnOrphanedResource, Message: "resource orphaned", Detail: []string{o.fileOf(id), o.orphans[id]}})
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
		for crd := range o.store.RequiredInputs(id) {
			if !crd.IsCRD() {
				continue
			}
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
	slog.Debug("orchestrator: reconcile complete",
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
	failed = sanitizeFailed(failed)
	for _, id := range slices.SortedFunc(maps.Keys(failed), manifest.NamedResource.Compare) {
		info := failed[id]
		if f := o.fileOf(id); f != "" {
			slog.Debug("orchestrator: resource failed", "id", id.String(), "reason", info.String(), "file", f)
		} else {
			slog.Debug("orchestrator: resource failed", "id", id.String(), "reason", info.String())
		}
	}
}

// FailuresError aggregates per-resource reconcile failure messages.
type FailuresError struct{ Message string }

func (e *FailuresError) Error() string { return e.Message }

// Unwrap returns the shared domain identity for resource failures.
func (*FailuresError) Unwrap() error { return manifest.ErrFlux }

func (o *Orchestrator) aggregateFailures(failed map[manifest.NamedResource]store.StatusInfo) error {
	msgs := make([]string, 0, len(failed))
	for id, info := range sanitizeFailed(failed) {
		if f := o.fileOf(id); f != "" {
			msgs = append(msgs, fmt.Sprintf("%s (%s): %s", id.String(), f, info.String()))
		} else {
			msgs = append(msgs, fmt.Sprintf("%s: %s", id.String(), info.String()))
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
		info.Message = manifest.TrimSentinelPrefix(info.String())
		out[id] = info
	}
	return out
}
