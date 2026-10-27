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

// External sources cannot parent local-tree orphans.
func (o *Orchestrator) reportOwnership(claims []manifest.KSClaim) []manifest.KSClaim {
	local := make([]manifest.KSClaim, 0, len(claims))
	for _, claim := range claims {
		source, _, _ := o.store.GetObject(claim.ID).SourcePath()
		if _, _, external := loader.SourceView(o.store, source, o.repoRoot); !external {
			local = append(local, claim)
		}
	}
	return local
}

// detectOrphans finds failed resources a successful covering parent never emitted.
// Flux never sees these local files; failed-parent cascade victims remain failures.
func (o *Orchestrator) detectOrphans(failed map[manifest.NamedResource]store.StatusInfo, prefixes []manifest.KSClaim) map[manifest.NamedResource]string {
	out := make(map[manifest.NamedResource]string)
	for id, info := range failed {
		if !id.KindInfo().Render {
			continue
		}
		// Any parent's emission proves that Flux saw this resource.
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
		// A failed covering parent emitted nothing. Its children are cascade
		// victims and MUST retain the failure that attributes their root cause.
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

func (o *Orchestrator) classifyCompleted() {
	for id := range o.orphans {
		if info, _ := o.store.GetStatus(id); info.Status != store.StatusReady || info.Message != orphanedMessage {
			delete(o.orphans, id)
		}
	}
	failed := o.store.FailedResources()
	if len(failed) == 0 {
		return
	}
	claims := loader.Claims(o.store, o.repoRoot)
	orphans := o.detectOrphans(failed, o.reportOwnership(claims))
	if len(orphans) != 0 && o.orphans == nil {
		o.orphans = make(map[manifest.NamedResource]string)
	}
	for _, id := range slices.SortedFunc(maps.Keys(orphans), manifest.NamedResource.Compare) {
		o.orphans[id] = orphans[id]
		o.store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: orphanedMessage})
	}
}

func (o *Orchestrator) finalize() error {
	failed := o.store.FailedResources()
	ksCount := len(o.store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
	hrCount := len(o.store.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease))
	for _, id := range slices.SortedFunc(maps.Keys(o.orphans), manifest.NamedResource.Compare) {
		o.store.AddWarning(manifest.Warning{Resource: id, Category: manifest.WarnOrphanedResource, Message: "resource orphaned", Detail: []string{o.fileOf(id), o.orphans[id]}})
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
	return errors.Join(errs...)
}

func (o *Orchestrator) warnMissingCRDs() {
	if !slices.Contains(o.cfg.AllowMissingKinds, manifest.KindCustomResourceDefinition) {
		return
	}
	missing := make(map[manifest.NamedResource]map[manifest.NamedResource]struct{})
	var inputBuf, gatedBuf [16]manifest.NamedResource
	inputs, gated := inputBuf[:0], gatedBuf[:0]
	for _, obj := range o.store.ListObjects("", "") {
		id := obj.Named()
		info, ok := o.store.GetStatus(id)
		if !ok || info.Status != store.StatusReady {
			continue
		}
		_, _, inventory := o.store.GetEmission(id)
		valid := inventory.OutputReads.Valid()
		inventory.Finish()
		if !valid || !o.store.HasInventory(id) {
			continue
		}
		inputs = inputs[:0]
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
			inputs = append(inputs, crd)
		}
		if len(inputs) == 0 {
			continue
		}
		gated = gated[:0]
		for ref := range manifest.References(obj, manifest.ReferenceAll) {
			if ref.Gate != 0 && ref.IsCRD() {
				gated = append(gated, ref.NamedResource)
			}
		}
		for _, crd := range inputs {
			if !slices.Contains(gated, crd) {
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
		o.store.AddWarning(manifest.Warning{
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
