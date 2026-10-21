package helmrelease

// valuesfrom.go contains the valuesFrom omission helpers: functions that
// inspect a HelmRelease's valuesFrom list and strip refs that cannot be
// resolved offline (generated secrets, external-secret targets, etc.).
// Extracted from controller.go to keep domain helpers in named files,
// mirroring the kustomization package's dispatch.go / substitute.go split.

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// resolvePreRenderValuesFrom reduces hr's valuesFrom to the set helm.Prepare
// can resolve offline, isolating the one non-linear stretch of reconcile (a
// retry loop + a load-bearing store re-read) so reconcile itself reads as a
// linear gate/resolve/render sequence.
//
// Two phases. (1) Proactively omit refs that have a declared in-repo producer
// (an ExternalSecret/SealedSecret targeting the Secret): their value only
// materializes live, so flate has positive evidence to skip them rather than
// wait — this runs UNCONDITIONALLY, the producer is the signal, no flag
// required. A ref with no known producer is kept. (2) Wait for the remaining
// pre-render references — helm.Prepare reads the live Store synchronously and
// hard-fails on a missing chartRef HelmChart or non-optional valuesFrom
// CM/Secret, so a legitimate load order (HR observed before its HelmChart CR,
// or before a sibling KS emits its valuesFrom CM) must wait rather than fail.
// On a wait failure, ONLY under allowMissingSecrets, the failed refs are
// dropped (the blanket "skip anything that won't materialize" behavior) and
// the wait retried; otherwise the reconcile fails.
//
// On success the canonical HR is re-read from the store (a structural parent
// may have re-emitted it with the full valuesFrom list while we waited), then
// the accumulated failed-ref drops and the proactive omission are re-applied —
// the re-applied proactive pass re-evaluates against the now-current store, so
// a ref that materialized during the wait is correctly kept.
func (c *Controller) resolvePreRenderValuesFrom(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) (*manifest.HelmRelease, []manifest.NamedResource, error) {
	id := hr.Named()
	unfiltered := hr
	// Producer-backed omission runs regardless of the flag: a ref with a
	// declared producer is dropped, a ref without one is kept (and waited
	// on below). requireProducer=true is what enforces that asymmetry.
	hr = c.omitValuesFrom(hr, nil, true)
	omittedValuesRefs := map[manifest.NamedResource]struct{}{}
	for {
		preDeps := preparePrereqs(hr)
		if len(preDeps) == 0 {
			break
		}
		c.Store.UpdateStatus(id, store.StatusPending, "", "awaiting pre-render references")
		fresh, found, err := c.RequireRefresh[*manifest.HelmRelease](ctx, id, preDeps, level)
		if err != nil {
			if failed, ok := errors.AsType[*manifest.DependencyFailedError](err); ok && c.allowMissingSecrets {
				if next, ok := c.omitFailedValuesFrom(hr, failed.Failed); ok {
					for _, omitted := range omittedValuesRefIDs(hr, next) {
						omittedValuesRefs[omitted] = struct{}{}
					}
					hr = next
					continue
				}
			}
			return nil, nil, err
		}
		if found {
			hr = fresh
			unfiltered = fresh
		}
		if len(omittedValuesRefs) > 0 {
			hr = removeValuesRefs(hr, omittedValuesRefs)
		}
		// Re-apply producer-backed omission against the now-current store: a
		// ref that materialized during the wait is kept (valuesRefExists
		// wins), one still backed only by a producer is dropped.
		hr = c.omitValuesFrom(hr, nil, true)
		break
	}
	return hr, omittedValuesRefIDs(unfiltered, hr), nil
}

func (c *Controller) recordOmittedValues(id manifest.NamedResource, refs []manifest.NamedResource) {
	for _, ref := range refs {
		c.Store.AddWarning(id, manifest.Warning{
			Resource: id,
			Category: manifest.WarnOmittedValues,
			Message:  fmt.Sprintf("required valuesFrom %s omitted from offline render", ref),
		})
	}
}

func (c *Controller) omitFailedValuesFrom(hr *manifest.HelmRelease, failed []manifest.NamedResource) (*manifest.HelmRelease, bool) {
	failedSet := make(map[manifest.NamedResource]struct{}, len(failed))
	for _, id := range failed {
		failedSet[id] = struct{}{}
	}
	next := c.omitValuesFrom(hr, failedSet, false)
	return next, next != hr
}

func (c *Controller) omitValuesFrom(
	hr *manifest.HelmRelease,
	failed map[manifest.NamedResource]struct{},
	requireProducer bool,
) *manifest.HelmRelease {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return hr
	}
	filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
	for edge := range manifest.References(hr) {
		if edge.Use != "valuesFrom" {
			continue
		}
		ref := hr.ValuesFrom[edge.Position]
		id, ok := valuesRefID(edge)
		if !ok {
			filtered = append(filtered, ref)
			continue
		}
		producer, hasProducer, omit := c.shouldOmitValuesRef(id, failed, requireProducer)
		if !omit {
			filtered = append(filtered, ref)
			continue
		}
		args := []any{"id", hr.Named().String(), "ref", id.String()}
		if hasProducer {
			args = append(args, "producer", producer.String())
		}
		c.Logger().Debug("omitted unavailable valuesFrom ref", args...)
	}
	return cloneWithValuesFrom(hr, filtered)
}

// shouldOmitValuesRef decides whether the valuesFrom ref identified by id
// should be dropped from the offline render. It also surfaces the indexed
// producer (if any) for logging. A ref is kept (omit=false) when it is not in
// the failed set, when it exists in the store, when it is file-indexed, or —
// under requireProducer — when no generating producer is known for it.
func (c *Controller) shouldOmitValuesRef(
	id manifest.NamedResource,
	failed map[manifest.NamedResource]struct{},
	requireProducer bool,
) (producer manifest.NamedResource, hasProducer, omit bool) {
	if failed != nil {
		if _, wasFailed := failed[id]; !wasFailed {
			return manifest.NamedResource{}, false, false
		}
	}
	if c.valuesRefExists(id) {
		return manifest.NamedResource{}, false, false
	}
	for _, p := range c.Store.Productions(id, nil) {
		if p.Declaration == (manifest.NamedResource{}) && (p.Owner != (manifest.NamedResource{}) || p.Path != "") {
			return manifest.NamedResource{}, false, false
		}
		if p.Declaration != (manifest.NamedResource{}) {
			producer, hasProducer = p.Declaration, true
		}
	}
	if requireProducer && !hasProducer {
		return producer, hasProducer, false
	}
	return producer, hasProducer, true
}

// cloneWithValuesFrom returns hr unchanged when filtered keeps every ref
// (the callers only ever drop, never add or reorder, so an equal length
// means nothing was omitted), otherwise a clone carrying filtered. Keeping
// the copy-on-write in one place lets the filtering loops stay declarative.
func cloneWithValuesFrom(hr *manifest.HelmRelease, filtered []manifest.ValuesReference) *manifest.HelmRelease {
	if len(filtered) == len(hr.ValuesFrom) {
		return hr
	}
	out := hr.Clone()
	out.ValuesFrom = filtered
	return out
}

func (c *Controller) valuesRefExists(id manifest.NamedResource) bool {
	return c.Store.GetObject(id) != nil
}

func valuesRefID(ref manifest.Reference) (manifest.NamedResource, bool) {
	if ref.Optional || ref.Name == "" {
		return manifest.NamedResource{}, false
	}
	switch ref.Kind {
	case manifest.KindSecret, manifest.KindConfigMap:
		return ref.NamedResource, true
	default:
		return manifest.NamedResource{}, false
	}
}

func omittedValuesRefIDs(before, after *manifest.HelmRelease) []manifest.NamedResource {
	if before == nil || after == nil || len(before.ValuesFrom) == len(after.ValuesFrom) {
		return nil
	}
	kept := make(map[manifest.NamedResource]struct{}, len(after.ValuesFrom))
	for ref := range manifest.References(after) {
		if ref.Use != "valuesFrom" {
			continue
		}
		if id, ok := valuesRefID(ref); ok {
			kept[id] = struct{}{}
		}
	}
	var out []manifest.NamedResource
	for ref := range manifest.References(before) {
		if ref.Use != "valuesFrom" {
			continue
		}
		id, ok := valuesRefID(ref)
		if !ok {
			continue
		}
		if _, ok := kept[id]; !ok {
			out = append(out, id)
		}
	}
	slices.SortFunc(out, manifest.NamedResource.Compare)
	return slices.Compact(out)
}

func removeValuesRefs(hr *manifest.HelmRelease, ids map[manifest.NamedResource]struct{}) *manifest.HelmRelease {
	if hr == nil || len(ids) == 0 || len(hr.ValuesFrom) == 0 {
		return hr
	}
	filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
	for edge := range manifest.References(hr) {
		if edge.Use != "valuesFrom" {
			continue
		}
		ref := hr.ValuesFrom[edge.Position]
		if id, ok := valuesRefID(edge); ok {
			if _, drop := ids[id]; drop {
				continue
			}
		}
		filtered = append(filtered, ref)
	}
	return cloneWithValuesFrom(hr, filtered)
}
