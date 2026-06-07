package helmrelease

// valuesfrom.go contains the valuesFrom omission helpers: functions that
// inspect a HelmRelease's valuesFrom list and strip refs that cannot be
// resolved offline (generated secrets, external-secret targets, etc.).
// Extracted from controller.go to keep domain helpers in named files,
// mirroring the kustomization package's dispatch.go / substitute.go split.

import (
	"context"
	"log/slog"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// resolvePreRenderValuesFrom reduces hr's valuesFrom to the set helm.Prepare
// can resolve offline, isolating the one non-linear stretch of reconcile (a
// retry loop + a load-bearing store re-read) so reconcile itself reads as a
// linear gate/resolve/render sequence.
//
// Two phases. (1) When allowMissingSecrets, proactively omit refs whose
// producer can't materialize offline so they don't become pre-render waits.
// (2) Wait for the remaining pre-render references — helm.Prepare reads the
// live Store synchronously and hard-fails on a missing chartRef HelmChart or
// non-optional valuesFrom CM/Secret, so a legitimate load order (HR observed
// before its HelmChart CR, or before a sibling KS emits its valuesFrom CM)
// must wait rather than fail. On a wait failure under allowMissingSecrets the
// failed refs are dropped and the wait retried; otherwise the reconcile fails.
//
// On success the canonical HR is re-read from the store (a structural parent
// may have re-emitted it with the full valuesFrom list while we waited), then
// the accumulated failed-ref drops and the proactive omission are re-applied —
// the re-applied proactive pass re-evaluates against the now-current store, so
// a ref that materialized during the wait is correctly kept.
func (c *Controller) resolvePreRenderValuesFrom(ctx context.Context, id manifest.NamedResource, hr *manifest.HelmRelease) (*manifest.HelmRelease, error) {
	if c.allowMissingSecrets {
		hr = c.omitValuesFrom(hr, nil, true)
	}
	omittedValuesRefs := map[manifest.NamedResource]struct{}{}
	for {
		preDeps := preparePrereqs(hr)
		if len(preDeps) == 0 {
			break
		}
		var preSum depwait.Summary
		if err := c.Await(ctx, id, c.NewWaiter(id, hr.Timeout), preDeps,
			"awaiting pre-render references",
			func(sum depwait.Summary) error {
				preSum = sum
				return base.DepFailed(id)(sum)
			}); err != nil {
			if c.allowMissingSecrets {
				if next, ok := c.omitFailedValuesFrom(hr, preSum.Failed); ok {
					for _, omitted := range omittedValuesRefIDs(hr, next) {
						omittedValuesRefs[omitted] = struct{}{}
					}
					hr = next
					continue
				}
			}
			return nil, err
		}
		if obj, ok := store.Get[*manifest.HelmRelease](c.Store, id); ok {
			hr = obj
		}
		if len(omittedValuesRefs) > 0 {
			hr = removeValuesRefs(hr, omittedValuesRefs)
		}
		if c.allowMissingSecrets {
			hr = c.omitValuesFrom(hr, nil, true)
		}
		break
	}
	return hr, nil
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
	for _, ref := range hr.ValuesFrom {
		id, ok := valuesRefID(hr, ref)
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
		slog.Debug("helmrelease: omitted unavailable valuesFrom ref", args...)
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
	if c.valuesRefExists(id) || c.IsFileIndexed(id) {
		return manifest.NamedResource{}, false, false
	}
	producer, hasProducer = c.generatedValuesProducer(id)
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
	return c.Store.GetByName(id.Kind, id.Namespace, id.Name) != nil
}

// generatedValuesProducer reports the producer that would generate the
// Secret identified by id, if one is indexed. Coverage is limited to the
// kinds rawProducerTargetID recognises (ExternalSecret, SealedSecret);
// any other kind returns (zero, false) and is treated as non-generated —
// see the rawProducerIndex field comment for why that is safe-but-degraded.
func (c *Controller) generatedValuesProducer(id manifest.NamedResource) (manifest.NamedResource, bool) {
	if v, ok := c.rawProducerIndex.Load(id); ok {
		return v.(manifest.NamedResource), true
	}
	return manifest.NamedResource{}, false
}

func valuesRefID(hr *manifest.HelmRelease, ref manifest.ValuesReference) (manifest.NamedResource, bool) {
	if ref.Optional || ref.Name == "" {
		return manifest.NamedResource{}, false
	}
	switch ref.Kind {
	case manifest.KindSecret, manifest.KindConfigMap:
		return manifest.NamedResource{Kind: ref.Kind, Namespace: hr.Namespace, Name: ref.Name}, true
	default:
		return manifest.NamedResource{}, false
	}
}

func omittedValuesRefIDs(before, after *manifest.HelmRelease) []manifest.NamedResource {
	if before == nil || after == nil {
		return nil
	}
	kept := make(map[manifest.NamedResource]struct{}, len(after.ValuesFrom))
	for _, ref := range after.ValuesFrom {
		if id, ok := valuesRefID(after, ref); ok {
			kept[id] = struct{}{}
		}
	}
	var out []manifest.NamedResource
	for _, ref := range before.ValuesFrom {
		id, ok := valuesRefID(before, ref)
		if !ok {
			continue
		}
		if _, ok := kept[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func removeValuesRefs(hr *manifest.HelmRelease, ids map[manifest.NamedResource]struct{}) *manifest.HelmRelease {
	if hr == nil || len(ids) == 0 || len(hr.ValuesFrom) == 0 {
		return hr
	}
	filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
	for _, ref := range hr.ValuesFrom {
		if id, ok := valuesRefID(hr, ref); ok {
			if _, drop := ids[id]; drop {
				continue
			}
		}
		filtered = append(filtered, ref)
	}
	return cloneWithValuesFrom(hr, filtered)
}
