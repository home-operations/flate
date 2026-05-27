package helmrelease

// valuesfrom.go contains the valuesFrom omission helpers: functions that
// inspect a HelmRelease's valuesFrom list and strip refs that cannot be
// resolved offline (generated secrets, external-secret targets, etc.).
// Extracted from controller.go to keep domain helpers in named files,
// mirroring the kustomization package's dispatch.go / substitute.go split.

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
)

func (c *Controller) omitGeneratedValuesFrom(hr *manifest.HelmRelease) *manifest.HelmRelease {
	return c.omitValuesFrom(hr, nil, true)
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
	omitted := false
	for _, ref := range hr.ValuesFrom {
		id, ok := valuesRefID(hr, ref)
		if !ok {
			filtered = append(filtered, ref)
			continue
		}
		if failed != nil {
			if _, wasFailed := failed[id]; !wasFailed {
				filtered = append(filtered, ref)
				continue
			}
		}
		if c.valuesRefExists(id) {
			filtered = append(filtered, ref)
			continue
		}
		if c.IsFileIndexed(id) {
			filtered = append(filtered, ref)
			continue
		}
		producer, hasProducer := c.generatedValuesProducer(id)
		if requireProducer && !hasProducer {
			filtered = append(filtered, ref)
			continue
		}
		omitted = true
		args := []any{"id", hr.Named().String(), "ref", id.String()}
		if hasProducer {
			args = append(args, "producer", producer.String())
		}
		slog.Debug("helmrelease: omitted unavailable valuesFrom ref", args...)
	}
	if !omitted {
		return hr
	}
	out := hr.Clone()
	out.ValuesFrom = filtered
	return out
}

func (c *Controller) valuesRefExists(id manifest.NamedResource) bool {
	return c.Store.GetByName(id.Kind, id.Namespace, id.Name) != nil
}

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
	omitted := false
	for _, ref := range hr.ValuesFrom {
		id, ok := valuesRefID(hr, ref)
		if ok {
			if _, drop := ids[id]; drop {
				omitted = true
				continue
			}
		}
		filtered = append(filtered, ref)
	}
	if !omitted {
		return hr
	}
	out := hr.Clone()
	out.ValuesFrom = filtered
	return out
}
