package diff

import "reflect"

// ChangeStatus classifies how a resource differs between two doc sets.
type ChangeStatus string

const (
	// StatusAdded — present only on the right (New) side.
	StatusAdded ChangeStatus = "added"
	// StatusChanged — present on both sides with differing content.
	StatusChanged ChangeStatus = "changed"
	// StatusRemoved — present only on the left (Old) side.
	StatusRemoved ChangeStatus = "removed"
)

// Change is one resource that differs between two rendered doc sets —
// the structured form of what RenderDocs formats. SDK consumers build
// their own output (an API payload, a web UI, an image report) from a
// []Change instead of re-implementing the pairing + normalization that
// RenderDocs does internally.
type Change struct {
	// Parent is the Flux Kustomization / HelmRelease that rendered this
	// resource — the pairing discriminator (so a Deployment from HR A is
	// never matched against the same-named Deployment from HR B).
	Parent                Parent
	Kind, Namespace, Name string
	Status                ChangeStatus
	// Old / New are the NORMALIZED manifests (Options strip + binaryData
	// redaction + the optional Normalize hook applied). Old is nil for an
	// add; New is nil for a remove.
	Old, New map[string]any
	// OldChart / NewChart are the resource's "helm.sh/chart" label
	// ("<name>-<version>") captured BEFORE normalization stripped it —
	// "" when absent. Surfaces a chart-version bump the stripped Old/New
	// no longer carry.
	OldChart, NewChart string
}

// Changes pairs left against right by (parent, apiVersion, kind,
// namespace, name), drops byte-identical resources, and returns the
// remaining differences as structured data. Each side is normalized
// (opts.StripAttrs / opts.StripFields, ConfigMap binaryData redaction,
// then the optional opts.Normalize hook) before the equality check, so
// render-time noise — chart-bump annotations, volatile spec fields, a
// consumer's secret/cert redaction — never reads as a spurious change.
// opts.Format is ignored (Changes returns data, not formatted bytes).
//
// This is the data RenderDocs renders: exposed so SDK consumers stop
// re-implementing the pairing. The result is sorted by parent then
// resource identity for deterministic output.
func Changes(left, right []Doc, opts Options) []Change {
	// Pair the RAW docs so OldChart/NewChart can read helm.sh/chart
	// before the strip removes it; normalize each side per-resource
	// afterward. The pair key (apiVersion/kind/ns/name + parent) is
	// untouched by normalization, so raw and normalized pair identically.
	paired := pair(left, right)
	out := make([]Change, 0, len(paired))
	for _, p := range paired {
		old := normalizeManifest(p.a, opts.StripAttrs, opts.StripFields, opts.Normalize)
		nw := normalizeManifest(p.b, opts.StripAttrs, opts.StripFields, opts.Normalize)
		status, differs := classify(old, nw)
		if !differs {
			continue
		}
		out = append(out, Change{
			Parent:    p.parent,
			Kind:      p.kind,
			Namespace: p.namespace,
			Name:      p.name,
			Status:    status,
			Old:       old,
			New:       nw,
			OldChart:  chartLabel(p.a),
			NewChart:  chartLabel(p.b),
		})
	}
	return out
}

// classify reports the change status of a paired resource and whether it
// differs at all. old/new are the normalized sides (nil when the
// resource is absent on that side). A both-present pair that is
// DeepEqual after normalization is unchanged (differs=false) and dropped.
func classify(old, nw map[string]any) (status ChangeStatus, differs bool) {
	switch {
	case old == nil:
		return StatusAdded, true
	case nw == nil:
		return StatusRemoved, true
	case reflect.DeepEqual(old, nw):
		return "", false
	default:
		return StatusChanged, true
	}
}

// chartLabel returns a manifest's "helm.sh/chart" label
// ("<name>-<version>"), or "" when the manifest is nil or carries no such
// label. Read before normalization strips it.
func chartLabel(m map[string]any) string {
	if m == nil {
		return ""
	}
	meta, ok := m["metadata"].(map[string]any)
	if !ok {
		return ""
	}
	labels, ok := meta["labels"].(map[string]any)
	if !ok {
		return ""
	}
	s, _ := labels["helm.sh/chart"].(string)
	return s
}
