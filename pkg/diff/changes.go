package diff

import (
	"bytes"
	"reflect"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
)

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
	Parent Parent `json:"parent"`
	// APIVersion is the authored current version, or the original version for removals.
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Namespace  string       `json:"namespace"`
	Name       string       `json:"name"`
	Status     ChangeStatus `json:"status"`
	// Old / New are the NORMALIZED manifests (Options strip + binaryData
	// redaction + the optional Normalize hook applied). Old is nil for an
	// add; New is nil for a remove.
	Old map[string]any `json:"old"`
	New map[string]any `json:"new"`
	// OldChart / NewChart are the resource's "helm.sh/chart" label
	// ("<name>-<version>") captured BEFORE normalization stripped it —
	// "" when absent. Surfaces a chart-version bump the stripped Old/New
	// no longer carry.
	OldChart string `json:"oldChart"`
	NewChart string `json:"newChart"`
}

// Changes normalizes left and right before pairing by canonical parent and
// resource identity, and returns the differing resources as structured data.
// Normalization strips opts.StripAttrs and opts.StripFields, redacts ConfigMap
// binaryData, then applies opts.Normalize. Original chart labels are retained
// as provenance. The result is sorted by parent then resource identity.
// Returned manifests are independent of the input documents.
// Warnings identify shadowed documents.
// opts.Format is ignored.
func Changes(left, right []Doc, opts Options) ([]Change, []manifest.Warning) {
	paired, warnings, leftCopied, rightCopied := pair(left, right, opts)
	return changes(paired, leftCopied, rightCopied), warnings
}

func changes(paired []pairedResource, leftCopied, rightCopied bool) []Change {
	out := make([]Change, 0, len(paired))
	for _, p := range paired {
		status, differs := classify(p.a, p.b)
		if !differs {
			continue
		}
		if !leftCopied {
			p.a = manifest.DeepCopyMap(p.a)
		}
		if !rightCopied {
			p.b = manifest.DeepCopyMap(p.b)
		}
		out = append(out, Change{
			Parent:     p.parent,
			APIVersion: p.apiVersion,
			Kind:       p.resource.Kind,
			Namespace:  p.resource.Namespace,
			Name:       p.resource.Name,
			Status:     status,
			Old:        p.a,
			New:        p.b,
			OldChart:   p.aChart,
			NewChart:   p.bChart,
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
	s, _ := labels[helmChartLabel].(string)
	return s
}

func renderJSON(changes []Change, suppressed []Suppression) ([]byte, error) {
	var buf bytes.Buffer
	err := (output.Encoder{Writer: &buf}).JSON(struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}{changes, append([]Suppression{}, suppressed...)})
	return buf.Bytes(), err
}
