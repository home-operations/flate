package diff

import (
	"bytes"

	"github.com/home-operations/flate/internal/diffprep"
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
	prepared := diffprep.Prepare(prepareDocs(left), prepareDocs(right), prepareOptions(opts))
	return publicChanges(prepared), prepared.Warnings
}

func renderJSON(changes []Change, suppressed []Suppression, subjects [2]map[manifest.NamedResource]manifest.DiagnosticIdentity) ([]byte, error) {
	for i := range changes {
		selected := subjects[1]
		if changes[i].Status == StatusRemoved {
			selected = subjects[0]
		}
		if subject, ok := selected[changes[i].Parent.NamedResource]; ok {
			changes[i].Parent = Parent{NamedResource: subject.Resource, Path: subject.Path}
		}
	}
	notes := append([]Suppression{}, suppressed...)
	for i := range notes {
		if subject := notes[i].Subject; subject.ID != (manifest.NamedResource{}) {
			notes[i].Parent.NamedResource = subject.Resource
			notes[i].Parent.Path = subject.Path
		}
	}
	var buf bytes.Buffer
	err := (output.Encoder{Writer: &buf}).JSON(struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}{changes, notes})
	return buf.Bytes(), err
}

func publicChanges(prepared diffprep.Prepared) []Change {
	out := make([]Change, 0, len(prepared.Pairs))
	for c, status := range prepared.Changes() {
		out = append(out, Change{
			Parent: Parent(c.Parent), APIVersion: c.APIVersion,
			Kind: c.Resource.Kind, Namespace: c.Resource.Namespace, Name: c.Resource.Name,
			Status: ChangeStatus(status), Old: c.Old, New: c.New,
			OldChart: c.OldChart, NewChart: c.NewChart,
		})
	}
	return out
}
