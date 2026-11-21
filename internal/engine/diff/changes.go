package diff

import (
	"bytes"

	"github.com/home-operations/flate/pkg/manifest"
)

// ChangeStatus classifies how a resource differs between two doc sets.
type ChangeStatus string

// StatusRemoved identifies a resource present only in the original snapshot.
const StatusRemoved ChangeStatus = "removed"

// Change describes one resource comparison.
type Change struct {
	Parent         Parent          `json:"parent"`
	ProducerChange *producerChange `json:"producerChange,omitempty"`
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

type producerChange struct {
	From *Parent `json:"from"`
	To   *Parent `json:"to"`
}

func producerEndpoint(endpoint *manifest.DiagnosticIdentity) *Parent {
	if endpoint.ID == (manifest.NamedResource{}) {
		return nil
	}
	return &Parent{NamedResource: endpoint.Resource, Path: endpoint.Path}
}

func renderJSON(
	changes []Change,
	suppressed []Suppression,
	subjects [2]map[manifest.NamedResource]manifest.DiagnosticIdentity,
) ([]byte, error) {
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
	err := (encoder{Writer: &buf}).json(struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}{changes, notes})
	return buf.Bytes(), err
}

func publicChanges(prepared prepared) []Change {
	out := make([]Change, 0, len(prepared.Pairs))
	for c, status := range prepared.changes() {
		var move *producerChange
		if c.ProducerChange != nil {
			move = &producerChange{
				From: producerEndpoint(&c.ProducerChange.From),
				To:   producerEndpoint(&c.ProducerChange.To),
			}
		}
		out = append(out, Change{
			Parent: c.Parent, ProducerChange: move, APIVersion: c.APIVersion,
			Kind: c.Resource.Kind, Namespace: c.Resource.Namespace, Name: c.Resource.Name,
			Status: ChangeStatus(status), Old: c.Old, New: c.New,
			OldChart: c.OldChart, NewChart: c.NewChart,
		})
	}
	return out
}
