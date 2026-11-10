package diff

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestMergeWarnings_Presentation(t *testing.T) {
	t.Parallel()
	duplicate := manifest.Warning{Category: manifest.WarnDuplicateDocument, Message: "shadowed"}
	suppressed := []Suppression{
		{
			Parent: Parent{NamedResource: testutil.HelmReleaseID("apps", "demo")},
			Side:   SideCurrent,
			Count:  2,
			Reason: "missing source",
		},
	}
	want := manifest.Warning{
		Resource: suppressed[0].Parent.NamedResource, Category: manifest.WarnDiffSuppressed,
		Message: "2 objects not rendered on the current side; diff suppressed (missing source)",
	}
	assert.Diff(t, MergeWarnings(nil, suppressed), []manifest.Warning{want})
	assert.Diff(
		t,
		MergeWarnings([]manifest.Warning{duplicate}, suppressed),
		[]manifest.Warning{want, duplicate},
	)
	assert.Diff(t, MergeWarnings([]manifest.Warning{duplicate}, nil), []manifest.Warning{duplicate})
}

func TestMergeWarnings_PreservesInputsAndPayload(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("apps", "render"), Path: "apps"}
	captured := manifest.DiagnosticIdentity{
		ID: parent.NamedResource,
		Resource: manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      "FluxInstance",
			Namespace: "flux-system",
			Name:      "cluster",
		},
		Path: "cluster",
	}
	backing := []manifest.Warning{
		{Category: manifest.WarnInputReadFailed, Message: "read failed", Count: 3},
		{
			Resource: parent.NamedResource,
			Subject:  captured,
			Related: []manifest.NamedResource{
				testutil.HelmReleaseID("apps", "second"),
				testutil.HelmReleaseID("apps", "first"),
			},
			Category: manifest.WarnDuplicateDocument,
			Message:  "shadowed",
			Detail:   []string{"second detail", "first detail"},
			Count:    7,
		},
		{Category: "sentinel", Message: "spare warning slot"},
		{Category: "sentinel", Message: "another spare warning slot"},
	}
	before := slices.Clone(backing)
	for i := range before {
		before[i].Related = slices.Clone(backing[i].Related)
		before[i].Detail = slices.Clone(backing[i].Detail)
	}
	note := Suppression{Parent: parent, Subject: captured, Side: SideOrig, Count: 2, Reason: "missing source"}
	notes := []Suppression{note, note, {Reason: "spare suppression slot"}}
	beforeNotes := slices.Clone(notes)
	disclosure := manifest.Warning{
		Resource: parent.NamedResource, Subject: captured, Category: manifest.WarnDiffSuppressed,
		Message: "2 objects not rendered on the orig side; diff suppressed (missing source)",
	}
	got := MergeWarnings(backing[:2], notes[:2])
	assert.Diff(t, got, []manifest.Warning{disclosure, disclosure, before[1], before[0]})
	assert.Diff(t, backing, before)
	assert.Diff(t, notes, beforeNotes)
	assert.Diff(t, MergeWarnings(backing[:2], nil), []manifest.Warning{before[1], before[0]})
	assert.Diff(t, backing, before)
}
