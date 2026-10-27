package diffprep

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func cmDoc(parent Parent, id manifest.NamedResource, dataVal, chart string) Doc {
	meta := map[string]any{"name": id.Name}
	if id.Namespace != "" {
		meta["namespace"] = id.Namespace
	}
	if chart != "" {
		meta["labels"] = map[string]any{"helm.sh/chart": chart}
	}
	return Doc{
		Parent: parent,
		Manifest: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": meta,
			"data":     map[string]any{"k": dataVal},
		},
	}
}

func TestPair_DuplicateDocumentsWarnAndLastWins(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "./apps"}
	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) {
			t.Parallel()

			first := cmDoc(parent, manifest.NamedResource{Name: "first", Namespace: "ns"}, "shadowed", "first-1")
			last := cmDoc(parent, manifest.NamedResource{Name: "last", Namespace: "ns"}, "retained", "last-2")
			last.Parent.Path = "new/apps"
			last.Manifest["apiVersion"] = "v2"
			other := cmDoc(parent, manifest.NamedResource{Name: "other", Namespace: "ns"}, "other-side", "other-3")
			foreign := cmDoc(parent, manifest.NamedResource{Name: "foreign", Namespace: "ns"}, "separate-group", "")
			foreign.Manifest["apiVersion"] = "example.test/v1"
			left, right := []Doc{first, last, foreign}, []Doc{other}
			if side == "right" {
				left, right = right, left
			}
			prepared := Prepare(left, right, Options{StripAttrs: []string{"helm.sh/chart", "checksum/", "app.kubernetes.io/version", "chart"}, Normalize: func(m map[string]any) {
				meta := m["metadata"].(map[string]any)
				meta["name"], meta["namespace"] = "normalized", "normalized-ns"
				apiVersion := "v3"
				if group := manifest.DocIdentity(m).Group; group != "" {
					apiVersion = group + "/" + apiVersion
				}
				m["apiVersion"] = apiVersion
			}})
			pairs, warnings := prepared.Pairs, prepared.Warnings
			if len(pairs) != 2 {
				t.Fatalf("expected two paired resources, got %d", len(pairs))
			}
			assert.Equal(t, pairs[1].Resource, manifest.NamedResource{Group: "example.test", Kind: "ConfigMap", Namespace: "normalized-ns", Name: "normalized"})
			foreignDoc, absent := pairs[1].Old, pairs[1].New
			if side == "right" {
				foreignDoc, absent = absent, foreignDoc
			}
			if absent != nil || foreignDoc["data"].(map[string]any)["k"] != "separate-group" {
				t.Fatal("cross-group duplicate changed pairing")
			}
			retained, counterpart := pairs[0].Old, pairs[0].New
			if side == "right" {
				retained, counterpart = counterpart, retained
			}
			if retained["data"].(map[string]any)["k"] != "retained" || counterpart["data"].(map[string]any)["k"] != "other-side" {
				t.Fatalf("expected last document paired with other side, got %+v", pairs[0])
			}
			retainedChart, counterpartChart := pairs[0].OldChart, pairs[0].NewChart
			wantParent := other.Parent
			if side == "right" {
				retainedChart, counterpartChart = counterpartChart, retainedChart
				wantParent = last.Parent
			}
			assert.Equal(t, retainedChart, "last-2")
			assert.Equal(t, counterpartChart, "other-3")
			assert.Equal(t, pairs[0].Parent, wantParent)
			assert.Equal(t, pairs[0].APIVersion, "v3")
			assert.Equal(t, pairs[0].Resource.Name, "normalized")
			assert.Equal(t, chartLabel(retained), "")
			assert.Equal(t, chartLabel(counterpart), "")
			warningSide := SideOrig
			if side == "right" {
				warningSide = SideCurrent
			}
			assert.Diff(t, warnings, []manifest.Warning{{
				Resource: manifest.NamedResource{Kind: "ConfigMap", Namespace: "normalized-ns", Name: "normalized"},
				Category: manifest.WarnDuplicateDocument, Message: "duplicate document shadows an earlier document",
				Detail: []string{"side: " + string(warningSide), "parent: " + parent.String(), "parent path: new/apps", "apiVersion: v2"},
			}})
		})
	}
}
