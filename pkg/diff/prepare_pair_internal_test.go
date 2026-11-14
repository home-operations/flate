package diff

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func preparedCMDoc(parent *Parent, id manifest.NamedResource, dataVal, chart string) Doc {
	meta := map[string]any{"name": id.Name}
	if id.Namespace != "" {
		meta["namespace"] = id.Namespace
	}
	if chart != "" {
		meta["labels"] = map[string]any{"helm.sh/chart": chart}
	}
	return Doc{
		Parent: *parent,
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
		t.Run(side, func(t *testing.T) { t.Parallel(); pairDuplicateDocumentsWarnAndLastWinsSide(t, &parent, &side) })
	}
}

func pairDuplicateDocumentsWarnAndLastWinsSide(t *testing.T, parent *Parent, side *string) {
	t.Helper()

	first := preparedCMDoc(parent, manifest.NamedResource{Name: "first", Namespace: "ns"}, "shadowed", "first-1")
	last := preparedCMDoc(parent, manifest.NamedResource{Name: "last", Namespace: "ns"}, "retained", "last-2")
	last.Parent.Path = "new/apps"
	last.Manifest["apiVersion"] = "v2"
	other := preparedCMDoc(parent, manifest.NamedResource{Name: "other", Namespace: "ns"}, "other-side", "other-3")
	foreign := preparedCMDoc(parent, manifest.NamedResource{Name: "foreign", Namespace: "ns"}, "separate-group", "")
	foreign.Manifest["apiVersion"] = "example.test/v1"
	left, right := []Doc{first, last, foreign}, []Doc{other}
	if (*side) == "right" {
		left, right = right, left
	}
	result := prepare(
		left,
		right,
		&Options{
			StripAttrs: []string{"helm.sh/chart", "checksum/", "app.kubernetes.io/version", "chart"},
			Normalize: func(m map[string]any) {
				meta := m["metadata"].(map[string]any)
				meta["name"], meta["namespace"] = "normalized", "normalized-ns"
				apiVersion := "v3"
				if group := manifest.DocIdentity(m).Group; group != "" {
					apiVersion = group + "/" + apiVersion
				}
				m["apiVersion"] = apiVersion
			},
		},
	)
	pairs, warnings := result.Pairs, result.Warnings
	assert.FatalIf(t, len(pairs) != 2, "expected two paired resources, got %d", len(pairs))
	assert.Equal(
		t,
		pairs[1].Resource,
		manifest.NamedResource{
			Group:     "example.test",
			Kind:      "ConfigMap",
			Namespace: "normalized-ns",
			Name:      "normalized",
		},
	)
	foreignDoc, absent := pairs[1].Old, pairs[1].New
	if (*side) == "right" {
		foreignDoc, absent = absent, foreignDoc
	}
	assert.FatalIf(
		t,
		absent != nil || foreignDoc["data"].(map[string]any)["k"] != "separate-group",
		"%v",
		"cross-group duplicate changed pairing",
	)
	retained, counterpart := pairs[0].Old, pairs[0].New
	if (*side) == "right" {
		retained, counterpart = counterpart, retained
	}
	if retained["data"].(map[string]any)["k"] != "retained" ||
		counterpart["data"].(map[string]any)["k"] != "other-side" {
		t.Fatalf("expected last document paired with other side, got %+v", pairs[0])
	}
	retainedChart, counterpartChart := pairs[0].OldChart, pairs[0].NewChart
	wantParent := other.Parent
	if (*side) == "right" {
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
	if (*side) == "right" {
		warningSide = SideCurrent
	}
	assert.Diff(t, warnings, []manifest.Warning{
		{
			Resource: manifest.NamedResource{Kind: "ConfigMap", Namespace: "normalized-ns", Name: "normalized"},
			Category: manifest.WarnDuplicateDocument,
			Message:  "duplicate document shadows an earlier document",
			Detail: []string{
				"side: " + string(warningSide),
				"parent: " + parent.String(),
				"parent path: new/apps",
				"apiVersion: v2",
			},
		},
	})
}

func TestPair_EntryComparisonPreservesCurrentParent(t *testing.T) {
	t.Parallel()
	old := Parent{NamedResource: testutil.KustomizationID("apps", "old"), Path: "entry"}
	current := Parent{NamedResource: testutil.KustomizationID("apps", "current"), Path: "entry"}
	key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "payload", Namespace: "apps"}
	result := prepare(
		[]Doc{preparedCMDoc(&old, id, "before", "")},
		[]Doc{preparedCMDoc(&current, id, "after", "")},
		&Options{producerKeys: [2]map[manifest.NamedResource]manifest.NamedResource{
			{old.NamedResource: key}, {current.NamedResource: key},
		}},
	)
	assert.Equal(t, len(result.Pairs), 1)
	assert.Equal(t, len(result.Warnings), 0)
	entry := result.Pairs[0]
	assert.Equal(t, entry.Parent, current)
	assert.Equal(t, entry.status(), "changed")
	assert.Equal(t, entry.Old["data"].(map[string]any)["k"], any("before"))
	assert.Equal(t, entry.New["data"].(map[string]any)["k"], any("after"))
}

func TestPair_GeneratorRenamePairsBothSides(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "media"), Path: "./media"}
	removed := manifest.NamedResource{Kind: "ConfigMap", Namespace: "media", Name: "plex-values-7c8m2d9f5b"}
	added := manifest.NamedResource{Kind: "ConfigMap", Namespace: "media", Name: "plex-values-k4h2g6t8bm"}

	pairs := prepare(
		[]Doc{preparedCMDoc(&parent, removed, "before", "")},
		[]Doc{preparedCMDoc(&parent, added, "after", "")},
		&Options{},
	).Pairs

	assert.FatalIf(t, len(pairs) != 1, "expected one paired resource, got %d", len(pairs))
	entry := pairs[0]
	assert.Equal(t, entry.status(), "changed")
	assert.Equal(t, entry.Resource, added)
	assert.Equal(t, entry.Output, [2]manifest.NamedResource{removed, added})
	assert.Equal(t, entry.Old["data"].(map[string]any)["k"], any("before"))
	assert.Equal(t, entry.New["data"].(map[string]any)["k"], any("after"))
}

func TestPair_GeneratorRenameNeedsOneCandidatePerSide(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "media"), Path: "./media"}
	doc := func(kind, name string) Doc {
		d := preparedCMDoc(&parent, manifest.NamedResource{Namespace: "media", Name: name}, name, "")
		d.Manifest["kind"] = kind
		return d
	}
	tests := []struct {
		name        string
		left, right []Doc
		want        []string
	}{
		{
			name:  "two removed candidates",
			left:  []Doc{doc("ConfigMap", "plex-values-7c8m2d9f5b"), doc("ConfigMap", "plex-values-9d5f2c8b7m")},
			right: []Doc{doc("ConfigMap", "plex-values-k4h2g6t8bm")},
			want:  []string{"removed", "removed", "added"},
		},
		{
			name:  "suffix outside the generator alphabet",
			left:  []Doc{doc("ConfigMap", "release-2026110100")},
			right: []Doc{doc("ConfigMap", "release-2026110200")},
			want:  []string{"removed", "added"},
		},
		{
			name:  "different kinds",
			left:  []Doc{doc("ConfigMap", "plex-values-7c8m2d9f5b")},
			right: []Doc{doc("Secret", "plex-values-k4h2g6t8bm")},
			want:  []string{"removed", "added"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pairs := prepare(tt.left, tt.right, &Options{}).Pairs
			got := make([]string, 0, len(pairs))
			for i := range pairs {
				got = append(got, pairs[i].status())
			}
			assert.Diff(t, got, tt.want)
		})
	}
}
