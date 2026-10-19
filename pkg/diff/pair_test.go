package diff

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPair_DistinguishesByParent(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, parentGroup, parentName, apiVersion string
		pairs                                     int
	}{
		{"parent name", manifest.HelmReleaseDomain, "b", "apps/v1", 2},
		{"parent group", "example.test", "a", "apps/v1", 2},
		{"resource group", manifest.HelmReleaseDomain, "a", "example.test/v1", 2},
		{"resource version", manifest.HelmReleaseDomain, "a", "apps/v2", 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parent := Parent{NamedResource: testutil.HelmReleaseID("ns", "a")}
			left := cmDoc(parent, manifest.NamedResource{Name: "x", Namespace: "ns"}, "old", "")
			left.Manifest["kind"], left.Manifest["apiVersion"] = "Deployment", "apps/v1"
			parent.Group, parent.Name = tt.parentGroup, tt.parentName
			right := cmDoc(parent, manifest.NamedResource{Name: "x", Namespace: "ns"}, "new", "")
			right.Manifest["kind"], right.Manifest["apiVersion"] = "Deployment", tt.apiVersion
			l, r := []Doc{left}, []Doc{right}
			if got, _, _ := pair(l, r, Options{}); len(got) != tt.pairs {
				t.Fatalf("pairs = %+v, want %d", got, tt.pairs)
			}
			changes := Changes(l, r, Options{})
			if len(changes) != tt.pairs {
				t.Fatalf("changes = %+v, want %d", changes, tt.pairs)
			}
			count := 0
			for c, err := range changedPairs(l, r, Options{}) {
				count++
				if err != nil {
					t.Fatal(err)
				}
				want := left
				if c.Status != string(StatusRemoved) {
					want = right
				}
				group := "apps"
				if want.Manifest["apiVersion"] == "example.test/v1" {
					group = "example.test"
				}
				if c.Parent != want.Parent.NamedResource || c.Resource != (manifest.NamedResource{Group: group, Kind: "Deployment", Namespace: "ns", Name: "x"}) {
					t.Errorf("line change dropped identity: %+v", c)
				}
			}
			if count != tt.pairs {
				t.Errorf("line changes = %d, want %d", count, tt.pairs)
			}
			if tt.pairs == 1 && (changes[0].Status != StatusChanged || changes[0].APIVersion != tt.apiVersion) {
				t.Errorf("version change must retain the current authored version: %+v", changes)
			}
		})
	}
}

func TestPair_PathIsPresentation(t *testing.T) {
	t.Parallel()
	leftParent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "old/apps"}
	for _, tt := range []struct {
		name, path, resource, parent string
	}{
		{"moved", "new/apps", "ConfigMap apps/shared (new/apps)", "Kustomization.kustomize.toolkit.fluxcd.io flux-system/apps (new/apps)"},
		{"cleared", "", "ConfigMap apps/shared", "Kustomization.kustomize.toolkit.fluxcd.io flux-system/apps"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rightParent := leftParent
			rightParent.Path = tt.path
			left := []Doc{cmDoc(leftParent, manifest.NamedResource{Name: "shared", Namespace: "apps"}, "old", "")}
			right := []Doc{cmDoc(rightParent, manifest.NamedResource{Name: "shared", Namespace: "apps"}, "old", "")}
			if got := Changes(left, right, Options{}); len(got) != 0 {
				t.Fatalf("path-only move produced changes: %+v", got)
			}
			for _, format := range []Format{FormatDiff, FormatHTML} {
				out, err := RenderDocs(left, right, Options{Format: format})
				if err != nil || len(out) != 0 {
					t.Fatalf("%s path-only move produced output %q: %v", format, out, err)
				}
			}
			right[0].Manifest["data"] = map[string]any{"k": "new"}
			changes := Changes(left, right, Options{})
			if len(changes) != 1 || changes[0].Status != StatusChanged || changes[0].Parent != rightParent {
				t.Fatalf("content change must use current parent: %+v", changes)
			}
			for _, format := range []Format{FormatDiff, FormatHTML} {
				out, err := RenderDocs(left, right, Options{Format: format})
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(out), "old/apps") {
					t.Errorf("%s heading contains old path: %s", format, out)
				}
				if format == FormatDiff {
					want := "--- " + tt.resource + "\n+++ " + tt.resource + "\n"
					if !strings.HasPrefix(string(out), want) {
						t.Errorf("unified headers want %q, got %s", want, out)
					}
				} else if want := `<div class="rparent">` + tt.parent + `</div>`; !strings.Contains(string(out), want) {
					t.Errorf("HTML parent heading missing %q", want)
				}
			}
			removed := Changes(left, nil, Options{})
			if len(removed) != 1 || removed[0].Parent != leftParent {
				t.Errorf("removal must use original parent: %+v", removed)
			}
			want := "--- ConfigMap apps/shared (old/apps)\n+++ ConfigMap apps/shared (old/apps)\n"
			if out := unified(t, left, nil); !strings.HasPrefix(out, want) {
				t.Errorf("removal headers want %q, got %s", want, out)
			}
		})
	}
}

func TestPair_DuplicateDocumentsWarnAndLastWins(t *testing.T) {
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "./apps"}
	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })

			first := cmDoc(parent, manifest.NamedResource{Name: "first", Namespace: "ns"}, "shadowed", "first-1")
			last := cmDoc(parent, manifest.NamedResource{Name: "last", Namespace: "ns"}, "retained", "last-2")
			last.Parent.Path = "new/apps"
			last.Manifest["apiVersion"] = "v2"
			other := cmDoc(parent, manifest.NamedResource{Name: "other", Namespace: "ns"}, "other-side", "other-3")
			left, right := []Doc{first, last}, []Doc{other}
			if side == "right" {
				left, right = right, left
			}
			pairs, _, _ := pair(left, right, Options{StripAttrs: DefaultStripAttrs, Normalize: func(m map[string]any) {
				meta := m["metadata"].(map[string]any)
				meta["name"], meta["namespace"] = "normalized", "normalized-ns"
			}})
			if len(pairs) != 1 {
				t.Fatalf("expected one paired resource, got %d", len(pairs))
			}
			retained, counterpart := pairs[0].a, pairs[0].b
			if side == "right" {
				retained, counterpart = counterpart, retained
			}
			if retained["data"].(map[string]any)["k"] != "retained" || counterpart["data"].(map[string]any)["k"] != "other-side" {
				t.Fatalf("expected last document paired with other side, got %+v", pairs[0])
			}
			retainedChart, counterpartChart := pairs[0].aChart, pairs[0].bChart
			wantParent, wantVersion := other.Parent, "v1"
			if side == "right" {
				retainedChart, counterpartChart = counterpartChart, retainedChart
				wantParent, wantVersion = last.Parent, "v2"
			}
			assert.Equal(t, retainedChart, "last-2")
			assert.Equal(t, counterpartChart, "other-3")
			assert.Equal(t, pairs[0].parent, wantParent)
			assert.Equal(t, pairs[0].apiVersion, wantVersion)
			assert.Equal(t, pairs[0].resource.Name, "normalized")
			assert.Equal(t, chartLabel(retained), "")
			assert.Equal(t, chartLabel(counterpart), "")
			if strings.Count(logs.String(), "level=WARN") != 1 {
				t.Fatalf("expected one duplicate warning, got %q", logs.String())
			}
			for _, field := range []string{"diff: duplicate document", "side=" + side, "parentKind=Kustomization", "parentNamespace=flux-system", "parentName=apps", "parentPath=new/apps", "apiVersion=v2", "kind=ConfigMap", "namespace=normalized-ns", "name=normalized"} {
				if !strings.Contains(logs.String(), field) {
					t.Errorf("warning missing %q: %s", field, logs.String())
				}
			}
		})
	}
}

func TestChangedPairs_NormalizeOncePerIteration(t *testing.T) {
	t.Parallel()
	left, right := []Doc{cm("x", "ns", "owner", "old")}, []Doc{cm("x", "ns", "owner", "new")}
	before := []Doc{left[0], right[0]}
	for i := range before {
		before[i].Manifest = manifest.DeepCopyMap(before[i].Manifest)
	}
	calls := 0
	seq := changedPairs(left, right, Options{Normalize: func(m map[string]any) {
		calls++
		meta := m["metadata"].(map[string]any)
		meta["name"] = meta["name"].(string) + "-normalized"
	}})
	var first []output.LineChange
	for c, err := range seq {
		assert.NoError(t, err)
		first = append(first, c)
	}
	assert.Equal(t, calls, 2)
	if len(first) != 1 {
		t.Fatalf("want one changed pair, got %+v", first)
	}
	assert.Equal(t, first[0].Resource.Name, "x-normalized")
	calls = 0
	var second []output.LineChange
	for c, err := range seq {
		assert.NoError(t, err)
		second = append(second, c)
	}
	assert.Equal(t, calls, 2)
	assert.Diff(t, second, first)
	assert.Diff(t, []Doc{left[0], right[0]}, before)
}
