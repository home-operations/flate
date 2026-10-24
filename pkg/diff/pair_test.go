package diff

import (
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
		name, parentGroup, parentKind, parentNamespace, parentName, apiVersion string
		pairs                                                                  int
	}{
		{"ownerless parent", "", "", "", "", "apps/v1", 2},
		{"parent name", manifest.HelmReleaseDomain, "HelmRelease", "ns", "b", "apps/v1", 2},
		{"parent group", "example.test", "HelmRelease", "ns", "a", "apps/v1", 2},
		{"resource group", manifest.HelmReleaseDomain, "HelmRelease", "ns", "a", "example.test/v1", 2},
		{"resource version", manifest.HelmReleaseDomain, "HelmRelease", "ns", "a", "apps/v2", 1},
		{"parent kind", manifest.HelmReleaseDomain, "OtherProducer", "ns", "a", "apps/v1", 2},
		{"parent namespace", manifest.HelmReleaseDomain, "HelmRelease", "other", "a", "apps/v1", 2},
		{"permissive resource group", manifest.HelmReleaseDomain, "HelmRelease", "ns", "a", "apps/v1/extra", 1},
		{"core resource group", manifest.HelmReleaseDomain, "HelmRelease", "ns", "a", "v1", 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parent := Parent{NamedResource: testutil.HelmReleaseID("ns", "a")}
			left := cmDoc(parent, manifest.NamedResource{Name: "x", Namespace: "ns"}, "old", "")
			left.Manifest["kind"], left.Manifest["apiVersion"] = "Deployment", "apps/v1"
			parent.Group, parent.Kind, parent.Namespace, parent.Name = tt.parentGroup, tt.parentKind, tt.parentNamespace, tt.parentName
			right := cmDoc(parent, manifest.NamedResource{Name: "x", Namespace: "ns"}, "new", "")
			right.Manifest["kind"], right.Manifest["apiVersion"] = "Deployment", tt.apiVersion
			l, r := []Doc{left}, []Doc{right}
			if got, _, _, _ := pair(l, r, Options{}); len(got) != tt.pairs {
				t.Fatalf("pairs = %+v, want %d", got, tt.pairs)
			}
			changes, _ := Changes(l, r, Options{})
			if len(changes) != tt.pairs {
				t.Fatalf("changes = %+v, want %d", changes, tt.pairs)
			}
			count := 0
			paired, _, _, _ := pair(l, r, Options{})
			for c, err := range changedPairs(paired) {
				count++
				if err != nil {
					t.Fatal(err)
				}
				want := left
				if c.Status != string(StatusRemoved) {
					want = right
				}
				group := "apps"
				if c.Status != string(StatusRemoved) {
					group, _, _ = strings.Cut(tt.apiVersion, "/")
					if !strings.Contains(tt.apiVersion, "/") {
						group = ""
					}
				}
				if c.Parent != want.Parent.NamedResource || c.Resource != (manifest.NamedResource{Group: group, Kind: "Deployment", Namespace: "ns", Name: "x"}) {
					t.Errorf("line change dropped identity: %+v", c)
				}
			}
			if count != tt.pairs {
				t.Errorf("line changes = %d, want %d", count, tt.pairs)
			}
			native := renderDiff(t, l, r, Options{Format: FormatGitHub})
			if tt.pairs == 2 {
				if strings.Count(native, "one document removed:") != 1 || strings.Count(native, "one document added:") != 1 {
					t.Errorf("native lost parent/resource identity:\n%s", native)
				}
			} else if strings.Contains(native, "one document") || !strings.Contains(native, "@@ apiVersion @@") {
				t.Errorf("native version update must be one content change:\n%s", native)
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
			if got, _ := Changes(left, right, Options{}); len(got) != 0 {
				t.Fatalf("path-only move produced changes: %+v", got)
			}
			for _, format := range []Format{FormatDiff, FormatHTML, FormatGitHub} {
				out, _, err := RenderDocs(left, right, Options{Format: format})
				if err != nil || len(out) != 0 {
					t.Fatalf("%s path-only move produced output %q: %v", format, out, err)
				}
			}
			right[0].Manifest["data"] = map[string]any{"k": "new"}
			changes, _ := Changes(left, right, Options{})
			if len(changes) != 1 || changes[0].Status != StatusChanged || changes[0].Parent != rightParent {
				t.Fatalf("content change must use current parent: %+v", changes)
			}
			for _, format := range []Format{FormatDiff, FormatHTML, FormatGitHub} {
				out, _, err := RenderDocs(left, right, Options{Format: format})
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
				} else if format == FormatGitHub {
					if !strings.Contains(string(out), "@@ data.k @@") || strings.Contains(string(out), "one document") {
						t.Errorf("native path edit split one pair: %s", out)
					}
				} else if want := `<div class="rparent">` + tt.parent + `</div>`; !strings.Contains(string(out), want) {
					t.Errorf("HTML parent heading missing %q", want)
				}
			}
			removed, _ := Changes(left, nil, Options{})
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
			pairs, warnings, _, _ := pair(left, right, Options{StripAttrs: DefaultStripAttrs, Normalize: func(m map[string]any) {
				meta := m["metadata"].(map[string]any)
				meta["name"], meta["namespace"] = "normalized", "normalized-ns"
				apiVersion := "v3"
				if group := manifest.DocIdentity(m).Group; group != "" {
					apiVersion = group + "/" + apiVersion
				}
				m["apiVersion"] = apiVersion
			}})
			if len(pairs) != 2 {
				t.Fatalf("expected two paired resources, got %d", len(pairs))
			}
			assert.Equal(t, pairs[1].resource, manifest.NamedResource{Group: "example.test", Kind: "ConfigMap", Namespace: "normalized-ns", Name: "normalized"})
			foreignDoc, absent := pairs[1].a, pairs[1].b
			if side == "right" {
				foreignDoc, absent = absent, foreignDoc
			}
			if absent != nil || foreignDoc["data"].(map[string]any)["k"] != "separate-group" {
				t.Fatal("cross-group duplicate changed pairing")
			}
			retained, counterpart := pairs[0].a, pairs[0].b
			if side == "right" {
				retained, counterpart = counterpart, retained
			}
			if retained["data"].(map[string]any)["k"] != "retained" || counterpart["data"].(map[string]any)["k"] != "other-side" {
				t.Fatalf("expected last document paired with other side, got %+v", pairs[0])
			}
			retainedChart, counterpartChart := pairs[0].aChart, pairs[0].bChart
			wantParent := other.Parent
			if side == "right" {
				retainedChart, counterpartChart = counterpartChart, retainedChart
				wantParent = last.Parent
			}
			assert.Equal(t, retainedChart, "last-2")
			assert.Equal(t, counterpartChart, "other-3")
			assert.Equal(t, pairs[0].parent, wantParent)
			assert.Equal(t, pairs[0].apiVersion, "v3")
			assert.Equal(t, pairs[0].resource.Name, "normalized")
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

func TestRenderDocs_DuplicateWarningOrder(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "./apps"}
	z := manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "z"}
	a := z
	a.Name = "a"
	left := []Doc{
		cmDoc(parent, z, "old", ""), cmDoc(parent, z, "new", ""),
		cmDoc(parent, a, "old", ""), cmDoc(parent, a, "new", ""),
	}
	var want []manifest.Warning
	for _, id := range []manifest.NamedResource{a, z} {
		want = append(want, manifest.Warning{
			Resource: id, Category: manifest.WarnDuplicateDocument,
			Message: "duplicate document shadows an earlier document",
			Detail:  []string{"side: orig", "parent: " + parent.String(), "parent path: ./apps", "apiVersion: v1"},
		})
	}
	first, warnings, err := RenderDocs(left, nil, Options{Format: FormatGitHub})
	assert.NoError(t, err)
	assert.Diff(t, warnings, want)
	second, warnings, err := RenderDocs(left, nil, Options{Format: FormatGitHub})
	assert.NoError(t, err)
	assert.Diff(t, warnings, want)
	assert.Equal(t, string(first), string(second))
}

func TestChangedPairs_NormalizeOncePerIteration(t *testing.T) {
	t.Parallel()
	left, right := []Doc{cm("x", "ns", "owner", "old")}, []Doc{cm("x", "ns", "owner", "new")}
	before := []Doc{left[0], right[0]}
	for i := range before {
		before[i].Manifest = manifest.DeepCopyMap(before[i].Manifest)
	}
	calls := 0
	paired, warnings, _, _ := pair(left, right, Options{Normalize: func(m map[string]any) {
		calls++
		meta := m["metadata"].(map[string]any)
		meta["name"] = meta["name"].(string) + "-normalized"
	}})
	assert.Equal(t, len(warnings), 0)
	seq := changedPairs(paired)
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
	assert.Equal(t, calls, 0)
	assert.Diff(t, second, first)
	assert.Diff(t, []Doc{left[0], right[0]}, before)
	bad := cm("z", "ns", "owner", "unencodable")
	bad.Manifest["bad"] = make(chan int)
	lazyPairs, _, _, _ := pair(append(left, bad), nil, Options{})
	consumed := 0
	for _, err := range changedPairs(lazyPairs) {
		assert.NoError(t, err)
		consumed++
		break
	}
	assert.Equal(t, consumed, 1)
}

func TestRenderDocs_NormalizedDuplicatesOnce(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "apps"}
	left := []Doc{cmDoc(parent, manifest.NamedResource{Name: "shadow-orig"}, "shadowed", ""), cmDoc(parent, manifest.NamedResource{Name: "last-orig"}, "old", "")}
	right := []Doc{cmDoc(parent, manifest.NamedResource{Name: "shadow-current"}, "shadowed", ""), cmDoc(parent, manifest.NamedResource{Name: "last-current"}, "new", "")}
	for _, format := range []Format{FormatHuman, FormatGitHub, FormatBrief, FormatGitLab, FormatGitea, FormatDiff, FormatHTML, FormatJSON, ""} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			var first []byte
			var firstWarnings []manifest.Warning
			for run := range 2 {
				calls := make(map[string]int)
				out, warnings, err := RenderDocs(left, right, Options{Format: format, Normalize: func(m map[string]any) {
					meta := m["metadata"].(map[string]any)
					calls[meta["name"].(string)]++
					meta["name"] = "normalized"
				}})
				assert.NoError(t, err)
				assert.Diff(t, calls, map[string]int{"shadow-orig": 1, "last-orig": 1, "shadow-current": 1, "last-current": 1})
				if len(warnings) != 2 {
					t.Fatalf("want two snapshot warnings, got %+v", warnings)
				}
				sides := map[string]bool{}
				for _, warning := range warnings {
					assert.Equal(t, warning.Category, manifest.WarnDuplicateDocument)
					assert.Equal(t, warning.Resource, manifest.NamedResource{Kind: "ConfigMap", Name: "normalized"})
					assert.Diff(t, warning.Detail[1:], []string{"parent: " + parent.String(), "parent path: apps", "apiVersion: v1"})
					sides[warning.Detail[0]] = true
				}
				assert.Diff(t, sides, map[string]bool{"side: orig": true, "side: current": true})
				if len(out) == 0 || strings.Contains(string(out), "shadowed") || strings.Contains(string(out), "shadows an earlier") {
					t.Fatalf("unexpected winning product: %s", out)
				}
				if run == 0 {
					first, firstWarnings = out, warnings
				} else {
					assert.Diff(t, out, first)
					assert.Diff(t, warnings, firstWarnings)
				}
			}
		})
	}
}
