package diff

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diffprep"
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
			if got := diffprep.Prepare(prepareDocs(l), prepareDocs(r), diffprep.Options{}).Pairs; len(got) != tt.pairs {
				t.Fatalf("pairs = %+v, want %d", got, tt.pairs)
			}
			changes, _ := Changes(l, r, Options{})
			if len(changes) != tt.pairs {
				t.Fatalf("changes = %+v, want %d", changes, tt.pairs)
			}
			count := 0
			prepared := diffprep.Prepare(prepareDocs(l), prepareDocs(r), diffprep.Options{})
			lines, err := output.PrepareLines(prepared)
			assert.NoError(t, err)
			for _, c := range lines {
				count++
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
				if c.Parent.ID != want.Parent.NamedResource || c.Resource.ID != (manifest.NamedResource{Group: group, Kind: "Deployment", Namespace: "ns", Name: "x"}) {
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
		{"moved", "new/apps", "ConfigMap apps/shared (new/apps)", "Kustomization flux-system/apps (new/apps)"},
		{"cleared", "", "ConfigMap apps/shared", "Kustomization flux-system/apps"},
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
