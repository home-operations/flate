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
			testPairDistinguishesByParentCase(t, &tt)
		},
		)
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
			testPairPathIsPresentationCase(t, &testPairPathIsPresentationCaseInputs{leftParent: &leftParent, tt: &tt})
		},
		)
	}
}

func TestRenderDocs_DuplicateWarningOrder(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "./apps"}
	z := manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "z"}
	a := z
	a.Name = "a"
	left := []Doc{
		cmDoc(new(parent), z, "old", ""), cmDoc(new(parent), z, "new", ""),
		cmDoc(new(parent), a, "old", ""), cmDoc(new(parent), a, "new", ""),
	}
	want := make([]manifest.Warning, 0, 2)
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
	left := []Doc{
		cmDoc(new(parent), manifest.NamedResource{Name: "shadow-orig"}, "shadowed", ""),
		cmDoc(new(parent), manifest.NamedResource{Name: "last-orig"}, "old", ""),
	}
	right := []Doc{
		cmDoc(new(parent), manifest.NamedResource{Name: "shadow-current"}, "shadowed", ""),
		cmDoc(new(parent), manifest.NamedResource{Name: "last-current"}, "new", ""),
	}
	for _, format := range []Format{FormatHuman, FormatGitHub, FormatBrief, FormatGitLab, FormatGitea, FormatDiff, FormatHTML, FormatJSON, ""} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			var first []byte
			var firstWarnings []manifest.Warning
			for run := range 2 {
				calls := make(map[string]int)
				out, warnings, err := RenderDocs(
					left,
					right,
					Options{Format: format, Normalize: func(m map[string]any) {
						meta := m["metadata"].(map[string]any)
						calls[meta["name"].(string)]++
						meta["name"] = "normalized"
					}},
				)
				assert.NoError(t, err)
				assert.Diff(
					t,
					calls,
					map[string]int{"shadow-orig": 1, "last-orig": 1, "shadow-current": 1, "last-current": 1},
				)
				assert.FatalIf(t, len(warnings) != 2, "want two snapshot warnings, got %+v", warnings)
				sides := map[string]bool{}
				for _, warning := range warnings {
					assert.Equal(t, warning.Category, manifest.WarnDuplicateDocument)
					assert.Equal(t, warning.Resource, manifest.NamedResource{Kind: "ConfigMap", Name: "normalized"})
					assert.Diff(
						t,
						warning.Detail[1:],
						[]string{"parent: " + parent.String(), "parent path: apps", "apiVersion: v1"},
					)
					sides[warning.Detail[0]] = true
				}
				assert.Diff(t, sides, map[string]bool{"side: orig": true, "side: current": true})
				assert.FatalIf(t, len(out) == 0 || strings.Contains(string(out), "shadowed") ||
					strings.Contains(string(out), "shadows an earlier"), "unexpected winning product: %s", out)
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

func testPairDistinguishesByParentCase(t *testing.T, testCaseInput *struct {
	name            string
	parentGroup     string
	parentKind      string
	parentNamespace string
	parentName      string
	apiVersion      string
	pairs           int
},
) {
	t.Helper()

	parent := Parent{NamedResource: testutil.HelmReleaseID("ns", "a")}
	left := cmDoc(new(parent), manifest.NamedResource{Name: "x", Namespace: "ns"}, "old", "")
	left.Manifest["kind"], left.Manifest["apiVersion"] = "Deployment", "apps/v1"
	parent.Group, parent.Kind, parent.Namespace, parent.Name = testCaseInput.parentGroup, testCaseInput.parentKind, testCaseInput.parentNamespace, testCaseInput.parentName
	right := cmDoc(new(parent), manifest.NamedResource{Name: "x", Namespace: "ns"}, "new", "")
	right.Manifest["kind"], right.Manifest["apiVersion"] = "Deployment", testCaseInput.apiVersion
	l, r := []Doc{left}, []Doc{right}
	if got := diffprep.Prepare(
		prepareDocs(l),
		prepareDocs(r),
		&diffprep.Options{},
	).Pairs; len(
		got,
	) != testCaseInput.pairs {
		t.Fatalf("pairs = %+v, want %d", got, testCaseInput.pairs)
	}
	changes, _ := Changes(l, r, Options{})
	if len(changes) != testCaseInput.pairs {
		t.Fatalf("changes = %+v, want %d", changes, testCaseInput.pairs)
	}
	count := 0
	prepared := diffprep.Prepare(prepareDocs(l), prepareDocs(r), &diffprep.Options{})
	lines, err := output.PrepareLines(prepared)
	assert.NoError(t, err)
	{
		cValues := lines
		for cIndex := range cValues {
			c := cValues[cIndex]
			count++
			want := left
			if c.Status != string(StatusRemoved) {
				want = right
			}
			group := "apps"
			if c.Status != string(StatusRemoved) {
				group, _, _ = strings.Cut(testCaseInput.apiVersion, "/")
				if !strings.Contains(testCaseInput.apiVersion, "/") {
					group = ""
				}
			}
			assert.ErrorIf(t, c.Parent.ID != want.Parent.NamedResource ||
				c.Resource.ID != (manifest.NamedResource{Group: group, Kind: "Deployment", Namespace: "ns", Name: "x"}), "line change dropped identity: %+v", c)
		}
	}
	if count != testCaseInput.pairs {
		t.Errorf("line changes = %d, want %d", count, testCaseInput.pairs)
	}
	native := renderDiff(t, l, r, new(Options{Format: FormatGitHub}))
	if testCaseInput.pairs == 2 {
		assert.ErrorIf(t, strings.Count(native, "one document removed:") != 1 ||
			strings.Count(
				native,
				"one document added:",
			) != 1, "native lost parent/resource identity:\n%s", native)
	} else {
		assert.ErrorIf(
			t,
			strings.Contains(native, "one document") || !strings.Contains(native, "@@ apiVersion @@"),
			"native version update must be one content change:\n%s",
			native,
		)
	}
	assert.ErrorIf(
		t,
		testCaseInput.pairs == 1 &&
			(changes[0].Status != StatusChanged || changes[0].APIVersion != testCaseInput.apiVersion),
		"version change must retain the current authored version: %+v",
		changes,
	)
}

type testPairPathIsPresentationCaseInputs struct {
	leftParent *Parent
	tt         *struct {
		name     string
		path     string
		resource string
		parent   string
	}
}

func testPairPathIsPresentationCase(t *testing.T, testCaseInput *testPairPathIsPresentationCaseInputs) {
	t.Helper()

	rightParent := (*testCaseInput.leftParent)
	rightParent.Path = testCaseInput.tt.path
	left := []Doc{
		cmDoc(new((*testCaseInput.leftParent)), manifest.NamedResource{Name: "shared", Namespace: "apps"}, "old", ""),
	}
	right := []Doc{cmDoc(new(rightParent), manifest.NamedResource{Name: "shared", Namespace: "apps"}, "old", "")}
	{
		got, _ := Changes(left, right, Options{})
		assert.FatalIf(t, len(got) != 0, "path-only move produced changes: %+v", got)
	}
	for _, format := range []Format{FormatDiff, FormatHTML, FormatGitHub} {
		out, _, err := RenderDocs(left, right, Options{Format: format})
		assert.FatalIf(
			t,
			err != nil || len(out) != 0,
			"%s path-only move produced output %q: %v",
			format,
			out,
			err,
		)
	}
	right[0].Manifest["data"] = map[string]any{"k": "new"}
	changes, _ := Changes(left, right, Options{})
	assert.FatalIf(
		t,
		len(changes) != 1 || changes[0].Status != StatusChanged || changes[0].Parent != rightParent,
		"content change must use current parent: %+v",
		changes,
	)
	for _, format := range []Format{FormatDiff, FormatHTML, FormatGitHub} {
		out, _, err := RenderDocs(left, right, Options{Format: format})
		assert.NoError(t, err)
		assert.ErrorIf(
			t,
			strings.Contains(string(out), "old/apps"),
			"%s heading contains old path: %s",
			format,
			out,
		)
		switch format {
		case FormatDiff:
			want := "--- " + testCaseInput.tt.resource + "\n+++ " + testCaseInput.tt.resource + "\n"
			assert.ErrorIf(
				t,
				!strings.HasPrefix(string(out), want),
				"unified headers want %q, got %s",
				want,
				out,
			)
		case FormatGitHub:
			assert.ErrorIf(
				t,
				!strings.Contains(string(out), "@@ data.k @@") ||
					strings.Contains(string(out), "one document"),
				"native path edit split one pair: %s",
				out,
			)
		case FormatHuman, FormatBrief, FormatGitLab, FormatGitea, FormatHTML, FormatJSON, "":
			want := `<div class="rparent">` + testCaseInput.tt.parent + `</div>`
			assert.ErrorIf(t, !strings.Contains(string(out), want), "HTML parent heading missing %q", want)
		}
	}
	removed, _ := Changes(left, nil, Options{})
	assert.ErrorIf(
		t,
		len(removed) != 1 || removed[0].Parent != (*testCaseInput.leftParent),
		"removal must use original parent: %+v",
		removed,
	)
	want := "--- ConfigMap apps/shared (old/apps)\n+++ ConfigMap apps/shared (old/apps)\n"
	{
		out := unified(t, left, nil)
		assert.ErrorIf(t, !strings.HasPrefix(out, want), "removal headers want %q, got %s", want, out)
	}
}
