package diff

import (
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPair_DistinguishesByIdentity(t *testing.T) {
	t.Parallel()
	oldParent := Parent{NamedResource: testutil.HelmReleaseID("ns", "a")}
	newParent := Parent{NamedResource: testutil.HelmReleaseID("ns", "b")}
	for _, tc := range []struct {
		name, apiVersion, namespace string
		paired                      bool
	}{
		{"same identity", "apps/v1", "ns", true},
		{"resource group", "example.test/v1", "ns", false},
		{"resource version", "apps/v2", "ns", true},
		{"resource namespace", "apps/v1", "other", false},
		{"permissive resource group", "apps/v1/extra", "ns", true},
		{"core resource group", "v1", "ns", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			old := cmDoc(&oldParent, manifest.NamedResource{Name: "x", Namespace: "ns"}, "old", "")
			current := cmDoc(&newParent, manifest.NamedResource{Name: "x", Namespace: tc.namespace}, "new", "")
			old.Manifest["kind"], old.Manifest["apiVersion"] = "Deployment", "apps/v1"
			current.Manifest["kind"], current.Manifest["apiVersion"] = "Deployment", tc.apiVersion
			left, right := []Doc{old}, []Doc{current}
			result := prepare(left, right, &Options{})
			changes := publicChanges(result)
			lines, err := prepareLines(result)
			assert.NoError(t, err)
			out := renderDiff(t, left, right, &Options{Format: FormatGitHub})
			if !tc.paired {
				assert.Equal(t, len(lines), 2)
				assert.Equal(t, len(changes), 2)
				assert.Equal(t, changes[0].Status, StatusRemoved)
				assert.Equal(t, changes[1].Status, ChangeStatus("added"))
				assert.Equal(t, strings.Count(out, "one document removed:"), 1)
				assert.Equal(t, strings.Count(out, "one document added:"), 1)
				return
			}
			assert.Equal(t, len(lines), 1)
			assert.Equal(t, len(changes), 1)
			assert.Equal(t, lines[0].Parent.ID, newParent.NamedResource)
			assert.Equal(t, lines[0].Resource.ID, manifest.DocIdentity(current.Manifest))
			assert.Equal(t, changes[0].Status, ChangeStatus("changed"))
			assert.Equal(t, changes[0].APIVersion, tc.apiVersion)
			assert.Equal(t, strings.Count(out, "producer: HelmRelease ns/a -> HelmRelease ns/b"), 1)
			assert.Equal(t, strings.Contains(out, "one document"), false)
			assert.Equal(t, strings.Contains(out, "@@ apiVersion @@"), tc.apiVersion != "apps/v1")
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
			testPairPathIsPresentationCase(t, &testPairPathIsPresentationCaseInputs{leftParent: &leftParent, tt: &tt})
		},
		)
	}
}

func TestComparison_DuplicateWarningOrder(t *testing.T) {
	t.Parallel()
	parent := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "./apps"}
	z := manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "z"}
	a := z
	a.Name = "a"
	left := []Doc{
		cmDoc(new(parent), z, "old", ""), cmDoc(new(parent), z, "new", ""),
		cmDoc(new(parent), a, "old", ""), cmDoc(new(parent), a, "new", ""),
	}
	winner := Parent{NamedResource: testutil.KustomizationID("flux-system", "winner")}
	conflicts := []Doc{cmDoc(&winner, z, "chosen", ""), cmDoc(&winner, a, "chosen", "")}
	winnerFirst := slices.Concat(conflicts, left)
	want := make([]manifest.Warning, 0, 2)
	for _, id := range []manifest.NamedResource{a, z} {
		want = append(want, manifest.Warning{
			Resource: id, Category: manifest.WarnDuplicateDocument,
			Message: "duplicate document shadows an earlier document",
			Detail:  []string{"side: orig", "parent: " + parent.String(), "parent path: ./apps", "apiVersion: v1"},
		})
	}
	first, warnings, err := renderDocuments(append(left, conflicts...), nil, &Options{Format: FormatGitHub})
	assert.NoError(t, err)
	assert.Diff(t, warnings, want)
	second, warnings, err := renderDocuments(winnerFirst, nil, &Options{Format: FormatGitHub})
	assert.NoError(t, err)
	assert.Diff(t, warnings, want)
	assert.Equal(t, string(first), string(second))
	changes := jsonChanges(t, winnerFirst, nil, &Options{})
	assert.Equal(t, len(changes), 2)
	for _, change := range changes {
		assert.Equal(t, change.Parent, winner)
		assert.Equal(t, change.Old["data"].(map[string]any)["k"], any("chosen"))
	}
}

func TestComparison_NormalizedDuplicatesOnce(t *testing.T) {
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
				out, warnings, err := renderDocuments(
					left,
					right,
					new(Options{Format: format, Normalize: func(m map[string]any) {
						meta := m["metadata"].(map[string]any)
						calls[meta["name"].(string)]++
						meta["name"] = "normalized"
					}}),
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
		got := publicChanges(prepare(left, right, new(Options{})))
		assert.FatalIf(t, len(got) != 0, "path-only move produced changes: %+v", got)
	}
	for _, format := range []Format{FormatDiff, FormatHTML, FormatGitHub} {
		out, _, err := renderDocuments(left, right, new(Options{Format: format}))
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
	changes := publicChanges(prepare(left, right, new(Options{})))
	assert.FatalIf(
		t,
		len(changes) != 1 || changes[0].Status != ChangeStatus("changed") || changes[0].Parent != rightParent,
		"content change must use current parent: %+v",
		changes,
	)
	for _, format := range []Format{FormatDiff, FormatHTML, FormatGitHub} {
		out, _, err := renderDocuments(left, right, new(Options{Format: format}))
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
	removed := publicChanges(prepare(left, nil, new(Options{})))
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
