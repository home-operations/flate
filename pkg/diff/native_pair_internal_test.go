package diff

import (
	"cmp"
	"html"
	"regexp"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diffprep"
	"github.com/home-operations/flate/internal/testutil"
)

func TestRenderDocs_ParentQualifiedMatching(t *testing.T) {
	t.Parallel()
	left, right := ncm("hello", "greeting", "same"), ncm("hello", "greeting", "same")
	left.Parent = Parent{NamedResource: testutil.HelmReleaseID("apps", "a")}
	right.Parent = Parent{NamedResource: testutil.HelmReleaseID("apps", "b")}
	for _, format := range []Format{FormatHuman, FormatGitHub, FormatBrief, FormatGitLab, FormatGitea, FormatDiff, FormatHTML, FormatJSON, ""} {
		t.Run(cmp.Or(string(format), "default"), func(t *testing.T) {
			t.Parallel()
			testRenderDocsParentQualifiedMatchingCase(
				t,
				&testRenderDocsParentQualifiedMatchingCaseInputs{left: &left, right: &right, format: &format},
			)
		},
		)
	}

	t.Run("shared native label", func(t *testing.T) {
		t.Parallel()
		testRenderDocsParentQualifiedMatchingSharedNativeLabel(
			t,
			&testRenderDocsParentQualifiedMatchingSharedNativeLabelInputs{left: &left, right: &right},
		)
	},
	)
}

func TestRenderDocs_NativePresentation(t *testing.T) {
	t.Parallel()
	left := []Doc{
		ncm("b-removed", "greeting", "gone"),
		ncm("z-modified", "greeting", "old"),
		ncm("middle-unchanged", "greeting", "same"),
		ncm("a-modified", "greeting", "before"),
	}
	right := []Doc{
		ncm("y-added", "greeting", "born"),
		ncm("a-modified", "greeting", "after"),
		left[2],
		ncm("z-modified", "greeting", "new"),
	}
	const github = `
@@ data.greeting @@
# v1/ConfigMap/apps/z-modified
! ± value change
- old
+ new

@@ data.greeting @@
# v1/ConfigMap/apps/a-modified
! ± value change
- before
+ after

@@ (root level) @@
# v1/ConfigMap/apps/b-removed
! - one document removed:
- apiVersion: v1
- kind: ConfigMap
- metadata:
-   name: b-removed
-   namespace: apps
- data:
-   greeting: gone

@@ (root level) @@
# v1/ConfigMap/apps/y-added
! + one document added:
+ apiVersion: v1
+ kind: ConfigMap
+ metadata:
+   name: y-added
+   namespace: apps
+ data:
+   greeting: born

`
	note := Suppression{
		Parent: Parent{NamedResource: testutil.HelmReleaseID("apps", "owner")},
		Side:   SideCurrent,
		Count:  1,
		Reason: "not ready",
	}
	for _, tc := range []struct {
		format Format
		want   string
	}{
		{FormatGitHub, github},
		{FormatBrief, "four changes detected between from, four documents and to, four documents\n\n"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			t.Parallel()
			opts := Options{Format: tc.format, Suppressed: []Suppression{note}}
			first := renderDiff(t, left, right, new(opts))
			assert.Equal(t, first, tc.want)
			assert.Equal(t, renderDiff(t, left, right, new(opts)), first)
		})
	}
}

func TestRenderDocs_NativeCanonicalEdges(t *testing.T) {
	t.Parallel()
	t.Run(
		"namespace presence",
		func(t *testing.T) { t.Parallel(); testRenderDocsNativeCanonicalEdgesNamespacePresence(t) },
	)
	t.Run(
		"same group version",
		func(t *testing.T) { t.Parallel(); testRenderDocsNativeCanonicalEdgesSameGroupVersion(t) },
	)
	t.Run(
		"incomplete normalized identity",
		func(t *testing.T) { t.Parallel(); testRenderDocsNativeCanonicalEdgesIncompleteNormalizedIdentity(t) },
	)
	t.Run(
		"validation before equality",
		func(t *testing.T) { t.Parallel(); testRenderDocsNativeCanonicalEdgesValidationBeforeEquality(t) },
	)
	t.Run("empty documents", func(t *testing.T) { t.Parallel(); testRenderDocsNativeCanonicalEdgesEmptyDocuments(t) })
}

func TestRenderDocs_NativeDuplicateEdges(t *testing.T) {
	t.Parallel()
	for _, side := range []string{"left", "right"} {
		for _, collision := range []bool{false, true} {
			name := side + " duplicate"
			if collision {
				name += " normalized"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testRenderDocsNativeDuplicateEdgesCase(
					t,
					&testRenderDocsNativeDuplicateEdgesCaseInputs{side: &side, collision: &collision},
				)
			},
			)
		}
	}
}

type testRenderDocsParentQualifiedMatchingCaseInputs struct {
	left   *Doc
	right  *Doc
	format *Format
}

func testRenderDocsParentQualifiedMatchingCase(
	t *testing.T,
	testCaseInput *testRenderDocsParentQualifiedMatchingCaseInputs,
) {
	t.Helper()

	out := renderDiff(
		t,
		[]Doc{(*testCaseInput.left)},
		[]Doc{(*testCaseInput.right)}, new(Options{Format: (*testCaseInput.format)}),
	)
	switch *testCaseInput.format {
	case FormatJSON:
		changes := jsonChanges(t, []Doc{(*testCaseInput.left)}, []Doc{(*testCaseInput.right)}, new(Options{}))
		{
			assert.FatalIf(t, len(changes) != 2, "changes = %+v", changes)
		}
		assert.Equal(t, changes[0].Parent, testCaseInput.left.Parent)
		assert.Equal(t, changes[0].Status, StatusRemoved)
		assert.Diff(t, changes[0].Old, testCaseInput.left.Manifest)
		assert.Equal(t, changes[1].Parent, testCaseInput.right.Parent)
		assert.Equal(t, changes[1].Status, StatusAdded)
		assert.Diff(t, changes[1].New, testCaseInput.right.Manifest)
	case FormatBrief:
		{
			assert.FatalIf(t, !strings.Contains(out, "two changes detected"), "want two changes, got %q", out)
		}
	case FormatHTML:
		cells := regexp.MustCompile(`(?s)<td class="diff-code diff-(del|add)">(.*?)</td>`).
			FindAllStringSubmatch(out, -1)
		bodies := map[string]string{}
		tags := regexp.MustCompile(`<[^>]*>`)
		for _, cell := range cells {
			bodies[cell[1]] += html.UnescapeString(tags.ReplaceAllString(cell[2], "")) + "\n"
		}
		const body = "apiVersion: v1\ndata:\n  greeting: same\nkind: ConfigMap\nmetadata:\n  name: hello\n  namespace: apps\n"
		assert.Equal(t, bodies["del"], body)
		assert.Equal(t, bodies["add"], body)
		for _, want := range []string{"1 added", "1 removed", "status-removed", "status-added", "apiVersion", "ConfigMap", "namespace", "hello", "greeting", "same"} {
			{
				assert.ErrorIf(t, !strings.Contains(out, want), "missing %q in %s", want, out)
			}
		}
	case FormatHuman, FormatGitHub, FormatGitLab, FormatGitea, FormatDiff, "":
		for _, want := range []string{"apiVersion: v1", "kind: ConfigMap", "name: hello", "namespace: apps", "greeting: same"} {
			{
				assert.ErrorIf(
					t,
					strings.Count(out, want) != 2,
					"want two whole bodies containing %q, got:\n%s",
					want,
					out,
				)
			}
		}
		if (*testCaseInput.format) == FormatDiff {
			{
				assert.ErrorIf(
					t,
					!strings.Contains(out, "-apiVersion: v1") || !strings.Contains(out, "+apiVersion: v1"),
					"missing removal/addition:\n%s",
					out,
				)
			}
		} else {
			assert.ErrorIf(
				t,
				!strings.Contains(out, "removed") || !strings.Contains(out, "added"),
				"missing removal/addition:\n%s",
				out,
			)
		}
	}
}

type testRenderDocsParentQualifiedMatchingSharedNativeLabelInputs struct {
	left  *Doc
	right *Doc
}

func testRenderDocsParentQualifiedMatchingSharedNativeLabel(
	t *testing.T,
	testCaseInput *testRenderDocsParentQualifiedMatchingSharedNativeLabelInputs,
) {
	t.Helper()

	old, current := ncm("hello", "greeting", "old"), ncm("hello", "greeting", "new")
	old.Parent, current.Parent = testCaseInput.left.Parent, testCaseInput.left.Parent
	out := renderDiff(
		t,
		[]Doc{old, (*testCaseInput.right)},
		[]Doc{current, (*testCaseInput.right)}, new(Options{Format: FormatGitHub}),
	)
	{
		assert.FatalIf(t, strings.Count(out, "@@ data.greeting @@") != 1 || !strings.Contains(out, "- old") ||
			!strings.Contains(out, "+ new"), "want only a's change, got:\n%s", out)
	}
}

func testRenderDocsNativeCanonicalEdgesNamespacePresence(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name    string
		present bool
		want    string
	}{
		{"absent", false, "v1/ConfigMap/hello"},
		{"empty", true, "v1/ConfigMap//hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			left, right := ncm("hello", "greeting", "old"), ncm("hello", "greeting", "new")
			for _, doc := range []Doc{left, right} {
				md := doc.Manifest["metadata"].(map[string]any)
				delete(md, "namespace")
				if tc.present {
					md["namespace"] = ""
				}
			}
			report, err := diffprep.Prepare(prepareDocs([]Doc{left}), prepareDocs([]Doc{right}), prepareOptions(new(Options{}))).
				Native()
			assert.NoError(t, err)
			assert.Diff(t, report.From.Names, []string{tc.want})
			assert.Diff(t, report.To.Names, []string{tc.want})
			assert.Equal(t, report.Diffs[0].Path.RootDescription(), tc.want)
		})
	}
}

func testRenderDocsNativeCanonicalEdgesSameGroupVersion(t *testing.T) {
	t.Helper()

	left, right := ncm("hello", "greeting", "same"), ncm("hello", "greeting", "same")
	left.Manifest["apiVersion"], right.Manifest["apiVersion"] = "example.test/v1", "example.test/v2"
	out := renderDiff(t, []Doc{left}, []Doc{right}, new(Options{Format: FormatGitHub}))
	const want = "\n@@ apiVersion @@\n! ± value change\n- example.test/v1\n+ example.test/v2\n\n"
	assert.Equal(t, out, want)
	changes := jsonChanges(t, []Doc{left}, []Doc{right}, new(Options{}))
	{
		assert.FatalIf(t, len(changes) != 1 || changes[0].Status != StatusChanged, "version change: %+v", changes)
	}
}

func testRenderDocsNativeCanonicalEdgesIncompleteNormalizedIdentity(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name        string
		left, right []Doc
		wantKinds   []rune
	}{
		{"rename", []Doc{ncm("old", "greeting", "same")}, []Doc{ncm("new", "greeting", "same")}, []rune{dyff.REMOVAL, dyff.ADDITION}},
		{"addition", nil, []Doc{ncm("new", "greeting", "same")}, []rune{dyff.ADDITION}},
		{"reordered matches", []Doc{ncm("z", "greeting", "old"), ncm("a", "greeting", "same")}, []Doc{ncm("a", "greeting", "same"), ncm("z", "greeting", "new")}, []rune{dyff.MODIFICATION}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := Options{Format: FormatGitHub, Normalize: func(m map[string]any) { delete(m, "apiVersion") }}
			report, err := diffprep.Prepare(prepareDocs(tc.left), prepareDocs(tc.right), prepareOptions(new(opts))).
				Native()
			assert.NoError(t, err)
			{
				assert.FatalIf(
					t,
					len(report.Diffs) != len(tc.wantKinds),
					"wrong canonical operations: %+v",
					report.Diffs,
				)
			}
			for i, kind := range tc.wantKinds {
				assert.Equal(t, report.Diffs[i].Details[0].Kind, kind)
			}
			out := renderDiff(t, tc.left, tc.right, new(opts))
			{
				assert.FatalIf(
					t,
					strings.Contains(out, "apiVersion") || strings.Contains(out, "@@ metadata.name @@"),
					"positional matching or normalization lost: %s",
					out,
				)
			}
			changes := jsonChanges(t, tc.left, tc.right, new(opts))
			{
				assert.FatalIf(t, len(changes) != len(tc.wantKinds), "native and JSON differ: %+v", changes)
			}
		})
	}
}

func testRenderDocsNativeCanonicalEdgesValidationBeforeEquality(t *testing.T) {
	t.Helper()

	value := make(chan int)
	bad := ncm("hello", "greeting", "same")
	bad.Manifest["bad"] = value
	for _, tc := range []struct {
		name        string
		left, right []Doc
		prefix      string
	}{
		{"equal channel", []Doc{bad}, []Doc{bad}, "marshal from: "},
		{"right channel", nil, []Doc{bad}, "marshal to: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, _, err := RenderDocs(
				tc.left,
				tc.right,
				Options{
					Format:     FormatGitHub,
					Suppressed: []Suppression{{Parent: helmReleaseParent(), Side: SideCurrent, Reason: "not ready"}},
				},
			)
			{
				assert.FatalIf(
					t,
					out != nil || err == nil || !strings.HasPrefix(err.Error(), tc.prefix),
					"RenderDocs = %q, %v; want nil and %s",
					out,
					err,
					tc.prefix,
				)
			}
		})
	}
}

func testRenderDocsNativeCanonicalEdgesEmptyDocuments(t *testing.T) {
	t.Helper()

	assert.Equal(t, renderDiff(t, []Doc{{}}, []Doc{{}}, new(Options{Format: FormatGitHub})), "")
}

type testRenderDocsNativeDuplicateEdgesCaseInputs struct {
	side      *string
	collision *bool
}

func testRenderDocsNativeDuplicateEdgesCase(t *testing.T, testCaseInput *testRenderDocsNativeDuplicateEdgesCaseInputs) {
	t.Helper()

	shadow, old, current := ncm(
		"hello",
		"greeting",
		"shadowed",
	), ncm(
		"hello",
		"greeting",
		"retained",
	), ncm(
		"hello",
		"greeting",
		"counterpart",
	)
	opts := Options{Format: FormatGitHub}
	if *testCaseInput.collision {
		shadow.Manifest["metadata"].(map[string]any)["name"] = "shadow"
		opts.Normalize = func(m map[string]any) { m["metadata"].(map[string]any)["name"] = "normalized" }
	}
	left, right := []Doc{shadow, shadow, old}, []Doc{current}
	if (*testCaseInput.side) == "right" {
		left, right = right, left
	}
	out := renderDiff(t, left, right, new(opts))
	{
		assert.FatalIf(
			t,
			strings.Count(out, "@@ data.greeting @@") != 1 || strings.Contains(out, "shadowed") ||
				!strings.Contains(out, "retained") ||
				!strings.Contains(out, "counterpart"),
			"wrong winning content:\n%s",
			out,
		)
	}
	_, warnings, err := RenderDocs(left, right, opts)
	assert.NoError(t, err)
	assert.Equal(t, len(warnings), 2)
	warningSide := SideOrig
	if (*testCaseInput.side) == "right" {
		warningSide = SideCurrent
	}
	{
		warningValues := warnings
		for warningIndex := range warningValues {
			warning := warningValues[warningIndex]
			assert.Equal(t, warning.Detail[0], "side: "+string(warningSide))
		}
	}
}
