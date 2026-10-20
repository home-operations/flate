package diff

import (
	"bytes"
	"cmp"
	"html"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"
	yamlv3 "go.yaml.in/yaml/v3"

	"github.com/home-operations/flate/internal/assert"
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
			out := renderDiff(t, []Doc{left}, []Doc{right}, Options{Format: format})
			switch format {
			case FormatJSON:
				changes := jsonChanges(t, []Doc{left}, []Doc{right}, Options{})
				if len(changes) != 2 {
					t.Fatalf("changes = %+v", changes)
				}
				assert.Equal(t, changes[0].Parent, left.Parent)
				assert.Equal(t, changes[0].Status, StatusRemoved)
				assert.Diff(t, changes[0].Old, left.Manifest)
				assert.Equal(t, changes[1].Parent, right.Parent)
				assert.Equal(t, changes[1].Status, StatusAdded)
				assert.Diff(t, changes[1].New, right.Manifest)
			case FormatBrief:
				if !strings.Contains(out, "two changes detected") {
					t.Fatalf("want two changes, got %q", out)
				}
			case FormatHTML:
				cells := regexp.MustCompile(`(?s)<td class="diff-code diff-(del|add)">(.*?)</td>`).FindAllStringSubmatch(out, -1)
				bodies := map[string]string{}
				tags := regexp.MustCompile(`<[^>]*>`)
				for _, cell := range cells {
					bodies[cell[1]] += html.UnescapeString(tags.ReplaceAllString(cell[2], "")) + "\n"
				}
				const body = "apiVersion: v1\ndata:\n  greeting: same\nkind: ConfigMap\nmetadata:\n  name: hello\n  namespace: apps\n"
				assert.Equal(t, bodies["del"], body)
				assert.Equal(t, bodies["add"], body)
				for _, want := range []string{"1 added", "1 removed", "status-removed", "status-added", "apiVersion", "ConfigMap", "namespace", "hello", "greeting", "same"} {
					if !strings.Contains(out, want) {
						t.Errorf("missing %q in %s", want, out)
					}
				}
			default:
				for _, want := range []string{"apiVersion: v1", "kind: ConfigMap", "name: hello", "namespace: apps", "greeting: same"} {
					if strings.Count(out, want) != 2 {
						t.Errorf("want two whole bodies containing %q, got:\n%s", want, out)
					}
				}
				if format == FormatDiff {
					if !strings.Contains(out, "-apiVersion: v1") || !strings.Contains(out, "+apiVersion: v1") {
						t.Errorf("missing removal/addition:\n%s", out)
					}
				} else if !strings.Contains(out, "removed") || !strings.Contains(out, "added") {
					t.Errorf("missing removal/addition:\n%s", out)
				}
			}
		})
	}
	t.Run("report bodies", func(t *testing.T) {
		t.Parallel()
		report, err := nativeReport([]Doc{left}, []Doc{right}, Options{})
		assert.NoError(t, err)
		if len(report.Diffs) != 2 {
			t.Fatalf("want two report diffs, got %+v", report.Diffs)
		}
		for i, want := range []struct {
			kind rune
			doc  Doc
		}{{dyff.REMOVAL, left}, {dyff.ADDITION, right}} {
			d := report.Diffs[i]
			if len(d.Path.PathElements) != 0 || len(d.Details) != 1 || d.Details[0].Kind != want.kind {
				t.Fatalf("wrong root operation: %+v", d)
			}
			node := d.Details[0].From
			if want.kind == dyff.ADDITION {
				node = d.Details[0].To
			}
			if node.Kind != yamlv3.DocumentNode {
				t.Fatalf("want whole document, got %+v", node)
			}
			var got map[string]any
			assert.NoError(t, node.Decode(&got))
			assert.Diff(t, got, want.doc.Manifest)
		}
	})
	t.Run("shared native label", func(t *testing.T) {
		t.Parallel()
		old, current := ncm("hello", "greeting", "old"), ncm("hello", "greeting", "new")
		old.Parent, current.Parent = left.Parent, left.Parent
		out := renderDiff(t, []Doc{old, right}, []Doc{current, right}, Options{Format: FormatGitHub})
		if strings.Count(out, "@@ data.greeting @@") != 1 || !strings.Contains(out, "- old") || !strings.Contains(out, "+ new") {
			t.Fatalf("want only a's change, got:\n%s", out)
		}
	})
}

func TestRenderDocs_NativePresentation(t *testing.T) {
	t.Parallel()
	left := []Doc{ncm("b-removed", "greeting", "gone"), ncm("z-modified", "greeting", "old"), ncm("middle-unchanged", "greeting", "same"), ncm("a-modified", "greeting", "before")}
	right := []Doc{ncm("y-added", "greeting", "born"), ncm("a-modified", "greeting", "after"), left[2], ncm("z-modified", "greeting", "new")}
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
	note := Suppression{Parent: Parent{NamedResource: testutil.HelmReleaseID("apps", "owner")}, Side: SideCurrent, Count: 1, Reason: "not ready"}
	for _, tc := range []struct {
		format Format
		want   string
	}{
		{FormatGitHub, "! " + note.String() + "\n\n" + github},
		{FormatBrief, "⚠ " + note.String() + "\n\nfour changes detected between from, four documents and to, four documents\n\n"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			t.Parallel()
			opts := Options{Format: tc.format, Suppressed: []Suppression{note}}
			first := renderDiff(t, left, right, opts)
			assert.Equal(t, first, tc.want)
			assert.Equal(t, renderDiff(t, left, right, opts), first)
		})
	}
	t.Run("namespace labels", func(t *testing.T) {
		t.Parallel()
		absent, empty := ncm("cluster", "greeting", "old"), ncm("empty", "greeting", "old")
		delete(absent.Manifest["metadata"].(map[string]any), "namespace")
		empty.Manifest["metadata"].(map[string]any)["namespace"] = ""
		report, err := nativeReport([]Doc{absent, empty}, nil, Options{})
		assert.NoError(t, err)
		assert.Equal(t, report.Diffs[0].Path.RootDescription(), "v1/ConfigMap/cluster")
		assert.Equal(t, report.Diffs[1].Path.RootDescription(), "v1/ConfigMap//empty")
	})
	report, err := nativeReport(left, right, Options{})
	assert.NoError(t, err)
	if len(report.From.Documents) != 4 || len(report.To.Documents) != 4 || len(report.Diffs) != 4 {
		t.Fatalf("incomplete display context: %+v", report)
	}
	for i, want := range []struct {
		location string
		index    int
		path     string
	}{
		{"from", 1, "data.greeting"}, {"from", 3, "data.greeting"}, {"from", 0, ""}, {"to", 0, ""},
	} {
		path := report.Diffs[i].Path
		assert.Equal(t, path.Root.Location, want.location)
		assert.Equal(t, path.DocumentIdx, want.index)
		assert.Equal(t, path.ToDotStyle(), want.path)
		wantRoot := report.From.Documents[want.index]
		if want.location == "to" {
			wantRoot = report.To.Documents[want.index]
		}
		assert.Diff(t, path.Root.Documents[want.index], wantRoot)
	}
}

func TestRenderDocs_NativeCanonicalEdges(t *testing.T) {
	t.Parallel()
	t.Run("namespace presence", func(t *testing.T) {
		t.Parallel()
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
				report, err := nativeReport([]Doc{left}, []Doc{right}, Options{})
				assert.NoError(t, err)
				assert.Diff(t, report.From.Names, []string{tc.want})
				assert.Diff(t, report.To.Names, []string{tc.want})
				assert.Equal(t, report.Diffs[0].Path.RootDescription(), tc.want)
			})
		}
	})
	t.Run("same group version", func(t *testing.T) {
		t.Parallel()
		left, right := ncm("hello", "greeting", "same"), ncm("hello", "greeting", "same")
		left.Manifest["apiVersion"], right.Manifest["apiVersion"] = "example.test/v1", "example.test/v2"
		out := renderDiff(t, []Doc{left}, []Doc{right}, Options{Format: FormatGitHub})
		const want = "\n@@ apiVersion @@\n! ± value change\n- example.test/v1\n+ example.test/v2\n\n"
		assert.Equal(t, out, want)
		changes := jsonChanges(t, []Doc{left}, []Doc{right}, Options{})
		if len(changes) != 1 || changes[0].Status != StatusChanged {
			t.Fatalf("version change: %+v", changes)
		}
	})
	t.Run("incomplete normalized identity", func(t *testing.T) {
		t.Parallel()
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
				report, err := nativeReport(tc.left, tc.right, opts)
				assert.NoError(t, err)
				if len(report.Diffs) != len(tc.wantKinds) {
					t.Fatalf("wrong canonical operations: %+v", report.Diffs)
				}
				for i, kind := range tc.wantKinds {
					assert.Equal(t, report.Diffs[i].Details[0].Kind, kind)
				}
				out := renderDiff(t, tc.left, tc.right, opts)
				if strings.Contains(out, "apiVersion") || strings.Contains(out, "@@ metadata.name @@") {
					t.Fatalf("positional matching or normalization lost: %s", out)
				}
				changes := jsonChanges(t, tc.left, tc.right, opts)
				if len(changes) != len(tc.wantKinds) {
					t.Fatalf("native and JSON differ: %+v", changes)
				}
			})
		}
	})
	t.Run("validation before equality", func(t *testing.T) {
		t.Parallel()
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
				out, err := RenderDocs(tc.left, tc.right, Options{Format: FormatGitHub, Suppressed: []Suppression{{Parent: hrParent, Side: SideCurrent, Reason: "not ready"}}})
				if out != nil || err == nil || !strings.HasPrefix(err.Error(), tc.prefix) {
					t.Fatalf("RenderDocs = %q, %v; want nil and %s", out, err, tc.prefix)
				}
			})
		}
	})
	t.Run("empty documents", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, renderDiff(t, []Doc{{}}, []Doc{{}}, Options{Format: FormatGitHub}), "")
	})
}

func TestRenderDocs_NativeDuplicateEdges(t *testing.T) {
	for _, side := range []string{"left", "right"} {
		for _, collision := range []bool{false, true} {
			name := side + " duplicate"
			if collision {
				name += " normalized"
			}
			t.Run(name, func(t *testing.T) {
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
				t.Cleanup(func() { slog.SetDefault(previous) })
				shadow, old, current := ncm("hello", "greeting", "shadowed"), ncm("hello", "greeting", "retained"), ncm("hello", "greeting", "counterpart")
				opts := Options{Format: FormatGitHub}
				if collision {
					shadow.Manifest["metadata"].(map[string]any)["name"] = "shadow"
					opts.Normalize = func(m map[string]any) { m["metadata"].(map[string]any)["name"] = "normalized" }
				}
				left, right := []Doc{shadow, shadow, old}, []Doc{current}
				if side == "right" {
					left, right = right, left
				}
				out := renderDiff(t, left, right, opts)
				if strings.Count(out, "@@ data.greeting @@") != 1 || strings.Contains(out, "shadowed") || !strings.Contains(out, "retained") || !strings.Contains(out, "counterpart") {
					t.Fatalf("wrong winning content:\n%s", out)
				}
				if strings.Count(logs.String(), "level=WARN") != 2 || strings.Count(logs.String(), "side="+side) != 2 {
					t.Fatalf("want one warning per shadowed input: %s", logs.String())
				}
			})
		}
	}
	t.Run("duplicate winning order", func(t *testing.T) {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
		t.Cleanup(func() { slog.SetDefault(previous) })
		left := []Doc{ncm("a", "greeting", "shadowed"), ncm("z", "greeting", "old-z"), ncm("a", "greeting", "old-a")}
		right := []Doc{ncm("a", "greeting", "new-a"), ncm("z", "greeting", "new-z")}
		report, err := nativeReport(left, right, Options{})
		assert.NoError(t, err)
		if len(report.Diffs) != 2 || len(report.From.Documents) != 2 {
			t.Fatalf("duplicate not removed: %+v", report)
		}
		assert.Equal(t, report.Diffs[0].Path.RootDescription(), "v1/ConfigMap/apps/z")
		assert.Equal(t, report.Diffs[1].Path.RootDescription(), "v1/ConfigMap/apps/a")
	})
}
