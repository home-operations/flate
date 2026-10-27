package diffprep

import (
	"testing"

	"github.com/homeport/dyff/pkg/dyff"
	yamlv3 "go.yaml.in/yaml/v3"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func ncm(name, key, val string) Doc {
	return Doc{Manifest: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": "apps"},
		"data":       map[string]any{key: val},
	}}
}

func TestPrepared_NativeInputs(t *testing.T) {
	t.Parallel()
	leftParent := Parent{NamedResource: testutil.HelmReleaseID("apps", "a")}
	rightParent := Parent{NamedResource: testutil.HelmReleaseID("apps", "b")}
	left, right := ncm("hello", "greeting", "same"), ncm("hello", "greeting", "same")
	left.Parent, right.Parent = leftParent, rightParent
	t.Run("report bodies", func(t *testing.T) {
		t.Parallel()
		report, err := Prepare([]Doc{left}, []Doc{right}, Options{}).Native()
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
	t.Run("namespace labels", func(t *testing.T) {
		t.Parallel()
		absent, empty := ncm("cluster", "greeting", "old"), ncm("empty", "greeting", "old")
		delete(absent.Manifest["metadata"].(map[string]any), "namespace")
		empty.Manifest["metadata"].(map[string]any)["namespace"] = ""
		report, err := Prepare([]Doc{absent, empty}, nil, Options{}).Native()
		assert.NoError(t, err)
		assert.Equal(t, report.Diffs[0].Path.RootDescription(), "v1/ConfigMap/cluster")
		assert.Equal(t, report.Diffs[1].Path.RootDescription(), "v1/ConfigMap//empty")
	})
}

func TestPrepared_NativeOrder(t *testing.T) {
	t.Parallel()
	t.Run("duplicate winning order", func(t *testing.T) {
		t.Parallel()

		left := []Doc{ncm("a", "greeting", "shadowed"), ncm("z", "greeting", "old-z"), ncm("a", "greeting", "old-a")}
		right := []Doc{ncm("a", "greeting", "new-a"), ncm("z", "greeting", "new-z")}
		report, err := Prepare(left, right, Options{}).Native()
		assert.NoError(t, err)
		if len(report.Diffs) != 2 || len(report.From.Documents) != 2 {
			t.Fatalf("duplicate not removed: %+v", report)
		}
		assert.Equal(t, report.Diffs[0].Path.RootDescription(), "v1/ConfigMap/apps/z")
		assert.Equal(t, report.Diffs[1].Path.RootDescription(), "v1/ConfigMap/apps/a")
	})
	left := []Doc{ncm("b-removed", "greeting", "gone"), ncm("z-modified", "greeting", "old"), ncm("middle-unchanged", "greeting", "same"), ncm("a-modified", "greeting", "before")}
	right := []Doc{ncm("y-added", "greeting", "born"), ncm("a-modified", "greeting", "after"), left[2], ncm("z-modified", "greeting", "new")}

	report, err := Prepare(left, right, Options{}).Native()
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
