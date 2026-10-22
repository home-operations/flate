package diff

import (
	"encoding/json"
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

func jsonChanges(t *testing.T, left, right []Doc, opts Options) []Change {
	t.Helper()
	opts.Format = FormatJSON
	out, _, err := RenderDocs(left, right, opts)
	assert.NoError(t, err)
	var report struct {
		Changes []Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(out, &report))
	return report.Changes
}

func TestChanges_Classification(t *testing.T) {
	t.Parallel()
	p := Parent{NamedResource: testutil.KustomizationID("flux-system", "apps")}
	left := []Doc{
		cmDoc(p, manifest.NamedResource{Name: "keep", Namespace: "ns"}, "same", ""),
		cmDoc(p, manifest.NamedResource{Name: "edit", Namespace: "ns"}, "old", ""),
		cmDoc(p, manifest.NamedResource{Name: "gone", Namespace: "ns"}, "x", "gone-1"),
	}
	right := []Doc{
		cmDoc(p, manifest.NamedResource{Name: "keep", Namespace: "ns"}, "same", ""),
		cmDoc(p, manifest.NamedResource{Name: "edit", Namespace: "ns"}, "new", ""),
		cmDoc(p, manifest.NamedResource{Name: "fresh", Namespace: "ns"}, "y", "fresh-2"),
	}
	opts := Options{StripAttrs: DefaultStripAttrs, Normalize: func(m map[string]any) {
		m["apiVersion"] = "v2"
		meta := m["metadata"].(map[string]any)
		meta["name"] = meta["name"].(string) + "-normalized"
		meta["namespace"] = "normalized-ns"
	}}
	changes, _ := Changes(left, right, opts)
	assert.Diff(t, jsonChanges(t, left, right, opts), changes)
	got := map[string]ChangeStatus{}
	for _, c := range changes {
		got[c.Name] = c.Status
		present := c.New
		if c.Status == StatusRemoved {
			present = c.Old
			assert.Equal(t, c.New == nil, true)
			assert.Equal(t, c.NewChart, "")
			assert.Equal(t, c.OldChart, "gone-1")
		}
		if c.Status == StatusAdded {
			assert.Equal(t, c.Old == nil, true)
			assert.Equal(t, c.OldChart, "")
			assert.Equal(t, c.NewChart, "fresh-2")
		}
		name, ns := manifest.DocMetadata(present)
		assert.Equal(t, c.Name, name)
		assert.Equal(t, c.Namespace, ns)
		assert.Equal(t, c.Kind, manifest.DocKind(present))
		assert.Equal(t, c.APIVersion, "v2")
		assert.Equal(t, c.APIVersion, manifest.DocAPIVersion(present))
	}
	assert.Diff(t, got, map[string]ChangeStatus{"edit-normalized": StatusChanged, "gone-normalized": StatusRemoved, "fresh-normalized": StatusAdded})
}

func TestChanges_ReturnedManifestsIndependent(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                    string
		status                  ChangeStatus
		opts                    Options
		leftBinary, rightBinary bool
	}{
		{name: "changed", status: StatusChanged, opts: Options{}},
		{name: "added", status: StatusAdded, opts: Options{}},
		{name: "removed", status: StatusRemoved, opts: Options{}},
		{name: "normalized options", status: StatusChanged, opts: Options{Normalize: func(m map[string]any) {
			m["data"].(map[string]any)["normalized"] = "yes"
		}}},
		{name: "left binaryData", status: StatusChanged, opts: Options{}, leftBinary: true},
		{name: "right binaryData", status: StatusChanged, opts: Options{}, rightBinary: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := Parent{NamedResource: testutil.KustomizationID("ns", "apps")}
			doc := func(name, value string, binary bool) Doc {
				d := cmDoc(p, manifest.NamedResource{Name: name, Namespace: "ns"}, value, "")
				d.Manifest["metadata"].(map[string]any)["ownerReferences"] = []any{
					map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "first", "uid": "first"},
					map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "second", "uid": "second"},
				}
				if binary {
					d.Manifest["binaryData"] = map[string]any{}
				}
				return d
			}
			inputs := func() ([]Doc, []Doc) {
				left := []Doc{doc("keep", "same", false)}
				right := []Doc{doc("keep", "same", false)}
				if tt.status != StatusAdded {
					left = append(left, doc("edit", "old", tt.leftBinary))
				}
				if tt.status != StatusRemoved {
					right = append(right, doc("edit", "new", tt.rightBinary))
				}
				return left, right
			}
			left, right := inputs()
			wantLeft, wantRight := inputs()
			want := []Change{{Parent: p, APIVersion: "v1", Kind: "ConfigMap", Namespace: "ns", Name: "edit", Status: tt.status}}
			if tt.status != StatusAdded {
				want[0].Old = doc("edit", "old", tt.leftBinary).Manifest
			}
			if tt.status != StatusRemoved {
				want[0].New = doc("edit", "new", tt.rightBinary).Manifest
			}
			if tt.opts.Normalize != nil {
				want[0].Old["data"].(map[string]any)["normalized"] = "yes"
				want[0].New["data"].(map[string]any)["normalized"] = "yes"
			}
			got, _ := Changes(left, right, tt.opts)
			assert.Diff(t, got, want)
			if len(got) != 1 {
				t.Fatalf("want one change with unchanged resource omitted, got %d", len(got))
			}
			assert.Equal(t, got[0].Old == nil, tt.status == StatusAdded)
			assert.Equal(t, got[0].New == nil, tt.status == StatusRemoved)
			for _, body := range []map[string]any{got[0].Old, got[0].New} {
				if body == nil {
					continue
				}
				body["data"].(map[string]any)["k"] = "mutated"
				refs := body["metadata"].(map[string]any)["ownerReferences"].([]any)
				refs[0].(map[string]any)["name"] = "mutated-owner"
				refs[1] = map[string]any{"name": "replaced-owner"}
			}
			assert.Diff(t, left, wantLeft)
			assert.Diff(t, right, wantRight)
			again, _ := Changes(left, right, tt.opts)
			assert.Diff(t, again, want)
		})
	}
}

func TestChanges_StripDropsNoiseOnlyChange(t *testing.T) {
	t.Parallel()
	p := Parent{NamedResource: testutil.HelmReleaseID("ns", "app")}
	left := []Doc{cmDoc(p, manifest.NamedResource{Name: "x", Namespace: "ns"}, "same", "app-1.0.0")}
	right := []Doc{cmDoc(p, manifest.NamedResource{Name: "x", Namespace: "ns"}, "same", "app-2.0.0")}
	opts := Options{StripAttrs: DefaultStripAttrs}
	changes, warnings := Changes(left, right, opts)
	assert.Equal(t, len(changes), 0)
	assert.Equal(t, len(warnings), 0)
	assert.Equal(t, len(jsonChanges(t, left, right, opts)), 0)
}

func TestChanges_CapturesChartBeforeStrip(t *testing.T) {
	t.Parallel()
	for _, replacement := range []string{"", "replacement-3"} {
		t.Run("replacement="+replacement, func(t *testing.T) {
			t.Parallel()
			p := Parent{NamedResource: testutil.HelmReleaseID("ns", "app")}
			left := []Doc{cmDoc(p, manifest.NamedResource{Name: "old", Namespace: "ns"}, "old", "app-1.0.0")}
			right := []Doc{cmDoc(p, manifest.NamedResource{Name: "new", Namespace: "ns"}, "new", "app-2.0.0")}
			opts := Options{StripAttrs: DefaultStripAttrs, Normalize: func(m map[string]any) {
				meta := m["metadata"].(map[string]any)
				meta["name"] = "normalized"
				if replacement != "" {
					meta["labels"] = map[string]any{helmChartLabel: replacement}
				}
			}}
			got, _ := Changes(left, right, opts)
			if len(got) != 1 {
				t.Fatalf("want 1 change, got %d: %+v", len(got), got)
			}
			assert.Diff(t, jsonChanges(t, left, right, opts), got)
			c := got[0]
			assert.Equal(t, c.Status, StatusChanged)
			assert.Equal(t, c.Name, "normalized")
			assert.Equal(t, c.OldChart, "app-1.0.0")
			assert.Equal(t, c.NewChart, "app-2.0.0")
			for _, body := range []map[string]any{c.Old, c.New} {
				assert.Equal(t, body["metadata"].(map[string]any)["name"].(string), "normalized")
				assert.Equal(t, chartLabel(body), replacement)
			}
			assert.Equal(t, c.Old["data"].(map[string]any)["k"].(string), "old")
			assert.Equal(t, c.New["data"].(map[string]any)["k"].(string), "new")
		})
	}
}

// TestChanges_NormalizeHookSuppressesChange is the konflate use case: a
// render-random Secret value reads as a change by default, but a redaction
// hook constant-izes it so it no longer does.
func TestChanges_NormalizeHookSuppressesChange(t *testing.T) {
	t.Parallel()
	p := Parent{NamedResource: testutil.KustomizationID("ns", "apps")}
	secret := func(val string) Doc {
		return Doc{
			Parent: p,
			Manifest: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"metadata": map[string]any{"name": "s", "namespace": "ns"},
				"data":     map[string]any{"token": val},
			},
		}
	}
	left := []Doc{secret("AAAA")}
	right := []Doc{secret("BBBB")}

	if got, _ := Changes(left, right, Options{}); len(got) != 1 {
		t.Fatalf("without redaction the value change must show; got %d", len(got))
	}
	redact := func(m map[string]any) {
		if m["kind"] != "Secret" {
			return
		}
		if d, ok := m["data"].(map[string]any); ok {
			for k := range d {
				d[k] = "<redacted>"
			}
		}
	}
	if got, _ := Changes(left, right, Options{Normalize: redact}); len(got) != 0 {
		t.Fatalf("redaction hook should suppress the render-random change; got %+v", got)
	}
}

func TestChanges_Sorted(t *testing.T) {
	t.Parallel()
	pa := Parent{NamedResource: testutil.KustomizationID("ns", "a")}
	pb := Parent{NamedResource: testutil.KustomizationID("ns", "b")}
	left := []Doc{cmDoc(pb, manifest.NamedResource{Name: "z", Namespace: "ns"}, "1", ""), cmDoc(pa, manifest.NamedResource{Name: "y", Namespace: "ns"}, "1", "")}
	right := []Doc{cmDoc(pb, manifest.NamedResource{Name: "z", Namespace: "ns"}, "2", ""), cmDoc(pa, manifest.NamedResource{Name: "y", Namespace: "ns"}, "2", "")}
	foreign := cmDoc(pa, manifest.NamedResource{Name: "y", Namespace: "ns"}, "3", "")
	foreign.Manifest["apiVersion"] = "example.test/v1"
	left = append(left, foreign)
	opts := Options{Normalize: func(m map[string]any) {
		meta := m["metadata"].(map[string]any)
		meta["name"] = meta["name"].(string) + "-normalized"
	}}
	got, _ := Changes(left, right, opts)
	again, _ := Changes(left, right, opts)
	assert.Diff(t, again, got)
	opts.Format = FormatJSON
	first, _, err := RenderDocs(left, right, opts)
	assert.NoError(t, err)
	second, _, err := RenderDocs(left, right, opts)
	assert.NoError(t, err)
	assert.Equal(t, string(second), string(first))
	if len(got) != 3 || got[0].Parent != pa || got[1].Parent != pa || got[2].Parent != pb ||
		got[0].APIVersion != "v1" || got[1].APIVersion != "example.test/v1" {
		t.Fatalf("changes not sorted by canonical parent and resource identity: %+v", got)
	}
}

func TestChanges_DuplicateDocumentsLastWins(t *testing.T) {
	t.Parallel()
	p := Parent{NamedResource: testutil.KustomizationID("ns", "apps")}
	left := []Doc{cmDoc(p, manifest.NamedResource{Name: "first-left", Namespace: "ns"}, "shadowed", "shadowed-1"), cmDoc(p, manifest.NamedResource{Name: "last-left", Namespace: "ns"}, "retained", "retained-1")}
	for _, tt := range []struct {
		name string
		last string
		want int
	}{
		{name: "shadowed change", last: "retained", want: 0},
		{name: "retained change", last: "changed", want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			right := []Doc{cmDoc(p, manifest.NamedResource{Name: "first-right", Namespace: "ns"}, "changed-shadowed", "shadowed-2"), cmDoc(p, manifest.NamedResource{Name: "last-right", Namespace: "ns"}, tt.last, "retained-2")}
			calls := map[string]int{}
			opts := Options{StripAttrs: DefaultStripAttrs, Normalize: func(m map[string]any) {
				meta := m["metadata"].(map[string]any)
				calls[meta["name"].(string)]++
				meta["name"] = "normalized"
			}}
			got, warnings := Changes(left, right, opts)
			assert.Equal(t, len(warnings), 2)
			for _, warning := range warnings {
				assert.Equal(t, warning.Category, manifest.WarnDuplicateDocument)
			}
			wantCalls := map[string]int{"first-left": 1, "last-left": 1, "first-right": 1, "last-right": 1}
			assert.Diff(t, calls, wantCalls)
			calls = map[string]int{}
			assert.Diff(t, jsonChanges(t, left, right, opts), got)
			assert.Diff(t, calls, wantCalls)
			calls = map[string]int{}
			lineCount := 0
			paired, _, _, _ := pair(left, right, opts)
			for _, err := range changedPairs(paired) {
				assert.NoError(t, err)
				lineCount++
			}
			assert.Equal(t, lineCount, tt.want)
			assert.Diff(t, calls, wantCalls)
			if len(got) != tt.want {
				t.Fatalf("expected %d changes, got %+v", tt.want, got)
			}
			if tt.want > 0 {
				c := got[0]
				assert.Equal(t, c.Status, StatusChanged)
				assert.Equal(t, c.Name, "normalized")
				assert.Equal(t, c.Old["data"].(map[string]any)["k"].(string), "retained")
				assert.Equal(t, c.New["data"].(map[string]any)["k"].(string), tt.last)
				assert.Equal(t, c.OldChart, "retained-1")
				assert.Equal(t, c.NewChart, "retained-2")
			}
		})
	}
}
