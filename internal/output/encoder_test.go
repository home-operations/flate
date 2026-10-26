package output

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"sigs.k8s.io/yaml"
)

func TestEncoder_Documents_Bytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		format Format
		docs   []map[string]any
		want   string
	}{
		{name: "yaml_order_empty_document", docs: []map[string]any{{"kind": "B"}, {"kind": "A"}, {}}, want: "---\nkind: B\n---\nkind: A\n---\n{}\n"},
		{name: "yaml_empty_stream", format: FormatYAML},
		{name: "json_nil", format: FormatJSON, want: "null\n"},
		{name: "json_empty", format: FormatJSON, docs: []map[string]any{}, want: "[]\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Documents(tc.docs); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
			b.Reset()
			if err := (Encoder{Writer: &b, Format: tc.format}).DocumentGroups([]DocumentGroup{{Cluster: "entry", Documents: tc.docs}}); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Images_Bytes(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("name write failed")
	if err := (Encoder{Writer: errorWriter{wantErr}, Format: FormatName}).Images([]string{"image:v1"}); !errors.Is(err, wantErr) {
		t.Fatalf("image writer error: %v", err)
	}
	var empty bytes.Buffer
	if err := (Encoder{Writer: &empty}).Images(nil); err != nil || empty.Len() != 0 {
		t.Fatalf("empty images: %q %v", &empty, err)
	}
	var single bytes.Buffer
	if err := (Encoder{Writer: &single}).Images([]string{"ghcr.io/demo/app:v1"}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, single.String(), "ghcr.io/demo/app:v1\n")
	images := []string{"雪:v2", "alpha:v1"}
	for _, tc := range []struct {
		format Format
		want   string
	}{
		{want: "雪:v2\nalpha:v1\n"},
		{format: FormatYAML, want: "- 雪:v2\n- alpha:v1\n"},
		{format: FormatJSON, want: "[\n  \"雪:v2\",\n  \"alpha:v1\"\n]\n"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Images(images); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
			b.Reset()
			if err := (Encoder{Writer: &b, Format: tc.format}).ImageGroups([]ImageGroup{{Cluster: "entry", Images: images}}); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Cluster_Bytes(t *testing.T) {
	t.Parallel()
	summary := map[string]any{"kustomizations": 2, "helmReleases": 1}
	for _, tc := range []struct {
		format Format
		want   string
	}{
		{want: "helmReleases: 1\nkustomizations: 2\n"},
		{format: FormatJSON, want: "{\n  \"helmReleases\": 1,\n  \"kustomizations\": 2\n}\n"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Cluster(summary); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
			b.Reset()
			if err := (Encoder{Writer: &b, Format: tc.format}).ClusterGroups([]SummaryGroup{{Cluster: "entry", Summary: summary}}); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Resources_Bytes(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("name write failed")
	if err := (Encoder{Writer: errorWriter{wantErr}, Format: FormatName}).Resources(nil, []map[string]string{{"name": "demo"}}, nil); !errors.Is(err, wantErr) {
		t.Fatalf("resource writer error: %v", err)
	}
	var empty bytes.Buffer
	if err := (Encoder{Writer: &empty, Format: FormatName}).Resources(nil, nil, nil); err != nil || empty.Len() != 0 {
		t.Fatalf("empty resources: %q %v", &empty, err)
	}
	var single bytes.Buffer
	if err := (Encoder{Writer: &single, Format: FormatName}).Resources(nil, []map[string]string{{"name": "demo"}}, nil); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, single.String(), "demo\n")
	cols := []Column{{Header: "NAME", Key: "name"}, {Header: "PATH", Key: "path"}}
	rows := []map[string]string{
		{"name": "éclair", "path": "./é"},
		{"name": "雪", "path": "./雪"},
		{"path": "./missing"},
	}
	docs := []map[string]any{{"kind": "A"}}
	for _, tc := range []struct {
		format Format
		want   string
	}{
		{want: "NAME      PATH\néclair    ./é\n雪         ./雪\n          ./missing\n"},
		{format: FormatName, want: "éclair\n雪\n\n"},
		{format: FormatYAML, want: "---\nkind: A\n"},
		{format: FormatJSON, want: "[\n  {\n    \"kind\": \"A\"\n  }\n]\n"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Resources(cols, rows, docs); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
			b.Reset()
			if err := (Encoder{Writer: &b, Format: tc.format}).ResourceGroups(cols, []ResourceGroup{{Cluster: "entry", Rows: rows, Documents: docs}}); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Documents_GroupBytes(t *testing.T) {
	t.Parallel()
	groups := []DocumentGroup{{Cluster: "a", Documents: []map[string]any{{"kind": "B"}, {"kind": "A"}}}, {Cluster: "b", Documents: []map[string]any{}}}
	for _, tc := range []struct {
		name   string
		format Format
		want   string
	}{
		{name: "yaml", format: FormatYAML, want: "# Cluster: a\n---\nkind: B\n---\nkind: A\n# Cluster: b\n"},
		{name: "json", format: FormatJSON, want: "[\n  {\n    \"cluster\": \"a\",\n    \"documents\": [\n      {\n        \"kind\": \"B\"\n      },\n      {\n        \"kind\": \"A\"\n      }\n    ]\n  },\n  {\n    \"cluster\": \"b\",\n    \"documents\": []\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).DocumentGroups(groups); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Images_GroupBytes(t *testing.T) {
	t.Parallel()
	groups := []ImageGroup{{Cluster: "a", Images: []string{"z:v1", "a:v1"}}, {Cluster: "b", Images: []string{"z:v1"}}}
	for _, tc := range []struct {
		name   string
		format Format
		want   string
	}{
		{name: "plain", want: "a: z:v1\na: a:v1\nb: z:v1\n"},
		{name: "yaml", format: FormatYAML, want: "- cluster: a\n  images:\n  - z:v1\n  - a:v1\n- cluster: b\n  images:\n  - z:v1\n"},
		{name: "json", format: FormatJSON, want: "[\n  {\n    \"cluster\": \"a\",\n    \"images\": [\n      \"z:v1\",\n      \"a:v1\"\n    ]\n  },\n  {\n    \"cluster\": \"b\",\n    \"images\": [\n      \"z:v1\"\n    ]\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).ImageGroups(groups); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Images_GroupPresenceBytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		format Format
		groups []ImageGroup
		want   string
	}{
		{name: "empty_added", groups: []ImageGroup{{Cluster: "a", Added: true}}, want: "Cluster: a (added)\n"},
		{name: "removed_with_survivor", groups: []ImageGroup{{Cluster: "a", Removed: true, Images: []string{"old:v1"}}, {Cluster: "b", Images: []string{"new:v2"}}}, want: "Cluster: a (removed)\na: old:v1\nb: new:v2\n"},
		{name: "yaml_removed", format: FormatYAML, groups: []ImageGroup{{Cluster: "a", Removed: true, Images: []string{}}}, want: "- cluster: a\n  images: []\n  removed: true\n"},
		{name: "json_added", format: FormatJSON, groups: []ImageGroup{{Cluster: "a", Added: true, Images: []string{}}}, want: "[\n  {\n    \"cluster\": \"a\",\n    \"images\": [],\n    \"added\": true\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).ImageGroups(tc.groups); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
	want := errors.New("write failed")
	if err := (Encoder{Writer: errorWriter{want}}).ImageGroups([]ImageGroup{{Cluster: "a", Added: true}}); !errors.Is(err, want) {
		t.Fatalf("image presence header error = %v, want %v", err, want)
	}
}

func TestEncoder_Cluster_GroupBytes(t *testing.T) {
	t.Parallel()
	groups := []SummaryGroup{{Cluster: "a", Summary: map[string]any{"name": "same"}}, {Cluster: "b", Summary: map[string]any{"name": "same"}}}
	for _, tc := range []struct {
		name   string
		format Format
		want   string
	}{
		{name: "yaml", format: FormatYAML, want: "- cluster: a\n  summary:\n    name: same\n- cluster: b\n  summary:\n    name: same\n"},
		{name: "json", format: FormatJSON, want: "[\n  {\n    \"cluster\": \"a\",\n    \"summary\": {\n      \"name\": \"same\"\n    }\n  },\n  {\n    \"cluster\": \"b\",\n    \"summary\": {\n      \"name\": \"same\"\n    }\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).ClusterGroups(groups); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Resources_GroupBytes(t *testing.T) {
	t.Parallel()
	cols := []Column{{Header: "NAME", Key: "name"}}
	row := map[string]string{"name": "same"}
	doc := map[string]any{"kind": "A"}
	groups := []ResourceGroup{{Cluster: "a", Rows: []map[string]string{row}, Documents: []map[string]any{doc}}, {Cluster: "b", Rows: []map[string]string{row}, Documents: []map[string]any{doc}}}
	for _, tc := range []struct {
		name   string
		format Format
		want   string
	}{
		{name: "table", format: FormatTable, want: "CLUSTER    NAME\na          same\nb          same\n"},
		{name: "name", format: FormatName, want: "a:same\nb:same\n"},
		{name: "yaml", format: FormatYAML, want: "# Cluster: a\n---\nkind: A\n# Cluster: b\n---\nkind: A\n"},
		{name: "json", format: FormatJSON, want: "[\n  {\n    \"cluster\": \"a\",\n    \"documents\": [\n      {\n        \"kind\": \"A\"\n      }\n    ]\n  },\n  {\n    \"cluster\": \"b\",\n    \"documents\": [\n      {\n        \"kind\": \"A\"\n      }\n    ]\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).ResourceGroups(cols, groups); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
			assert.Diff(t, row, map[string]string{"name": "same"})
			assert.Diff(t, doc, map[string]any{"kind": "A"})
			assert.Diff(t, cols, []Column{{Header: "NAME", Key: "name"}})
		})
	}
}

func TestEncoder_JSON_Bytes(t *testing.T) {
	t.Parallel()
	var b bytes.Buffer
	if err := (Encoder{Writer: &b}).JSON(map[string]string{"k": "v", "text": "<>&\"\n雪"}); err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, b.String(), "{\n  \"k\": \"v\",\n  \"text\": \"\\u003c\\u003e\\u0026\\\"\\n雪\"\n}\n")
}

func TestEncoder_Documents_DeterministicKeyOrder(t *testing.T) {
	t.Parallel()
	spec := map[string]any{
		"nested": map[string]any{"z": 1, "a": 2, "m": 3},
		"list":   []any{"c", "b", "a"},
	}
	for i := range 40 {
		spec[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("val-%d", i)
	}
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "x", "namespace": "y"},
		"spec":       spec,
	}
	var first string
	for n := range 200 {
		var b bytes.Buffer
		if err := (Encoder{Writer: &b, Format: FormatYAML}).Documents([]map[string]any{doc}); err != nil {
			t.Fatalf("run %d: %v", n, err)
		}
		if n == 0 {
			first = b.String()
			continue
		}
		if b.String() != first {
			t.Fatalf("run %d: output differs from the first run", n)
		}
	}
	if i0, i39 := strings.Index(first, "key00"), strings.Index(first, "key39"); i0 < 0 || i39 < 0 || i0 > i39 {
		t.Errorf("keys not sorted: key00@%d key39@%d", i0, i39)
	}
}

func TestEncoder_Documents_PreservesEmbeddedJSONStringVerbatim(t *testing.T) {
	t.Parallel()
	// Embedded JSON is opaque, including unsorted keys and a trailing comma.
	embedded := "{\n  \"z\": 1,\n  \"a\": 2,\n  \"panels\": [\n    {\"color\": \"red\"},\n  ]\n}"
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "dash"},
		"data":       map[string]any{"dashboard.json": embedded},
	}
	var b bytes.Buffer
	e := Encoder{Writer: &b, Format: FormatYAML}
	if err := e.Documents([]map[string]any{doc}); err != nil {
		t.Fatal(err)
	}
	var rt map[string]any
	if err := yaml.Unmarshal([]byte(strings.TrimPrefix(b.String(), "---\n")), &rt); err != nil {
		t.Fatalf("round-trip unmarshal: %v", err)
	}
	got, _ := rt["data"].(map[string]any)["dashboard.json"].(string)
	assert.Diff(t, got, embedded)
}
