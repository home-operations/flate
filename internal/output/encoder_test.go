package output

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
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
		{name: "yaml_map_nil", docs: []map[string]any{{"kind": "A"}, nil}, want: "---\nkind: A\n---\nnull\n"},
		{name: "yaml_empty_map", docs: []map[string]any{{}, {"kind": "A"}}, want: "---\n{}\n---\nkind: A\n"},
		{name: "yaml_nil_empty", docs: []map[string]any{nil, {}}, want: "---\nnull\n---\n{}\n"},
		{name: "yaml_keep_chomp_empty", docs: []map[string]any{{"text": "雪\n\n"}, {}}, want: "---\ntext: |+\n  雪\n\n---\n{}\n"},
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
			if err := (Encoder{Writer: &b, Format: tc.format}).ImageGroups([]ImageGroup{{Cluster: "entry", Images: images, Names: images}}); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Tables_Bytes(t *testing.T) {
	t.Parallel()
	cols := []Column{{Header: "NAME", Key: "name"}, {Header: "PATH", Key: "path"}}
	rows := []map[string]string{{"name": "éclair", "path": "./é"}, {"name": "雪", "path": "./雪"}, {"path": "./missing"}}
	var b bytes.Buffer
	if err := (Encoder{Writer: &b}).Tables([]Table{{Columns: cols, Rows: rows}}); err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, b.String(), "NAME      PATH\néclair    ./é\n雪         ./雪\n          ./missing\n")
	assert.Diff(t, cols, []Column{{Header: "NAME", Key: "name"}, {Header: "PATH", Key: "path"}})
	assert.Diff(t, rows, []map[string]string{{"name": "éclair", "path": "./é"}, {"name": "雪", "path": "./雪"}, {"path": "./missing"}})
	b.Reset()
	if err := (Encoder{Writer: &b}).Tables([]Table{{Columns: cols}}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, b.String(), "NAME    PATH\n")
	b.Reset()
	if err := (Encoder{Writer: &b}).Tables(nil); err != nil || b.Len() != 0 {
		t.Fatalf("empty sections: %q %v", &b, err)
	}
}

func TestEncoder_Tables_WriteFailures(t *testing.T) {
	t.Parallel()
	sections := []Table{
		{Columns: []Column{{Header: "NAME", Key: "name"}}, Rows: []map[string]string{{"name": "first"}}},
		{Columns: []Column{{Header: "IMAGE", Key: "image"}}, Rows: []map[string]string{{"image": "second:v1"}}},
		{Columns: []Column{{Header: "LATER", Key: "name"}}},
	}
	var full bytes.Buffer
	assert.NoError(t, (Encoder{Writer: &full}).Tables(sections))
	assert.Equal(t, full.String(), "NAME\nfirst\n\nIMAGE\nsecond:v1\n\nLATER\n")
	for _, tc := range []struct {
		name   string
		failAt int
		prefix string
	}{
		{"first_section", 1, ""},
		{"separator", 2, "NAME\nfirst\n"},
		{"second_section", 3, "NAME\nfirst\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := errors.New("table write failed")
			w := &sectionWriter{failAt: tc.failAt, err: want}
			if err := (Encoder{Writer: w}).Tables(sections); !errors.Is(err, want) {
				t.Fatalf("writer error = %v, want %v", err, want)
			}
			assert.Equal(t, w.calls, tc.failAt)
			assert.Equal(t, w.buf.String(), tc.prefix)
		})
	}
}

type sectionWriter struct {
	buf           bytes.Buffer
	calls, failAt int
	err           error
}

func (w *sectionWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, w.err
	}
	return w.buf.Write(p)
}

func TestEncoder_Names_Bytes(t *testing.T) {
	t.Parallel()
	want := errors.New("name write failed")
	if err := (Encoder{Writer: errorWriter{want}}).Names(slices.Values([]string{"demo"})); !errors.Is(err, want) {
		t.Fatalf("name writer error: %v", err)
	}
	var b bytes.Buffer
	assert.NoError(t, (Encoder{Writer: &b}).Names(slices.Values([]string{})))
	assert.Equal(t, b.String(), "")
	assert.NoError(t, (Encoder{Writer: &b}).Names(slices.Values([]string{"demo", "雪", ""})))
	assert.Equal(t, b.String(), "demo\n雪\n\n")
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
	groups := []ImageGroup{{Cluster: "a", Images: []string{"z:v1", "a:v1"}, Names: []string{"a: z:v1", "a: a:v1"}}, {Cluster: "b", Images: []string{"z:v1"}, Names: []string{"b: z:v1"}}}
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
		{name: "empty_added", groups: []ImageGroup{{Cluster: "a", Added: true}}, want: ""},
		{name: "removed_with_survivor", groups: []ImageGroup{{Cluster: "a", Removed: true, Images: []string{"old:v1"}, Names: []string{"a: old:v1"}}, {Cluster: "b", Images: []string{"new:v2"}, Names: []string{"b: new:v2"}}}, want: "a: old:v1\nb: new:v2\n"},
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
	if err := (Encoder{Writer: errorWriter{want}}).ImageGroups([]ImageGroup{{Cluster: "a", Added: true, Images: []string{"x"}, Names: []string{"x"}}}); !errors.Is(err, want) {
		t.Fatalf("image name write error = %v, want %v", err, want)
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
	for n := range 2 {
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
