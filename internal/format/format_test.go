package format

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestTable(t *testing.T) {
	var b bytes.Buffer
	cols := []Column{{"NAME", "name"}, {"PATH", "path"}}
	rows := []map[string]string{
		{"name": "apps", "path": "./apps"},
		{"name": "infra", "path": "./infrastructure"},
	}
	if err := Table(&b, cols, rows); err != nil {
		t.Fatalf("Table: %v", err)
	}
	got := b.String()
	if !strings.Contains(got, "NAME") || !strings.Contains(got, "PATH") {
		t.Errorf("missing headers: %s", got)
	}
	if !strings.Contains(got, "apps") || !strings.Contains(got, "./infrastructure") {
		t.Errorf("missing rows: %s", got)
	}
}

func TestYAMLMulti(t *testing.T) {
	var b bytes.Buffer
	if err := YAMLMulti(&b, []map[string]any{
		{"kind": "A", "metadata": map[string]any{"name": "1"}},
		{"kind": "B", "metadata": map[string]any{"name": "2"}},
	}); err != nil {
		t.Fatalf("YAMLMulti: %v", err)
	}
	out := b.String()
	if strings.Count(out, "---") != 2 {
		t.Errorf("expected 2 doc separators: %s", out)
	}
}

// TestYAMLMulti_DeterministicKeyOrder pins flate's core output-determinism
// contract: YAMLMulti marshals a map[string]any with SORTED keys (it routes
// through sigs.k8s.io/yaml, i.e. json.Marshal which sorts), so a Go map's
// randomized iteration order never leaks into rendered output. A
// non-sorting marshaler would flap across these runs (Go randomizes map
// iteration per range). This is why flate's STRUCTURED output is stable
// regardless of upstream (helm/kustomize) key order — and, with the verbatim
// guarantee below, why any embedded non-determinism must originate in opaque
// string VALUES (a chart's rendered JSON), not in flate's serialization.
func TestYAMLMulti_DeterministicKeyOrder(t *testing.T) {
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
		if err := YAMLMulti(&b, []map[string]any{doc}); err != nil {
			t.Fatalf("run %d: %v", n, err)
		}
		if n == 0 {
			first = b.String()
			continue
		}
		if b.String() != first {
			t.Fatalf("run %d: YAMLMulti output not deterministic across runs", n)
		}
	}
	// Sanity: keys are actually sorted (key00 precedes key39).
	if i0, i39 := strings.Index(first, "key00"), strings.Index(first, "key39"); i0 < 0 || i39 < 0 || i0 > i39 {
		t.Errorf("keys not sorted: key00@%d key39@%d", i0, i39)
	}
}

// TestYAMLMulti_PreservesEmbeddedJSONStringVerbatim documents the boundary
// for embedded JSON: a ConfigMap data value holding a JSON string (e.g. a
// Grafana dashboard) is an OPAQUE scalar — YAMLMulti emits the VALUE
// verbatim, never parsing it to reorder its internal keys or normalize
// trailing commas. So if such a string varies run-to-run, the variance
// originates upstream (the helm chart that produced it), not in flate's
// emit. Pins that flate must NOT "helpfully" re-serialize embedded JSON.
func TestYAMLMulti_PreservesEmbeddedJSONStringVerbatim(t *testing.T) {
	// Deliberately unsorted keys + a trailing comma — the exact shape a
	// non-deterministic dashboard render emits. flate must preserve it.
	embedded := "{\n  \"z\": 1,\n  \"a\": 2,\n  \"panels\": [\n    {\"color\": \"red\"},\n  ]\n}"
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "dash"},
		"data":       map[string]any{"dashboard.json": embedded},
	}
	var b bytes.Buffer
	if err := YAMLMulti(&b, []map[string]any{doc}); err != nil {
		t.Fatalf("YAMLMulti: %v", err)
	}
	var rt map[string]any
	if err := yaml.Unmarshal([]byte(strings.TrimPrefix(b.String(), "---\n")), &rt); err != nil {
		t.Fatalf("round-trip unmarshal: %v", err)
	}
	got, _ := rt["data"].(map[string]any)["dashboard.json"].(string)
	if got != embedded {
		t.Fatalf("embedded JSON string not preserved verbatim:\nwant %q\ngot  %q", embedded, got)
	}
}

func TestJSON(t *testing.T) {
	var b bytes.Buffer
	if err := JSON(&b, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if !strings.Contains(b.String(), `"k": "v"`) {
		t.Errorf("json output: %s", b.String())
	}
}
