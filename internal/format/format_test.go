package format

import (
	"bytes"
	"strings"
	"testing"
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

func TestJSON(t *testing.T) {
	var b bytes.Buffer
	if err := JSON(&b, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if !strings.Contains(b.String(), `"k": "v"`) {
		t.Errorf("json output: %s", b.String())
	}
}
