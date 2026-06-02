package diff

import (
	"strings"
	"testing"
)

// ncm builds a fully-formed (apiVersion-bearing) ConfigMap so dyff's
// Kubernetes entity detection can derive its native identity label.
func ncm(name, key, val string) Doc {
	return Doc{Manifest: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": "apps"},
		"data":       map[string]any{key: val},
	}}
}

// TestRenderDocs_NativeLabels pins that the dyff text styles render the
// whole set through dyff with its native apiVersion/kind/namespace/name
// label — and never the flate parent header. (A second, unchanged
// resource is present so dyff emits the document-root label.)
func TestRenderDocs_NativeLabels(t *testing.T) {
	left := []Doc{ncm("hello", "greeting", "hola"), ncm("other", "k", "v")}
	right := []Doc{ncm("hello", "greeting", "hi"), ncm("other", "k", "v")}

	cases := []struct {
		format   Format
		contains []string
	}{
		{FormatGitHub, []string{"@@ data.greeting @@", "# v1/ConfigMap/apps/hello", "- hola", "+ hi"}},
		{FormatGitea, []string{"@@ data.greeting @@", "= v1/ConfigMap/apps/hello"}},
		{FormatGitLab, []string{"= data.greeting", "= v1/ConfigMap/apps/hello"}},
		{FormatHuman, []string{"v1/ConfigMap/apps/hello", "data.greeting"}},
		{FormatBrief, []string{"detected"}},
		{"", []string{"@@ data.greeting @@", "# v1/ConfigMap/apps/hello"}}, // default = github
	}
	for _, tc := range cases {
		name := string(tc.format)
		if name == "" {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			out, err := RenderDocs(left, right, Options{Format: tc.format})
			if err != nil {
				t.Fatalf("RenderDocs: %v", err)
			}
			s := string(out)
			if strings.Contains(s, "Child:") || strings.Contains(s, "HelmRelease:") {
				t.Errorf("native output must not carry the flate parent header:\n%s", s)
			}
			for _, want := range tc.contains {
				if !strings.Contains(s, want) {
					t.Errorf("%s output missing %q:\n%s", tc.format, want, s)
				}
			}
		})
	}
}

// TestRenderDocs_StructuredFormats pins that diff/yaml/json/markdown route
// through the per-resource Run+Render pipeline rather than the native path.
func TestRenderDocs_StructuredFormats(t *testing.T) {
	left := []Doc{ncm("hello", "greeting", "hola")}
	right := []Doc{ncm("hello", "greeting", "hi")}

	out, err := RenderDocs(left, right, Options{Format: FormatJSON})
	if err != nil {
		t.Fatalf("RenderDocs json: %v", err)
	}
	if !strings.Contains(string(out), `"kind": "ConfigMap"`) {
		t.Errorf("json should be the structured per-resource list:\n%s", out)
	}

	out, err = RenderDocs(left, right, Options{Format: FormatDiff})
	if err != nil {
		t.Fatalf("RenderDocs diff: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "--- ") || !strings.Contains(s, "@@ -") {
		t.Errorf("diff should be a unified diff:\n%s", s)
	}
}

func TestRenderDocs_NoChange(t *testing.T) {
	d := ncm("hello", "k", "v")
	out, err := RenderDocs([]Doc{d}, []Doc{d}, Options{Format: FormatGitHub})
	if err != nil {
		t.Fatalf("RenderDocs: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("no-change should render empty, got:\n%s", out)
	}
}
