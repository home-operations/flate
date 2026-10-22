package diff

import (
	"cmp"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
	t.Parallel()
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
		{"", []string{"v1/ConfigMap/apps/hello", "data.greeting"}}, // default = human
	}
	for _, tc := range cases {
		name := cmp.Or(string(tc.format), "default")
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, _, err := RenderDocs(left, right, Options{Format: tc.format})
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

func TestRenderDocs_NoChange(t *testing.T) {
	t.Parallel()
	d := ncm("hello", "k", "v")
	out, _, err := RenderDocs([]Doc{d}, []Doc{d}, Options{Format: FormatGitHub})
	if err != nil {
		t.Fatalf("RenderDocs: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("no-change should render empty, got:\n%s", out)
	}
}

func TestRenderDocs_UnsupportedFormat(t *testing.T) {
	t.Parallel()
	d := ncm("hello", "k", "v")
	out, _, err := RenderDocs([]Doc{d}, nil, Options{Format: "bogus"})
	if err == nil || err.Error() != `unsupported diff format "bogus"` || out != nil {
		t.Errorf("RenderDocs = %q, %v; want nil bytes and exact unsupported-format error", out, err)
	}
	d.Manifest["bad"] = func() {}
	for _, tc := range []struct {
		format Format
		prefix string
	}{
		{FormatHuman, "marshal from: "},
		{FormatGitHub, "marshal from: "},
		{FormatBrief, "marshal from: "},
		{FormatGitLab, "marshal from: "},
		{FormatGitea, "marshal from: "},
		{FormatDiff, "marshal: "},
		{FormatHTML, "marshal: "},
		{FormatJSON, ""},
		{Format("bogus"), "unsupported diff format"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			t.Parallel()
			out, warnings, err := RenderDocs([]Doc{d, d}, nil, Options{Format: tc.format,
				Suppressed: []Suppression{{Parent: hrParent, Side: SideCurrent, Reason: "chart not ready"}}})
			if len(warnings) != 1 {
				t.Fatalf("formatter error lost discovered warnings: %+v", warnings)
			}
			categories := map[string]bool{}
			for _, warning := range warnings {
				categories[warning.Category] = true
			}
			assert.Diff(t, categories, map[string]bool{manifest.WarnDuplicateDocument: true})
			if err == nil || !strings.HasPrefix(err.Error(), tc.prefix) || out != nil {
				t.Fatalf("RenderDocs = %q, %v; want nil bytes and %s error", out, err, tc.prefix)
			}
		})
	}
}

// ndeploy builds an apiVersion-bearing Deployment whose pod template lists
// the named containers in order, so dyff's K8s entity detection matches
// list entries by container name.
func ndeploy(order ...string) []Doc {
	img := map[string]string{"nginx": "nginx:1.20", "sidecar": "envoy:1.30", "init": "busybox:1.36"}
	containers := make([]any, 0, len(order))
	for _, n := range order {
		containers = append(containers, map[string]any{"name": n, "image": img[n]})
	}
	return []Doc{{Manifest: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "x", "namespace": "ns"},
		"spec": map[string]any{
			"template": map[string]any{"spec": map[string]any{"containers": containers}},
		},
	}}}
}

// TestRenderDocs_ContainerReorder locks the K8s-aware payoff: reordering
// containers by name yields an `⇆ order changed` marker, NOT a wall of
// per-line value churn like a text diff would.
func TestRenderDocs_ContainerReorder(t *testing.T) {
	t.Parallel()
	out, _, err := RenderDocs(ndeploy("nginx", "sidecar", "init"), ndeploy("init", "nginx", "sidecar"),
		Options{Format: FormatGitHub})
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, "order changed") {
		t.Errorf("expected 'order changed' marker; got:\n%s", s)
	}
	// dyff matched by container name and saw the values were identical,
	// so no image-value churn should appear.
	if strings.Contains(s, "image:") {
		t.Errorf("reorder produced spurious image-value churn:\n%s", s)
	}
}

// TestRenderDocs_ContainerByNameImageChange complements the reorder test:
// a named container's image change is reported at the by-name path, not
// by array index.
func TestRenderDocs_ContainerByNameImageChange(t *testing.T) {
	t.Parallel()
	mk := func(image string) []Doc {
		return []Doc{{Manifest: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata":   map[string]any{"name": "x", "namespace": "ns"},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{
				map[string]any{"name": "nginx", "image": image},
				map[string]any{"name": "sidecar", "image": "envoy:1.30"},
			}}}},
		}}}
	}
	out, _, err := RenderDocs(mk("nginx:1.20"), mk("nginx:1.21"), Options{Format: FormatGitHub})
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, "containers.nginx.image") {
		t.Errorf("expected by-name path (containers.nginx.image); got:\n%s", s)
	}
	if !strings.Contains(s, "- nginx:1.20") || !strings.Contains(s, "+ nginx:1.21") {
		t.Errorf("expected image value change lines; got:\n%s", s)
	}
}

// TestRenderDocs_RenameNoFileLevelOrderChange locks the changeset fix: renaming
// ONE resource (a remove of the old identity + an add of the new) keeps the
// document COUNT equal, which makes dyff raise a file-level "order changed" note
// listing every document — dragging untouched neighbors into a localized diff.
// flate emits documents in a deterministic sorted order, so that note is always
// an artifact of the add/remove (already reported) and is filtered out. The
// rename itself must still surface; the unchanged neighbors must not.
func TestRenderDocs_RenameNoFileLevelOrderChange(t *testing.T) {
	t.Parallel()
	// Equal count on both sides (3 == 3) is what arms dyff's order check; the
	// middle doc is renamed, the neighbors are byte-identical.
	left := []Doc{ncm("aaa", "k", "v"), ncm("plex", "k", "v"), ncm("zzz", "k", "v")}
	right := []Doc{ncm("aaa", "k", "v"), ncm("plex2", "k", "v"), ncm("zzz", "k", "v")}
	for _, style := range []Format{FormatHuman, FormatGitHub} {
		out, _, err := RenderDocs(left, right, Options{Format: style})
		if err != nil {
			t.Fatalf("%s: %v", style, err)
		}
		s := string(out)
		if !strings.Contains(s, "plex2") {
			t.Errorf("%s: the rename must surface (added plex2); got:\n%s", style, s)
		}
		if strings.Contains(s, "order changed") {
			t.Errorf("%s: a localized rename must not emit a file-level 'order changed':\n%s", style, s)
		}
		if strings.Contains(s, "aaa") || strings.Contains(s, "zzz") {
			t.Errorf("%s: untouched neighbors must not be dragged into the diff:\n%s", style, s)
		}
	}
}
