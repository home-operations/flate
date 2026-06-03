package diff

import (
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func htmlDoc(t *testing.T, y string) Doc {
	t.Helper()
	var m map[string]any
	if err := yaml.Unmarshal([]byte(y), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return Doc{Manifest: m, Parent: Parent{Kind: "HelmRelease", Namespace: "apps", Name: "web"}}
}

func renderHTMLString(t *testing.T, left, right []Doc) string {
	t.Helper()
	out, err := RenderDocs(left, right, Options{Format: FormatHTML})
	if err != nil {
		t.Fatalf("RenderDocs(html): %v", err)
	}
	return string(out)
}

func TestRenderHTML_Changed(t *testing.T) {
	t.Parallel()
	from := htmlDoc(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web\n  namespace: apps\ndata:\n  replicas: \"2\"\n  level: info\n")
	to := htmlDoc(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web\n  namespace: apps\ndata:\n  replicas: \"3\"\n  level: info\n")
	out := renderHTMLString(t, []Doc{from}, []Doc{to})

	for _, want := range []string{
		"<!doctype html>",
		`<table class="view side">`,    // side-by-side view present
		`<table class="view unified">`, // unified view present
		`id="view-btn"`,                // side ⇄ unified toggle
		`id="theme-btn"`,               // light/dark toggle
		`class="tree-leaf"`,            // sidebar navigation tree
		"body.chroma.dark",             // dark-theme variables present
		".chroma.dark .nt",             // dark chroma token rule (dual-theme highlighting)
		`<span class="nt">`,            // chroma YAML key token → highlighting present
		".chroma.light .nt",            // light token rule is scoped so colors actually apply
		"status status-changed",        // changed chip
		`class="diff-code diff-del"`,   // removed line cell (side view)
		`class="diff-code diff-add"`,   // added line cell
		"1 changed",                    // summary count
		"ConfigMap apps/web",           // per-resource title
		"HelmRelease apps/web",         // parent attribution
	} {
		if !strings.Contains(out, want) {
			t.Errorf("changed-diff HTML missing %q", want)
		}
	}
}

func TestRenderHTML_AddedAndRemoved(t *testing.T) {
	t.Parallel()
	added := htmlDoc(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: added\n  namespace: apps\ndata:\n  k: v\n")
	removed := htmlDoc(t, "apiVersion: v1\nkind: Service\nmetadata:\n  name: removed\n  namespace: apps\nspec:\n  ports:\n    - port: 80\n")
	out := renderHTMLString(t, []Doc{removed}, []Doc{added})

	for _, want := range []string{
		"status status-added", "status status-removed",
		"ConfigMap apps/added", "Service apps/removed",
		"0 changed", "1 added", "1 removed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("added/removed HTML missing %q", want)
		}
	}
}

func TestRenderHTML_Identical(t *testing.T) {
	t.Parallel()
	d := htmlDoc(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web\n  namespace: apps\ndata:\n  k: v\n")
	out := renderHTMLString(t, []Doc{d}, []Doc{d})

	if len(out) != 0 {
		t.Errorf("identical docs should render empty, got:\n%s", out)
	}
}
