package diff

import (
	"fmt"
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
		`id="filter"`,                  // sidebar resource filter box
		`class="wd"`,                   // word-level intra-line highlight (the "2"→"3" change)
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
		// Column widths must live on <col>: the tables are table-layout:fixed,
		// whose column sizing comes from the first row — usually a colspan
		// expander — so a width on the line-number cells alone collapses every
		// column to an equal share. The <colgroup> keeps the gutters narrow.
		`<table class="view side"><colgroup><col class="cn">`,
		`<table class="view unified"><colgroup><col class="cn">`,
		"col.cn { width: 48px; }",
		// Responsive chrome: draggable sidebar splitter, mobile drawer toggle,
		// and the breakpoint that turns the sidebar into that drawer.
		`id="splitter"`,
		`id="menu-btn"`,
		"@media (max-width: 768px)",
		// Desktop sidebar collapse + the draggable side-by-side divider.
		"@media (min-width: 769px)",
		`class="side-wrap"`,
		`<div class="vsplit"`,
		`<col class="cl">`,
		// Status-filter chips toggle each status in the tree (zero-count
		// statuses render disabled); leaves carry their status for the filter.
		`class="count c-changed" data-status="changed" aria-pressed="true">`,
		`class="count c-added" data-status="added" aria-pressed="true" disabled>`,
		`data-id="r0" data-status="changed"`,
		// Title + parent share one sticky header block.
		`<div class="rtop">`,
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

func TestRenderHTML_FoldsContext(t *testing.T) {
	t.Parallel()
	// A single change surrounded by many unchanged lines: the context beyond 3
	// lines is collapsed behind expanders, but the dropped lines stay embedded
	// in the page so the whole file can be revealed in-browser.
	const base = `apiVersion: v1
kind: ConfigMap
metadata:
  name: web
  namespace: apps
data:
  k00: val00
  k01: val01
  k02: val02
  k03: val03
  k04: val04
  k05: val05
  k06: %s
  k07: val07
  k08: val08
  k09: val09
  k10: val10
  k11: val11
`
	from := htmlDoc(t, fmt.Sprintf(base, "old06"))
	to := htmlDoc(t, fmt.Sprintf(base, "new06"))
	out := renderHTMLString(t, []Doc{from}, []Doc{to})

	for _, want := range []string{
		`data-fold="r0-g0"`,          // leading-gap expander (context above the change was folded)
		`class="folded"`,             // collapsed context rows embedded, not dropped
		"Expand",                     // expander label
		"val00",                      // a line far above the change — present only if the whole file is embedded
		"val11",                      // a line far below the change
		`class="diff-code diff-del"`, // the change still renders
		`class="diff-code diff-add"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("folded-context HTML missing %q", want)
		}
	}
}

func TestRuneDiffRange(t *testing.T) {
	t.Parallel()
	// a value edit in the middle: only the differing run is flagged on each side.
	if lo, hi, _, _ := runeDiffRange("v1.19.6", "v1.20.0"); string([]rune("v1.19.6")[lo:hi]) != "19.6" {
		t.Errorf(`middle edit: got %q, want "19.6"`, string([]rune("v1.19.6")[lo:hi]))
	}
	// a suffix removed: the left flags the removed run, the right flags nothing.
	aLo, aHi, bLo, bHi := runeDiffRange("name: ceph-csi-controller-manager", "name: ceph-csi")
	if got := string([]rune("name: ceph-csi-controller-manager")[aLo:aHi]); got != "-controller-manager" {
		t.Errorf(`suffix removed (left): got %q, want "-controller-manager"`, got)
	}
	if bHi != bLo {
		t.Errorf("suffix removed (right): want empty range, got [%d,%d)", bLo, bHi)
	}
	// no shared prefix or suffix: nothing flagged, so the line is just tinted.
	if lo, hi, _, _ := runeDiffRange("abc", "xyz"); lo != 0 || hi != 0 {
		t.Errorf("unrelated lines: want empty range, got [%d,%d)", lo, hi)
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
