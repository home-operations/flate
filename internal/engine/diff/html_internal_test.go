package diff

import (
	"fmt"
	"html"
	"maps"
	"strings"
	"testing"

	"github.com/alecthomas/chroma/v2"
	"github.com/alecthomas/chroma/v2/lexers"
	xhtml "golang.org/x/net/html"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func htmlDoc(t *testing.T, y string) Doc {
	t.Helper()
	var m map[string]any
	{
		err := yaml.Unmarshal([]byte(y), &m)
		assert.FatalIf(t, err != nil, "unmarshal: %v", err)
	}
	return Doc{Manifest: m, Parent: Parent{NamedResource: testutil.HelmReleaseID("apps", "web")}}
}

func renderHTMLString(t *testing.T, left, right []Doc) string {
	t.Helper()
	out, _, err := renderDocuments(left, right, new(Options{Format: FormatHTML}))
	assert.FatalIf(t, err != nil, "renderDocuments(html): %v", err)
	return string(out)
}

func TestRenderHTML_Changed(t *testing.T) {
	t.Parallel()
	from := htmlDoc(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web\n  namespace: apps\ndata:\n  replicas: keep  red end\n  level: info\n",
	)
	to := htmlDoc(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web\n  namespace: apps\ndata:\n  replicas: keep  blue end\n  level: info\n",
	)
	for _, d := range []*Doc{&from, &to} {
		d.Manifest["metadata"].(map[string]any)["name"] = "web<resource>"
		d.Manifest["data"].(map[string]any)["payload"] = "<yaml>"
		d.Parent.Name, d.Parent.Path = "web<parent>", "apps/<path>"
	}
	raw, warnings, err := renderDocuments([]Doc{from}, []Doc{to}, new(Options{
		Format:     FormatHTML,
		Suppressed: []Suppression{{Parent: from.Parent, Side: sideCurrent, Reason: "<suppression>"}},
	}))
	assert.NoError(t, err)
	assert.FatalIf(t, len(warnings) != 0, "HTML renderer constructed presentation warnings: %+v", warnings)
	out := string(raw)

	highlighter, css, err := NewHighlighter()
	assert.NoError(t, err)
	assert.ErrorIf(t, !strings.Contains(out, css), "HTML is missing the generated stylesheet")

	yamlLexer := chroma.Coalesce(lexers.Get("yaml"))
	tokens, err := yamlLexer.Tokenise(nil, "  replicas: keep  red end")
	assert.NoError(t, err)
	var keyClass string
	for token := tokens(); token != chroma.EOF; token = tokens() {
		if token.Value == "replicas" {
			keyClass = chroma.StandardTypes[token.Type]
			break
		}
	}
	assert.FatalIf(t, keyClass == "", "Chroma generated no syntax class for the replicas key")
	for _, theme := range []string{"light", "dark"} {
		selector := ".chroma." + theme + " ." + keyClass
		_, rule, _ := strings.Cut(out, selector)
		assert.ErrorIf(
			t,
			!strings.HasPrefix(strings.TrimSpace(rule), "{"),
			"HTML is missing the CSS rule for %s",
			selector,
		)
	}
	keySpan := `<span class="` + keyClass + `">replicas</span>`
	for _, tc := range []struct {
		kind, line, change string
	}{
		{"del", "  replicas: keep  red end", "red"},
		{"add", "  replicas: keep  blue end", "blue"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			_, after, found := strings.Cut(out, `<td class="diff-code diff-`+tc.kind+`">`)
			assert.FatalIf(t, !found, "missing %s cell", tc.kind)
			cell, _, closed := strings.Cut(after, "</td>")
			assert.FatalIf(t, !closed, "unclosed %s cell", tc.kind)
			const changeStart = len("  replicas: keep  ")
			assert.Equal(t, cell, string(highlighter.Emit(tc.line, changeStart, changeStart+len(tc.change))))
			assert.ErrorIf(t, !strings.Contains(cell, keySpan), "%s cell missing YAML key span %q", tc.kind, keySpan)

			var visible strings.Builder
			tokens := xhtml.NewTokenizer(strings.NewReader(cell))
			for token := tokens.Next(); token != xhtml.ErrorToken; token = tokens.Next() {
				if token == xhtml.TextToken {
					visible.Write(tokens.Text())
				}
			}
			assert.Equal(t, visible.String(), tc.line)
			mark := `<span class="wd">` + tc.change + `</span>`
			assert.ErrorIf(t, !strings.Contains(cell, mark), "%s cell missing changed word %q", tc.kind, tc.change)
			assert.Equal(t, strings.Count(cell, `<span class="wd">`), 1)
		})
	}

	for _, want := range []string{
		"<!doctype html>",
		`<table class="view side">`,    // side-by-side view present
		`<table class="view unified">`, // unified view present
		`id="view-btn"`,                // side ⇄ unified toggle
		`id="theme-btn"`,               // light/dark toggle
		`class="tree-leaf"`,            // sidebar navigation tree
		`id="filter"`,                  // sidebar resource filter box
		"body.chroma.dark",
		"body.chroma.light",
		".chroma.dark .",
		".chroma.light .",
		"status status-changed",      // changed chip
		`class="diff-code diff-del"`, // removed line cell (side view)
		`class="diff-code diff-add"`, // added line cell
		"1 changed",                  // summary count
		"ConfigMap apps/web",         // per-resource title
		"HelmRelease apps/web",       // parent attribution
		"&lt;resource&gt;", "&lt;parent&gt;", "apps/&lt;path&gt;", "&lt;yaml&gt;",
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
		assert.ErrorIf(t, !strings.Contains(out, want), "changed-diff HTML missing %q", want)
	}
	for _, unescaped := range []string{"<resource>", "<parent>", "<path>", "<yaml>", "<suppression>"} {
		assert.ErrorIf(t, strings.Contains(out, unescaped), "unescaped input %q in HTML", unescaped)
	}
}

func TestRenderHTML_AddedAndRemoved(t *testing.T) {
	t.Parallel()
	added := htmlDoc(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: added\n  namespace: apps\ndata:\n  k: v\n")
	removed := htmlDoc(
		t,
		"apiVersion: v1\nkind: Service\nmetadata:\n  name: removed\n  namespace: apps\nspec:\n  ports:\n    - port: 80\n",
	)
	out := renderHTMLString(t, []Doc{removed}, []Doc{added})

	for _, want := range []string{
		"status status-added", "status status-removed",
		"ConfigMap apps/added", "Service apps/removed",
		"0 changed", "1 added", "1 removed",
	} {
		assert.ErrorIf(t, !strings.Contains(out, want), "added/removed HTML missing %q", want)
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
		assert.ErrorIf(t, !strings.Contains(out, want), "folded-context HTML missing %q", want)
	}
}

func TestRenderHTML_Identical(t *testing.T) {
	t.Parallel()
	d := htmlDoc(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: web\n  namespace: apps\ndata:\n  k: v\n")
	out := renderHTMLString(t, []Doc{d}, []Doc{d})

	assert.ErrorIf(t, out != "", "identical docs should render empty, got:\n%s", out)
}

func TestRenderHTML_PreparedSubjects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		original, current bool
		shared, removed   string
	}{
		{name: "neither", shared: "Kustomization apps/shared", removed: "Kustomization apps/removed"},
		{name: "original only", original: true, shared: `entry path "original"`, removed: `entry path "removed"`},
		{name: "current only", current: true, shared: `entry path "current"`, removed: "Kustomization apps/removed"},
		{name: "both", original: true, current: true, shared: `entry path "current"`, removed: `entry path "removed"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shared := Parent{NamedResource: testutil.KustomizationID("apps", "shared")}
			removed := Parent{NamedResource: testutil.KustomizationID("apps", "removed")}
			var subjects [2]map[manifest.NamedResource]manifest.DiagnosticIdentity
			if tc.original {
				subjects[0] = map[manifest.NamedResource]manifest.DiagnosticIdentity{
					shared.NamedResource:  {ID: shared.NamedResource, Path: "original"},
					removed.NamedResource: {ID: removed.NamedResource, Path: "removed"},
				}
			}
			if tc.current {
				subjects[1] = map[manifest.NamedResource]manifest.DiagnosticIdentity{
					shared.NamedResource: {ID: shared.NamedResource, Path: "current"},
				}
			}
			before := [2]map[manifest.NamedResource]manifest.DiagnosticIdentity{
				maps.Clone(subjects[0]),
				maps.Clone(subjects[1]),
			}
			left := []Doc{
				cmDoc(new(shared), manifest.NamedResource{Name: "shared"}, "before", ""),
				cmDoc(new(removed), manifest.NamedResource{Name: "removed"}, "before", ""),
			}
			right := []Doc{cmDoc(new(shared), manifest.NamedResource{Name: "shared"}, "after", "")}
			out, _, err := renderDocuments(left, right, new(Options{Format: FormatHTML, subjects: subjects}))
			assert.NoError(t, err)
			for _, label := range []string{tc.shared, tc.removed} {
				heading := `<div class="rparent">` + html.EscapeString(label) + `</div>`
				assert.ErrorIf(
					t,
					!strings.Contains(string(out), heading),
					"HTML missing parent heading %q",
					heading,
				)
			}
			assert.Diff(t, subjects, before)
		})
	}
}
