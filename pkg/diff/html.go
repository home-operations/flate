package diff

import (
	"bytes"
	_ "embed"
	"fmt"
	"html/template"
	"strings"

	"github.com/alecthomas/chroma/v2"
	chromahtml "github.com/alecthomas/chroma/v2/formatters/html"
	"github.com/alecthomas/chroma/v2/lexers"
	"github.com/alecthomas/chroma/v2/styles"
	"github.com/pmezard/go-difflib/difflib"
)

//go:embed templates/diff.html.tmpl
var htmlTmpl string

var diffHTMLTemplate = template.Must(template.New("diff").Parse(htmlTmpl))

// htmlData is the diff.html.tmpl payload.
type htmlData struct {
	Changed, Added, Removed int
	ChromaCSS               template.CSS // light + dark token stylesheets (chroma)
	Tree                    []treeParent // sidebar navigation
	Resources               []htmlResource
}

// treeParent / treeKind / treeItem build the sidebar tree:
// parent (HelmRelease/Kustomization) → kind → resource.
type treeParent struct {
	Label string
	Kinds []treeKind
}

type treeKind struct {
	Kind  string
	Items []treeItem
}

type treeItem struct {
	ID, Name, Status string
	Add, Del         int
}

// htmlResource is one changed/added/removed resource, pre-rendered for both
// the side-by-side (SRows) and unified (URows) views.
type htmlResource struct {
	ID       string // anchor + tree target, e.g. "r12"
	Title    string // e.g. "Deployment app/web"
	Kind     string // resource kind (tree grouping)
	Name     string // ns/name (tree leaf label)
	Parent   string // producing KS/HR, e.g. "HelmRelease app/web"
	Status   string // "changed" | "added" | "removed"
	Add, Del int    // changed-line counts (tree badges)
	URows    []uRow
	SRows    []sRow
}

// uRow is one unified-view row. Hunk marks a fold separator (the rest of
// the fields are then unused).
type uRow struct {
	Hunk         bool
	Kind         string // "ctx" | "add" | "del"
	OldNo, NewNo int    // 1-based line numbers; 0 renders a blank gutter
	HTML         template.HTML
}

// cell is one side of a side-by-side row.
type cell struct {
	Kind string // "ctx" | "add" | "del" | "blank"
	No   int
	HTML template.HTML
}

// sRow is one side-by-side row. Hunk marks a fold separator.
type sRow struct {
	Hunk        bool
	Left, Right cell
}

// renderHTML produces a self-contained HTML diff document: the same resource
// pairing and line diff as FormatDiff, rendered with YAML syntax highlighting,
// a left navigation tree, a side-by-side ⇄ unified toggle, and a light/dark
// theme. Identical resources are dropped, matching renderUnified.
func renderHTML(left, right []Doc, opts Options) ([]byte, error) {
	left = normalizeDocs(left, opts.StripAttrs)
	right = normalizeDocs(right, opts.StripAttrs)

	hl, css, err := newHighlighter()
	if err != nil {
		return nil, err
	}

	data := htmlData{ChromaCSS: template.CSS(css)} //nolint:gosec // chroma-generated stylesheet, not user input
	for _, p := range pair(left, right) {
		from, err := marshalForUnified(p.a)
		if err != nil {
			return nil, err
		}
		to, err := marshalForUnified(p.b)
		if err != nil {
			return nil, err
		}
		if from == to {
			continue // identical — drop, as the unified path does
		}
		r := buildHTMLResource(p, from, to, hl)
		r.ID = fmt.Sprintf("r%d", len(data.Resources))
		data.Resources = append(data.Resources, r)
		switch {
		case from == "":
			data.Added++
		case to == "":
			data.Removed++
		default:
			data.Changed++
		}
	}
	data.Tree = buildTree(data.Resources)

	var buf bytes.Buffer
	if err := diffHTMLTemplate.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("render html: %w", err)
	}
	return buf.Bytes(), nil
}

// buildTree groups the resources — already sorted by parent → kind → name by
// pair() — into the sidebar's parent → kind → resource hierarchy without a map,
// relying on that ordering.
func buildTree(res []htmlResource) []treeParent {
	var tree []treeParent
	for _, r := range res {
		if len(tree) == 0 || tree[len(tree)-1].Label != r.Parent {
			tree = append(tree, treeParent{Label: r.Parent})
		}
		tp := &tree[len(tree)-1]
		if len(tp.Kinds) == 0 || tp.Kinds[len(tp.Kinds)-1].Kind != r.Kind {
			tp.Kinds = append(tp.Kinds, treeKind{Kind: r.Kind})
		}
		tk := &tp.Kinds[len(tp.Kinds)-1]
		tk.Items = append(tk.Items, treeItem{ID: r.ID, Name: r.Name, Status: r.Status, Add: r.Add, Del: r.Del})
	}
	return tree
}

// buildHTMLResource diffs one resource's from/to YAML and pre-renders the rows
// for both views. Context is folded to 3 lines per hunk (git-style), with a
// separator row between hunks.
func buildHTMLResource(p pairedResource, from, to string, hl *highlighter) htmlResource {
	a := difflib.SplitLines(from)
	b := difflib.SplitLines(to)
	ah := hl.lines(a)
	bh := hl.lines(b)

	res := htmlResource{
		Title:  p.kind + " " + joinNS(p.namespace, p.name),
		Kind:   p.kind,
		Name:   joinNS(p.namespace, p.name),
		Parent: htmlParent(p.parent),
		Status: htmlStatus(from, to),
	}
	for gi, group := range difflib.NewMatcher(a, b).GetGroupedOpCodes(3) {
		if gi > 0 {
			res.URows = append(res.URows, uRow{Hunk: true})
			res.SRows = append(res.SRows, sRow{Hunk: true})
		}
		for _, op := range group {
			switch op.Tag {
			case 'e': // equal → context on both sides
				for k := range op.I2 - op.I1 {
					o, n := op.I1+k+1, op.J1+k+1
					res.URows = append(res.URows, uRow{Kind: "ctx", OldNo: o, NewNo: n, HTML: ah[op.I1+k]})
					res.SRows = append(res.SRows, sRow{
						Left:  cell{Kind: "ctx", No: o, HTML: ah[op.I1+k]},
						Right: cell{Kind: "ctx", No: n, HTML: bh[op.J1+k]},
					})
				}
			case 'd': // delete (left only)
				for k := range op.I2 - op.I1 {
					o := op.I1 + k + 1
					res.URows = append(res.URows, uRow{Kind: "del", OldNo: o, HTML: ah[op.I1+k]})
					res.SRows = append(res.SRows, sRow{Left: cell{Kind: "del", No: o, HTML: ah[op.I1+k]}, Right: cell{Kind: "blank"}})
				}
			case 'i': // insert (right only)
				for k := range op.J2 - op.J1 {
					n := op.J1 + k + 1
					res.URows = append(res.URows, uRow{Kind: "add", NewNo: n, HTML: bh[op.J1+k]})
					res.SRows = append(res.SRows, sRow{Left: cell{Kind: "blank"}, Right: cell{Kind: "add", No: n, HTML: bh[op.J1+k]}})
				}
			case 'r': // replace
				// Unified: all deletes, then all inserts.
				for k := range op.I2 - op.I1 {
					res.URows = append(res.URows, uRow{Kind: "del", OldNo: op.I1 + k + 1, HTML: ah[op.I1+k]})
				}
				for k := range op.J2 - op.J1 {
					res.URows = append(res.URows, uRow{Kind: "add", NewNo: op.J1 + k + 1, HTML: bh[op.J1+k]})
				}
				// Side-by-side: align line-for-line, pad the shorter side.
				dn, an := op.I2-op.I1, op.J2-op.J1
				for k := range max(dn, an) {
					l, r := cell{Kind: "blank"}, cell{Kind: "blank"}
					if k < dn {
						l = cell{Kind: "del", No: op.I1 + k + 1, HTML: ah[op.I1+k]}
					}
					if k < an {
						r = cell{Kind: "add", No: op.J1 + k + 1, HTML: bh[op.J1+k]}
					}
					res.SRows = append(res.SRows, sRow{Left: l, Right: r})
				}
			}
		}
	}
	for _, u := range res.URows {
		switch u.Kind {
		case "add":
			res.Add++
		case "del":
			res.Del++
		}
	}
	return res
}

func htmlParent(p Parent) string {
	s := p.Kind + " " + joinNS(p.Namespace, p.Name)
	if p.Path != "" {
		s += " (" + p.Path + ")"
	}
	return s
}

func htmlStatus(from, to string) string {
	switch {
	case from == "":
		return "added"
	case to == "":
		return "removed"
	default:
		return "changed"
	}
}

// highlighter renders single YAML lines to syntax-highlighted HTML spans
// (chroma, class-based). The matching stylesheet — both the light (github) and
// dark (github-dark) variants, scoped under .chroma.light / .chroma.dark — is
// emitted once into the document <style>; the spans themselves are
// theme-agnostic. Highlighting is per-line so it maps 1:1 onto the diff line
// indices; block-scalar bodies lose cross-line context, immaterial for review.
type highlighter struct {
	lexer chroma.Lexer
	style *chroma.Style
	fmtr  *chromahtml.Formatter
}

func newHighlighter() (*highlighter, string, error) {
	lexer := lexers.Get("yaml")
	if lexer == nil {
		lexer = lexers.Fallback
	}
	lexer = chroma.Coalesce(lexer)
	light := styles.Get("github")
	if light == nil {
		light = styles.Fallback
	}
	dark := styles.Get("github-dark")
	if dark == nil {
		dark = light
	}
	fmtr := chromahtml.New(chromahtml.WithClasses(true), chromahtml.PreventSurroundingPre(true))
	var css bytes.Buffer
	if err := fmtr.WriteCSS(&css, light); err != nil {
		return nil, "", fmt.Errorf("chroma css (light): %w", err)
	}
	css.WriteByte('\n')
	if err := fmtr.WriteCSS(&css, dark); err != nil {
		return nil, "", fmt.Errorf("chroma css (dark): %w", err)
	}
	return &highlighter{lexer: lexer, style: light, fmtr: fmtr}, css.String(), nil
}

func (h *highlighter) lines(src []string) []template.HTML {
	out := make([]template.HTML, len(src))
	for i, s := range src {
		out[i] = h.line(s)
	}
	return out
}

func (h *highlighter) line(s string) template.HTML {
	s = strings.TrimRight(s, "\n")
	esc := func() template.HTML { return rawHTML(template.HTMLEscapeString(s)) }
	it, err := h.lexer.Tokenise(nil, s)
	if err != nil {
		return esc()
	}
	var b strings.Builder
	if err := h.fmtr.Format(&b, h.style, it); err != nil {
		return esc()
	}
	return rawHTML(b.String())
}

// rawHTML wraps already-escaped markup for injection into the template.
// Callers pass chroma formatter output or template.HTMLEscapeString output,
// both of which HTML-escape the underlying token text.
func rawHTML(s string) template.HTML {
	return template.HTML(s) //nolint:gosec // s is pre-escaped (chroma / HTMLEscapeString)
}
