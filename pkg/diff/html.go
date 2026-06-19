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

// Row/cell kinds and resource statuses. These are not free-form: the
// template renders them straight into CSS class suffixes — diff-<kind>,
// status-<status>, and dot <status> — so each value must stay matched to a
// rule in templates/diff.html.tmpl.
const (
	kindCtx   = "ctx"
	kindAdd   = "add"
	kindDel   = "del"
	kindBlank = "blank"

	statusChanged = "changed"
	statusAdded   = "added"
	statusRemoved = "removed"
)

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

// uRow is one unified-view row. Hunk marks an expander (clickable fold
// separator) carrying Fold+Count; Folded marks a context row hidden behind
// the expander with that Fold id. An ordinary visible row sets neither.
type uRow struct {
	Hunk         bool   // expander row: reveals the rows sharing Fold
	Folded       bool   // hidden context row belonging to gap Fold
	Fold         string // resource-local gap id, e.g. "g2"
	Count        int    // hidden line count, for the expander label
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

// sRow is one side-by-side row. Hunk marks an expander, Folded a hidden
// context row behind it — both carry the gap's Fold id (see uRow).
type sRow struct {
	Hunk        bool   // expander row: reveals the rows sharing Fold
	Folded      bool   // hidden context row belonging to gap Fold
	Fold        string // resource-local gap id, e.g. "g2"
	Count       int    // hidden line count, for the expander label
	Left, Right cell
}

// renderHTML produces a self-contained HTML diff document: the same resource
// pairing and line diff as FormatDiff, rendered with YAML syntax highlighting,
// a left navigation tree, a side-by-side ⇄ unified toggle, and a light/dark
// theme. Identical resources are dropped, matching renderUnified; an empty
// diff produces no output at all, like the other formats.
func renderHTML(left, right []Doc, opts Options) ([]byte, error) {
	left = opts.normalize(left)
	right = opts.normalize(right)

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
		switch r.Status {
		case statusAdded:
			data.Added++
		case statusRemoved:
			data.Removed++
		default:
			data.Changed++
		}
	}
	if len(data.Resources) == 0 {
		return nil, nil // no diff — emit nothing, matching the other formats
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
// for both views. Context is folded to 3 lines per hunk (git-style); the lines
// trimmed before/between/after the hunks are emitted as collapsed rows behind
// an expander so the whole file can be revealed in place — see unifiedRows /
// sideRows.
func buildHTMLResource(p pairedResource, from, to string, hl *highlighter) htmlResource {
	a, b := difflib.SplitLines(from), difflib.SplitLines(to)
	ah, bh := hl.lines(a), hl.lines(b)
	groups := difflib.NewMatcher(a, b).GetGroupedOpCodes(3)
	markIntraline(a, b, groups, hl, ah, bh)

	id := joinNS(p.namespace, p.name)
	res := htmlResource{
		Title:  p.label(),
		Kind:   p.kind,
		Name:   id,
		Parent: htmlParent(p.parent),
		Status: htmlStatus(from, to),
		URows:  unifiedRows(ah, bh, groups),
		SRows:  sideRows(ah, bh, groups),
	}
	for _, u := range res.URows {
		switch u.Kind {
		case kindAdd:
			res.Add++
		case kindDel:
			res.Del++
		}
	}
	return res
}

// gap is one collapsed run of unchanged lines — the context GetGroupedOpCodes
// trims before, between and after the hunks — tagged with a positional fold id
// that both views share, so a single expand reveals the run in each.
type gap struct {
	id                 string
	oldStart, newStart int
	count              int
}

// foldGaps returns the trimmed unchanged runs indexed by the group they
// precede: index 0 is the run before the first hunk, index gi the run between
// groups gi-1 and gi, and index len(groups) the run after the last. Each is
// everything between one group's last opcode and the next group's first — the
// lines GetGroupedOpCodes drops. Empty runs stay zero-value (count 0) so the
// row builders can index by position and skip them.
func foldGaps(groups [][]difflib.OpCode, nOld int) []gap {
	gaps := make([]gap, len(groups)+1)
	set := func(pos, oldStart, oldEnd, newStart int) {
		if oldEnd > oldStart {
			gaps[pos] = gap{id: fmt.Sprintf("g%d", pos), oldStart: oldStart, newStart: newStart, count: oldEnd - oldStart}
		}
	}
	for gi, group := range groups {
		if gi == 0 {
			set(0, 0, group[0].I1, 0)
			continue
		}
		prev := groups[gi-1][len(groups[gi-1])-1]
		set(gi, prev.I2, group[0].I1, prev.J2)
	}
	if n := len(groups); n > 0 {
		last := groups[n-1][len(groups[n-1])-1]
		set(n, last.I2, nOld, last.J2)
	}
	return gaps
}

// unifiedRows renders the grouped opcodes as the single-column unified view:
// context lines carry both line numbers, deletes carry the old, inserts the
// new, and a replace is all of its deletes followed by all of its inserts. The
// runs foldGaps reports are emitted as collapsed context rows behind an
// expander, so the rest of the file can be revealed in place.
func unifiedRows(ah, bh []template.HTML, groups [][]difflib.OpCode) []uRow {
	var rows []uRow
	gaps := foldGaps(groups, len(ah))
	fold := func(pos int) {
		g := gaps[pos]
		if g.count == 0 {
			return
		}
		rows = append(rows, uRow{Hunk: true, Fold: g.id, Count: g.count})
		for k := range g.count {
			rows = append(rows, uRow{Folded: true, Fold: g.id, Kind: kindCtx,
				OldNo: g.oldStart + k + 1, NewNo: g.newStart + k + 1, HTML: ah[g.oldStart+k]})
		}
	}
	for gi, group := range groups {
		fold(gi)
		for _, op := range group {
			if op.Tag == 'e' {
				for k := range op.I2 - op.I1 {
					rows = append(rows, uRow{Kind: kindCtx, OldNo: op.I1 + k + 1, NewNo: op.J1 + k + 1, HTML: ah[op.I1+k]})
				}
				continue
			}
			if op.Tag == 'd' || op.Tag == 'r' { // old lines removed
				for k := range op.I2 - op.I1 {
					rows = append(rows, uRow{Kind: kindDel, OldNo: op.I1 + k + 1, HTML: ah[op.I1+k]})
				}
			}
			if op.Tag == 'i' || op.Tag == 'r' { // new lines added
				for k := range op.J2 - op.J1 {
					rows = append(rows, uRow{Kind: kindAdd, NewNo: op.J1 + k + 1, HTML: bh[op.J1+k]})
				}
			}
		}
	}
	fold(len(groups))
	return rows
}

// sideRows renders the grouped opcodes as the two-column side-by-side view:
// context mirrors both sides, a pure delete/insert blanks the opposite cell,
// and a replace aligns line-for-line, padding the shorter side with blanks.
// The runs foldGaps reports become collapsed context rows behind an expander,
// sharing fold ids with unifiedRows so one expand reveals both views.
func sideRows(ah, bh []template.HTML, groups [][]difflib.OpCode) []sRow {
	var rows []sRow
	gaps := foldGaps(groups, len(ah))
	fold := func(pos int) {
		g := gaps[pos]
		if g.count == 0 {
			return
		}
		rows = append(rows, sRow{Hunk: true, Fold: g.id, Count: g.count})
		for k := range g.count {
			rows = append(rows, sRow{
				Folded: true, Fold: g.id,
				Left:  cell{Kind: kindCtx, No: g.oldStart + k + 1, HTML: ah[g.oldStart+k]},
				Right: cell{Kind: kindCtx, No: g.newStart + k + 1, HTML: bh[g.newStart+k]},
			})
		}
	}
	for gi, group := range groups {
		fold(gi)
		for _, op := range group {
			switch op.Tag {
			case 'e':
				for k := range op.I2 - op.I1 {
					rows = append(rows, sRow{
						Left:  cell{Kind: kindCtx, No: op.I1 + k + 1, HTML: ah[op.I1+k]},
						Right: cell{Kind: kindCtx, No: op.J1 + k + 1, HTML: bh[op.J1+k]},
					})
				}
			case 'd':
				for k := range op.I2 - op.I1 {
					rows = append(rows, sRow{Left: cell{Kind: kindDel, No: op.I1 + k + 1, HTML: ah[op.I1+k]}, Right: cell{Kind: kindBlank}})
				}
			case 'i':
				for k := range op.J2 - op.J1 {
					rows = append(rows, sRow{Left: cell{Kind: kindBlank}, Right: cell{Kind: kindAdd, No: op.J1 + k + 1, HTML: bh[op.J1+k]}})
				}
			case 'r':
				dn, an := op.I2-op.I1, op.J2-op.J1
				for k := range max(dn, an) {
					l, r := cell{Kind: kindBlank}, cell{Kind: kindBlank}
					if k < dn {
						l = cell{Kind: kindDel, No: op.I1 + k + 1, HTML: ah[op.I1+k]}
					}
					if k < an {
						r = cell{Kind: kindAdd, No: op.J1 + k + 1, HTML: bh[op.J1+k]}
					}
					rows = append(rows, sRow{Left: l, Right: r})
				}
			}
		}
	}
	fold(len(groups))
	return rows
}

// markIntraline rewrites replace-aligned line pairs in ah/bh to add word-level
// highlight spans around the runes that actually changed, so a one-character
// edit reads as a one-character highlight instead of a whole-line tint.
func markIntraline(a, b []string, groups [][]difflib.OpCode, hl *highlighter, ah, bh []template.HTML) {
	for _, group := range groups {
		for _, op := range group {
			if op.Tag != 'r' {
				continue
			}
			for k := range min(op.I2-op.I1, op.J2-op.J1) {
				ai, bj := op.I1+k, op.J1+k
				aLo, aHi, bLo, bHi := runeDiffRange(strings.TrimRight(a[ai], "\n"), strings.TrimRight(b[bj], "\n"))
				if aHi > aLo {
					ah[ai] = hl.emit(a[ai], aLo, aHi)
				}
				if bHi > bLo {
					bh[bj] = hl.emit(b[bj], bLo, bHi)
				}
			}
		}
	}
}

// runeDiffRange returns the per-side changed rune ranges of two differing lines
// by trimming the common prefix and suffix. It returns empty ranges when the
// lines share no common affix (a total change — not worth a word highlight).
func runeDiffRange(a, b string) (aLo, aHi, bLo, bHi int) {
	ar, br := []rune(a), []rune(b)
	p := 0
	for p < len(ar) && p < len(br) && ar[p] == br[p] {
		p++
	}
	s := 0
	for s < len(ar)-p && s < len(br)-p && ar[len(ar)-1-s] == br[len(br)-1-s] {
		s++
	}
	if p == 0 && s == 0 {
		return 0, 0, 0, 0
	}
	return p, len(ar) - s, p, len(br) - s
}

// emit renders one YAML line to class-based highlighted HTML, walking chroma's
// token stream directly (rather than its formatter) so a word-level highlight
// can be spliced mid-token: when hi > lo, the rune range [lo,hi) is wrapped in
// <span class="wd">. Token classes mirror chroma's WithClasses output
// (StandardTypes) so they match the embedded stylesheet.
func (h *highlighter) emit(s string, lo, hi int) template.HTML {
	s = strings.TrimRight(s, "\n")
	it, err := h.lexer.Tokenise(nil, s)
	if err != nil {
		return rawHTML(template.HTMLEscapeString(s))
	}
	esc := func(r []rune) string { return template.HTMLEscapeString(string(r)) }
	var b strings.Builder
	pos := 0
	for t := it(); t != chroma.EOF; t = it() {
		rs := []rune(t.Value)
		n := len(rs)
		class := classFor(t.Type)
		if class != "" {
			b.WriteString(`<span class="`)
			b.WriteString(class)
			b.WriteString(`">`)
		}
		// Wrap [lo,hi)'s overlap with this token (clamped to the token's rune
		// span) in .wd; an empty overlap leaves the token plain.
		if lo2, hi2 := max(0, min(n, lo-pos)), max(0, min(n, hi-pos)); lo2 < hi2 {
			b.WriteString(esc(rs[:lo2]))
			b.WriteString(`<span class="wd">`)
			b.WriteString(esc(rs[lo2:hi2]))
			b.WriteString(`</span>`)
			b.WriteString(esc(rs[hi2:]))
		} else {
			b.WriteString(esc(rs))
		}
		if class != "" {
			b.WriteString(`</span>`)
		}
		pos += n
	}
	return rawHTML(b.String())
}

// classFor maps a chroma token type to its short CSS class, mirroring the
// formatter's StandardTypes lookup (with category fallback) so our hand-emitted
// spans align with chroma's generated stylesheet.
func classFor(tt chroma.TokenType) string {
	if c, ok := chroma.StandardTypes[tt]; ok {
		return c
	}
	if c, ok := chroma.StandardTypes[tt.SubCategory()]; ok {
		return c
	}
	if c, ok := chroma.StandardTypes[tt.Category()]; ok {
		return c
	}
	return ""
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
		return statusAdded
	case to == "":
		return statusRemoved
	default:
		return statusChanged
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
	// WithModeClasses scopes the wrapper class and WriteCSS rules by the style's
	// mode (.chroma.light / .chroma.dark) — the light/dark scoping diff.html.tmpl
	// toggles. chroma made it opt-in in v2.27.0 (implicitly on before); without it
	// both stylesheets collapse onto a single unscoped .chroma.
	fmtr := chromahtml.New(chromahtml.WithClasses(true), chromahtml.WithModeClasses(true), chromahtml.PreventSurroundingPre(true))
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
