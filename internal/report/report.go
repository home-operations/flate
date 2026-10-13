// Package report turns a reconcile's failures into a compact, styled end-of-run
// summary: the real (primary) errors shown once, every cascaded failure folded
// under the root that caused it, and any deferred log lines in a quiet footer.
// It exists so a single missing or failed dependency reads as one root cause
// with a list of what it blocked — not a wall of nested, repeated
// "dependencies failed: A: dependencies failed: B: …" chains.
//
// The model is built from selected orchestrator causes and status data and
// is pure/deterministic; rendering is gated on a color bool so it is plain and
// testable off a TTY. Styling reuses internal/style, the vocabulary the live
// status bar and `flate test` report already speak.
package report

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

// blockedSample bounds how many blocked/requiring ids are named inline per root
// before collapsing the rest to "+N more", so a root that blocks a whole cluster
// stays one readable line.
const blockedSample = 6

// Primary is a resource that failed on its own — a real render/template/build
// error. Blocks lists the resources whose failure cascaded from it (sorted).
type Primary struct {
	ID     manifest.NamedResource
	Msg    string
	Blocks []manifest.NamedResource
}

// Missing is a dependency that never rendered (absent from the store) yet was
// required by one or more resources — the other kind of root cause. RequiredBy
// lists the resources blocked on it (sorted).
type Missing struct {
	ID         manifest.NamedResource
	RequiredBy []manifest.NamedResource
}

// Note is one deferred log line for the footer, collapsed by identical text with
// a repeat count.
type Note struct {
	Text  string
	Count int
}

// Model is the partitioned, deterministic view the renderer consumes.
type Model struct {
	Primary []Primary
	Missing []Missing
	// Warnings are selected unrelated-failure advisories (Result.Unrelated).
	Warnings []manifest.Warning
	// Blocked is the count of distinct derived (cascaded) failures. It is the
	// verdict's "N blocked" figure — distinct resources, not the sum of the
	// per-root Blocks/RequiredBy lists, which can overlap when one resource
	// traces to more than one root (e.g. a failed parent AND a missing dep).
	Blocked int
}

// Empty reports whether there is nothing to render.
func (m Model) Empty() bool {
	return len(m.Primary) == 0 && len(m.Missing) == 0 && len(m.Warnings) == 0
}

// Build prepares rows from a selected result's classified failure causes.
func Build(result *orchestrator.Result) Model {
	var primary []Primary
	var missing []Missing
	blocked := selection.BlockedCount(result.Failed, result.Blocked, result.Causes)
	for root, victims := range result.Causes {
		if info, isFailed := result.Failed[root]; isFailed {
			primary = append(primary, Primary{ID: root, Msg: info.Message, Blocks: victims})
		} else {
			missing = append(missing, Missing{ID: root, RequiredBy: victims})
		}
	}

	slices.SortFunc(primary, func(a, b Primary) int { return a.ID.Compare(b.ID) })
	slices.SortFunc(missing, func(a, b Missing) int { return a.ID.Compare(b.ID) })
	return Model{Primary: primary, Missing: missing, Warnings: result.Unrelated, Blocked: blocked}
}

// WriteErrors prints the non-resource remainder returned by selection.
func WriteErrors(w io.Writer, err error) error {
	if err != nil {
		_, err := fmt.Fprintln(w, "flate error:", err)
		return err
	}
	return nil
}

// Write renders the model to w. color gates ANSI styling.
func (m Model) Write(w io.Writer, color bool) error {
	doc := Document(
		m.failuresBlock(color),
		WarningsBlock(m.Warnings, color),
		m.verdictBlock(color),
	)
	if doc == "" {
		return nil
	}
	_, err := io.WriteString(w, doc)
	return err
}

// failuresBlock renders the root-cause rows: each primary failure (its inline
// message and the resources it blocks) followed by each missing dependency.
func (m Model) failuresBlock(color bool) string {
	rows := make([]string, 0, len(m.Primary)+len(m.Missing))
	for _, p := range m.Primary {
		row := fmt.Sprintf("  %s  %s  %s",
			style.Fail(style.GlyphFail, color), style.Dim(p.ID.Kind, color), p.ID.NamespacedName()) +
			messageTail(p.Msg)
		if len(p.Blocks) > 0 {
			row += "\n" + msgIndent + style.Dim("blocks "+summarize(p.Blocks), color)
		}
		rows = append(rows, row)
	}
	for _, mr := range m.Missing {
		rows = append(rows, fmt.Sprintf("  %s  %s  %s  %s\n%s%s",
			style.Fail(style.GlyphFail, color), style.Dim(mr.ID.Kind, color), mr.ID.NamespacedName(),
			style.Fail("not found", color), msgIndent,
			style.Dim("required by "+summarize(mr.RequiredBy), color)))
	}
	return strings.Join(rows, "\n")
}

// WarningsBlock renders the advisory section: a header counting the warnings,
// then one attributed line each (with an indented detail line when present), or
// "" when there are none. Exported so the build/diff report and the `flate test`
// roster surface advisories identically.
func WarningsBlock(warnings []manifest.Warning, color bool) string {
	items := make([]string, 0, len(warnings))
	for _, wn := range warnings {
		head := wn.Message
		if wn.Resource != (manifest.NamedResource{}) {
			head = wn.Resource.Kind + " " + wn.Resource.NamespacedName() + ": " + wn.Message
		}
		if wn.Count > 1 {
			head = fmt.Sprintf("%s (×%d)", head, wn.Count)
		}
		item := style.Warn(head, color)
		if len(wn.Detail) > 0 {
			item += "\n      " + style.Dim(strings.Join(wn.Detail, ", "), color)
		}
		items = append(items, item)
	}
	return section(style.Warn(fmt.Sprintf("%s warnings (%d)", style.GlyphWarn, len(warnings)), color), items)
}

// NotesBlock renders the operational-log section: a header counting the notes,
// then one dim line each (collapsed identical lines carry a ×N suffix), or ""
// when there are none. Shared by every footer-bearing surface.
func NotesBlock(notes []Note, color bool) string {
	items := make([]string, 0, len(notes))
	var total int
	for _, n := range notes {
		total += cmp.Or(n.Count, 1)
		line := n.Text
		if n.Count > 1 {
			line = fmt.Sprintf("%s (×%d)", line, n.Count)
		}
		items = append(items, style.Dim(line, color))
	}
	return section(style.Dim(fmt.Sprintf("notes (%d)", total), color), items)
}

// verdictBlock renders the "✗ N failed · M blocked" line, or "" when
// nothing failed — a clean run carrying only advisories shows just those, not a
// misleading red "✗ 0 failed". failed = primary + missing roots; blocked is the
// distinct cascaded count (m.Blocked, not the sum of per-root lists).
func (m Model) verdictBlock(color bool) string {
	failed := len(m.Primary) + len(m.Missing)
	if failed == 0 && m.Blocked == 0 {
		return ""
	}
	return Verdict(color, style.GlyphFail, style.Fail, 0,
		Count{N: failed, Label: "failed", Paint: style.Fail},
		Count{N: m.Blocked, Label: "blocked", Paint: style.Dim},
	)
}

// section composes a titled footer block: the header line, then each item
// indented beneath it. Returns "" (an empty block Write drops) when no items.
func section(header string, items []string) string {
	if len(items) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  " + header)
	for _, it := range items {
		b.WriteString("\n    " + it)
	}
	return b.String()
}

// Document assembles a footer from section blocks: a leading blank line parts it
// from preceding output, a single blank line separates each non-empty block, and
// it ends with a newline. Returns "" when every block is empty, so the caller
// writes nothing. Blocks carry no blank lines of their own — spacing lives here,
// in one place, so there's no per-section newline bookkeeping to drift. Shared
// by the build/diff failure report and the `flate test` roster.
func Document(blocks ...string) string {
	nz := make([]string, 0, len(blocks))
	for _, b := range blocks {
		if b != "" {
			nz = append(nz, b)
		}
	}
	if len(nz) == 0 {
		return ""
	}
	return "\n" + strings.Join(nz, "\n\n") + "\n"
}

// Count is one labeled tally in a Verdict line: N items, a noun, and the paint
// that colors "<N> <Label>" (e.g. style.Fail for "failed").
type Count struct {
	N     int
	Label string
	Paint func(string, bool) string
}

// Verdict renders a one-line run summary: the lead glyph, then each count as
// "<N> <Label>" joined by " · ", then a dim elapsed clock (omitted when zero).
// The first count always shows (even at zero, e.g. "0 passed"); later counts
// show only when non-zero. Returns "" when given no counts. Shared by the
// build/diff failure report and the `flate test` roster so both speak one
// verdict grammar.
func Verdict(color bool, glyph string, paintGlyph func(string, bool) string, elapsed time.Duration, counts ...Count) string {
	if len(counts) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  " + paintGlyph(glyph, color))
	for i, c := range counts {
		if i > 0 && c.N == 0 {
			continue
		}
		sep := " "
		if i > 0 {
			sep = " · "
		}
		b.WriteString(sep + c.Paint(fmt.Sprintf("%d %s", c.N, c.Label), color))
	}
	if elapsed > 0 {
		b.WriteString("   " + style.Dim(style.Elapsed(elapsed), color))
	}
	return b.String()
}

// summarize caps an id list at blockedSample and includes kinds for ambiguous names.
func summarize(ids []manifest.NamedResource) string {
	names := make([]string, 0, len(ids))
	counts := make(map[string]int, len(ids))
	for _, id := range ids {
		name := id.NamespacedName()
		names = append(names, name)
		counts[name]++
	}
	for i, name := range names {
		if counts[name] > 1 {
			names[i] = ids[i].Kind + " " + name
		}
	}
	if len(names) <= blockedSample {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s +%d more", strings.Join(names[:blockedSample], ", "), len(names)-blockedSample)
}

// msgIndent aligns a primary failure's continuation lines (and its "blocks …"
// line) under the message column.
const msgIndent = "         "

// messageTail renders a primary failure's message as it trails the resource
// header line: the first line inline, any continuation lines indented beneath.
// Multi-line build/template diagnostics — notably the duplicate-id producer
// attribution from pkg/kustomize — survive intact instead of being truncated to
// their first line. Returns "" for an empty message.
func messageTail(msg string) string {
	lines := MessageLines(msg)
	if len(lines) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  " + lines[0])
	for _, line := range lines[1:] {
		b.WriteString("\n" + msgIndent + line)
	}
	return b.String()
}

// MessageLines splits a stored failure message into trimmed, non-empty lines.
// Shared with the test runner so a multi-line build/template diagnostic renders
// the same way on both surfaces.
func MessageLines(s string) []string {
	var out []string
	for line := range strings.SplitSeq(s, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			out = append(out, t)
		}
	}
	return out
}
