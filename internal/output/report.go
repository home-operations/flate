package output

import (
	"cmp"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
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
	// Warnings are the prepared selected diagnostics.
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

// Errors prints the non-resource remainder returned by selection.
func (e Encoder) Errors(err error) error {
	if err != nil {
		_, err := fmt.Fprintln(e.Writer, "flate error:", err)
		return err
	}
	return nil
}

// Report writes the failure model with the destination color policy.
func (e Encoder) Report(m Model) error {
	color := e.Color
	doc := Document(
		m.failuresBlock(color),
		WarningsBlock(m.Warnings, color),
		m.verdictBlock(color),
	)
	if doc == "" {
		return nil
	}
	_, err := io.WriteString(e.Writer, doc)
	return err
}

// failuresBlock renders the root-cause rows: each primary failure (its inline
// message and the resources it blocks) followed by each missing dependency.
func (m Model) failuresBlock(color bool) string {
	rows := make([]string, 0, len(m.Primary)+len(m.Missing))
	for _, p := range m.Primary {
		row := fmt.Sprintf("  %s  %s  %s",
			Fail(GlyphFail, color), Dim(p.ID.KindLabel(), color), p.ID.NamespacedName()) +
			MessageTail(p.Msg, nil, color)
		if len(p.Blocks) > 0 {
			row += "\n" + msgIndent + Dim("blocks "+summarize(p.Blocks), color)
		}
		rows = append(rows, row)
	}
	for _, mr := range m.Missing {
		rows = append(rows, fmt.Sprintf("  %s  %s  %s  %s\n%s%s",
			Fail(GlyphFail, color), Dim(mr.ID.KindLabel(), color), mr.ID.NamespacedName(),
			Fail("not found", color), msgIndent,
			Dim("required by "+summarize(mr.RequiredBy), color)))
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
			head = wn.Resource.KindLabel() + " " + wn.Resource.NamespacedName() + ": " + wn.Message
		}
		if wn.Count > 1 {
			head = fmt.Sprintf("%s (×%d)", head, wn.Count)
		}
		item := Warn(head, color)
		if len(wn.Detail) > 0 {
			item += "\n      " + Dim(strings.Join(wn.Detail, ", "), color)
		}
		items = append(items, item)
	}
	return section(Warn(fmt.Sprintf("%s warnings (%d)", GlyphWarn, len(warnings)), color), items)
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
		items = append(items, Dim(line, color))
	}
	return section(Dim(fmt.Sprintf("notes (%d)", total), color), items)
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
	return Verdict(color, Fail(GlyphFail, color), 0,
		Count{N: failed, Label: "failed", Paint: Fail},
		Count{N: m.Blocked, Label: "blocked", Paint: Dim},
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
// that colors "<N> <Label>" (e.g. Fail for "failed").
type Count struct {
	N     int
	Label string
	Paint func(string, bool) string
}

// Verdict renders a one-line run summary from an already painted glyph, counts
// joined by " · ", and a dim elapsed clock (omitted when zero).
// The first count always shows (even at zero, e.g. "0 passed"); later counts
// show only when non-zero. Returns "" when given no counts. Shared by the
// build/diff failure report and the `flate test` roster so both speak one
// verdict grammar.
func Verdict(color bool, glyph string, elapsed time.Duration, counts ...Count) string {
	if len(counts) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  " + glyph)
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
		b.WriteString("   " + Dim(Elapsed(elapsed), color))
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
			names[i] = ids[i].KindLabel() + " " + name
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

// MessageTail trims blank lines and paints each continuation line separately.
func MessageTail(message string, paint func(string, bool) string, color bool) string {
	var b strings.Builder
	for line := range strings.SplitSeq(message, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if paint != nil {
			line = paint(line, color)
		}
		if b.Len() == 0 {
			b.WriteString("  ")
		} else {
			b.WriteString("\n" + msgIndent)
		}
		b.WriteString(line)
	}
	return b.String()
}
