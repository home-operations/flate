package output

import (
	"errors"
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

// Model is the partitioned, deterministic view the renderer consumes.
type Model struct {
	Context string
	Primary []Primary
	Missing []Missing
	// Blocked is the count of distinct derived (cascaded) failures. It is the
	// verdict's "N blocked" figure — distinct resources, not the sum of the
	// per-root Blocks/RequiredBy lists, which can overlap when one resource
	// traces to more than one root (e.g. a failed parent AND a missing dep).
	Blocked int
}

// Errors prints the non-resource remainder returned by selection.
func (e Encoder) Errors(err error) error {
	if err != nil {
		_, err := fmt.Fprintln(e.Writer, "flate error:", err)
		return err
	}
	return nil
}

// Warning retains the producer payload and its prepared display context.
type Warning struct {
	Warning manifest.Warning
	// Group is the cluster index; negative values denote invocation-wide notices.
	Group   int
	Context string
	Subject Subject
	prefix  string
}

// ReportGroup contains one cluster's diagnostics and optional test roster.
type ReportGroup struct {
	Cluster string
	Models  []Model
	Test    *TestReport
	Names   Names
}

// Report coordinates the invocation's results, warnings and summaries.
type Report struct {
	Groups   []ReportGroup
	Warnings []Warning
	Test     bool
	Elapsed  time.Duration
	Error    error
}

// Prepare samples continuations and resolves every printed subject before writing.
func (r *Report) Prepare(extra [][]manifest.NamedResource) []Names {
	clusters := make([]string, len(r.Groups))
	inventory := make([][]manifest.NamedResource, len(r.Groups))
	for i, g := range r.Groups {
		clusters[i] = g.Cluster
		if i < len(extra) {
			inventory[i] = append(inventory[i], extra[i]...)
		}
		for _, m := range g.Models {
			for _, p := range m.Primary {
				inventory[i] = append(inventory[i], p.ID)
				inventory[i] = append(inventory[i], p.Blocks[:min(len(p.Blocks), blockedSample)]...)
			}
			for _, missing := range m.Missing {
				inventory[i] = append(inventory[i], missing.ID)
				inventory[i] = append(inventory[i], missing.RequiredBy[:min(len(missing.RequiredBy), blockedSample)]...)
			}
		}
		if g.Test != nil {
			for _, cases := range [][]Case{g.Test.Cases, g.Test.Blocked} {
				for _, c := range cases {
					inventory[i] = append(inventory[i], c.ID)
				}
			}
		}
	}
	for _, w := range r.Warnings {
		if w.Group >= 0 && w.Warning.Resource != (manifest.NamedResource{}) {
			inventory[w.Group] = append(inventory[w.Group], w.Warning.Resource)
		}
	}
	names := PrepareNames(clusters, inventory)
	for i := range r.Groups {
		r.Groups[i].Names = names[i]
	}
	for i, w := range r.Warnings {
		r.Warnings[i].prefix = ""
		if w.Group < 0 {
			continue
		}
		if w.Warning.Resource == (manifest.NamedResource{}) {
			r.Warnings[i].prefix = names[w.Group].Prefix
		} else {
			r.Warnings[i].Subject = names[w.Group].Subjects[w.Warning.Resource]
		}
	}
	return names
}

// Report writes results, warnings, cluster summaries and the test invocation total in order.
func (e Encoder) Report(r Report, results Encoder) error {
	var resultRows, failures, summaries []string
	var total TestReport
	for _, g := range r.Groups {
		if g.Test != nil {
			resultRows = append(resultRows, g.Test.rosterBlock(g.Names, results.Color))
			total.Passed += g.Test.Passed
			total.Skipped += g.Test.Skipped
			total.Failed += g.Test.Failed
			total.Blocked = append(total.Blocked, g.Test.Blocked...)
		}
		var failed, blocked int
		for _, m := range g.Models {
			failures = append(failures, m.failuresBlock(g.Names, e.Color))
			failed += len(m.Primary) + len(m.Missing)
			blocked += m.Blocked
		}
		var summary string
		if g.Test != nil {
			summary = g.Test.verdict(results.Color, 0)
		} else if failed != 0 || blocked != 0 || g.Names.Prefix != "" {
			glyph, paint := GlyphPass, Pass
			if failed != 0 || blocked != 0 {
				glyph, paint = GlyphFail, Fail
			}
			summary = Verdict(e.Color, paint(glyph, e.Color), 0,
				Count{N: failed, Label: "failed", Paint: paint}, Count{N: blocked, Label: "blocked", Paint: Dim})
		}
		if summary != "" && (!r.Test || len(r.Groups) > 1) {
			summaries = append(summaries, "  "+g.Names.Prefix+strings.TrimPrefix(summary, "  "))
		}
	}
	var writeErr error
	write := func(w io.Writer, blocks ...string) {
		if doc := Document(blocks...); doc != "" {
			_, err := io.WriteString(w, doc)
			writeErr = errors.Join(writeErr, err)
		}
	}
	write(results.Writer, resultRows...)
	write(e.Writer, failures...)
	writeErr = errors.Join(writeErr, e.Errors(r.Error))
	write(e.Writer, warningsBlock(r.Warnings, e.Color))
	if r.Test {
		write(results.Writer, summaries...)
		verdict := total.verdict(results.Color, r.Elapsed)
		if len(r.Groups) > 1 {
			verdict = "Invocation totals\n" + verdict
		}
		write(results.Writer, verdict)
	} else {
		write(e.Writer, summaries...)
	}
	return writeErr
}

func (m Model) failuresBlock(names Names, color bool) string {
	rows := make([]string, 0, len(m.Primary)+len(m.Missing))
	for _, p := range m.Primary {
		row := fmt.Sprintf("  %s  %s  %s",
			Fail(GlyphFail, color), Dim(withContext(m.Context, names.Subjects[p.ID].Kind), color), names.Subjects[p.ID].Name) +
			MessageTail(p.Msg, nil, color)
		if len(p.Blocks) > 0 {
			row += "\n" + msgIndent + Dim(withContext(m.Context, "blocks "+summarize(p.Blocks, names)), color)
		}
		rows = append(rows, row)
	}
	for _, mr := range m.Missing {
		rows = append(rows, fmt.Sprintf("  %s  %s  %s  %s\n%s%s",
			Fail(GlyphFail, color), Dim(withContext(m.Context, names.Subjects[mr.ID].Kind), color), names.Subjects[mr.ID].Name,
			Fail("not found", color), msgIndent,
			Dim(withContext(m.Context, "required by "+summarize(mr.RequiredBy, names)), color)))
	}
	return strings.Join(rows, "\n")
}

func withContext(context, text string) string {
	if context == "" {
		return text
	}
	return context + ": " + text
}

func warningsBlock(warnings []Warning, color bool) string {
	items := make([]string, 0, len(warnings))
	for _, w := range warnings {
		wn := w.Warning
		head := w.prefix + wn.Message
		if wn.Resource != (manifest.NamedResource{}) {
			head = w.Subject.Kind + " " + w.Subject.Name + ": " + wn.Message
		}
		head = withContext(w.Context, head)
		if wn.Category != "" {
			head = "[" + wn.Category + "] " + head
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
	return section(Warn(fmt.Sprintf("%s warnings (%d)", GlyphWarn, len(items)), color), items)
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
func summarize(ids []manifest.NamedResource, labels Names) string {
	total := len(ids)
	ids = ids[:min(len(ids), blockedSample)]
	names := make([]string, 0, len(ids))
	counts := make(map[string]int, len(ids))
	for _, id := range ids {
		name := labels.Subjects[id].Name
		names = append(names, name)
		counts[name]++
	}
	for i, name := range names {
		if counts[name] > 1 {
			names[i] = labels.Subjects[ids[i]].Kind + " " + name
		}
	}
	if total <= blockedSample {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s +%d more", strings.Join(names, ", "), total-blockedSample)
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
