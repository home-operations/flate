package output

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
)

// Outcome enumerates the per-resource result.
type Outcome int

// Possible Outcome values.
const (
	OutcomePassed Outcome = iota
	OutcomeSkipped
	OutcomeFailed
	// OutcomeBlocked means a failed or missing dependency prevented the body from running.
	OutcomeBlocked
)

// Case is one resource result.
type Case struct {
	ID      manifest.NamedResource
	Outcome Outcome
	Reason  string
}

// TestReport is the aggregate outcome.
type TestReport struct {
	Cases   []Case
	Passed  int
	Skipped int
	Failed  int
	// Blocked follows the roster in its supplied order, with prepared dependency reasons.
	Blocked []Case
	Matched int
}

// AnyFailed includes blocked-only runs, even without a primary failure in scope.
func (r TestReport) AnyFailed() bool { return r.Failed > 0 || len(r.Blocked) > 0 }

// Glyphs render at every destination; only color is gated.
func (o Outcome) decorate() (glyph string, paint func(string, bool) string) {
	switch o {
	case OutcomePassed:
		return GlyphPass, Pass
	case OutcomeFailed:
		return GlyphFail, Fail
	case OutcomeBlocked:
		return GlyphBlocked, Dim
	default: // OutcomeSkipped
		return GlyphSkip, Dim
	}
}

// Test writes the roster, advisories, deferred notes and one verdict.
func (e Encoder) Test(report TestReport, warnings []manifest.Warning, notes []Note, elapsed time.Duration) error {
	color := e.Color
	doc := Document(
		report.rosterBlock(color),
		WarningsBlock(warnings, color),
		NotesBlock(notes, color),
		report.verdict(color, elapsed),
	)
	_, err := io.WriteString(e.Writer, doc)
	return err
}

// Cases retain their supplied order; blocked cases follow them.
func (r TestReport) rosterBlock(color bool) string {
	all := slices.Concat(r.Cases, r.Blocked)
	kindW := 0
	for _, c := range all {
		kindW = max(kindW, len(c.ID.KindLabel()))
	}
	rows := make([]string, len(all))
	for i, c := range all {
		rows[i] = caseRow(c, kindW, color)
	}
	return strings.Join(rows, "\n")
}

func caseRow(c Case, kindW int, color bool) string {
	glyph, paint := c.Outcome.decorate()
	var row strings.Builder
	fmt.Fprintf(&row, "  %s  %s  %s",
		paint(glyph, color),
		Dim(fmt.Sprintf("%-*s", kindW, c.ID.KindLabel()), color),
		c.ID.NamespacedName())
	row.WriteString(MessageTail(c.Reason, Dim, color))
	return row.String()
}

func (r TestReport) verdict(color bool, elapsed time.Duration) string {
	glyph, paint := GlyphPass, Pass
	if r.AnyFailed() {
		glyph, paint = GlyphFail, Fail
	}
	return Verdict(color, paint(glyph, color), elapsed,
		Count{N: r.Passed, Label: "passed", Paint: Pass},
		Count{N: r.Skipped, Label: "skipped", Paint: Dim},
		Count{N: r.Failed, Label: "failed", Paint: Fail},
		Count{N: len(r.Blocked), Label: "blocked", Paint: Dim},
	)
}
