package output

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/pkg/diff"
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
func (r *TestReport) AnyFailed() bool { return r.Failed > 0 || len(r.Blocked) > 0 }

// Glyphs render at every destination; only color is gated.
func (o Outcome) decorate() decoration {
	switch o {
	case OutcomePassed:
		return decoration{glyph: GlyphPass, paint: Pass}
	case OutcomeFailed:
		return decoration{glyph: GlyphFail, paint: Fail}
	case OutcomeBlocked:
		return decoration{glyph: GlyphBlocked, paint: Dim}
	case OutcomeSkipped:
		return decoration{glyph: GlyphSkip, paint: Dim}
	}
	return decoration{glyph: GlyphSkip, paint: Dim}
}

// Cases retain their supplied order; blocked cases follow them.
func (r *TestReport) rosterBlock(names diff.Names, color bool) string {
	all := slices.Concat(r.Cases, r.Blocked)
	kindW := 0
	for _, c := range all {
		kindW = max(kindW, len(names.Subjects[c.ID].Kind))
	}
	rows := make([]string, len(all))
	for i, c := range all {
		subject := names.Subjects[c.ID]
		rows[i] = caseRow(&c, &subject, kindW, color)
	}
	return strings.Join(rows, "\n")
}

func caseRow(c *Case, subject *diff.Subject, kindW int, color bool) string {
	style := c.Outcome.decorate()

	var row strings.Builder
	fmt.Fprintf(&row, "  %s  %s  %s",
		style.paint(style.glyph, color),
		Dim(fmt.Sprintf("%-*s", kindW, subject.Kind), color),
		subject.Name)
	row.WriteString(MessageTail(c.Reason, Dim, color))
	return row.String()
}

func (r *TestReport) verdict(color bool, elapsed time.Duration) string {
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

type decoration struct {
	glyph string
	paint func(string, bool) string
}
