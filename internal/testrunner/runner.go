// Package testrunner implements `flate test`.
//
// The runner takes the post-reconcile state of the orchestrator and
// reports it in a pytest-like progress format. A test "passes" when its
// Kustomization (and every nested HelmRelease) reached Status.Ready; it
// "fails" otherwise. Resources
// that were skipped by --path-orig change filtering are reported as
// SKIPPED so users see what flate actually did.
package testrunner

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

// Outcome enumerates the per-resource result.
type Outcome int

// Possible Outcome values.
const (
	OutcomePassed Outcome = iota
	OutcomeSkipped
	OutcomeFailed
	// OutcomeBlocked is a resource that failed only because a dependency
	// failed or was missing — the body never ran. Blocked cases render after
	// the roster, each naming the root cause(s) it traces to.
	OutcomeBlocked
)

// Case is one Kustomization (or HelmRelease) result.
type Case struct {
	ID      manifest.NamedResource
	Outcome Outcome
	Reason  string
}

// Report is the aggregate outcome.
type Report struct {
	Cases   []Case
	Passed  int
	Skipped int
	Failed  int
	// Blocked holds one Case per resource whose dependency failed or was
	// missing — the body never ran. They render after the roster, each naming
	// the prepared root cause(s) it traces to.
	Blocked []Case
	Matched int
}

// AnyFailed reports whether the run should be considered failed: a primary
// failure, or a cascade blocked on one. A broken dependency that blocks
// everything downstream must still flip the exit code even if nothing reported
// a primary failure in scope.
func (r Report) AnyFailed() bool { return r.Failed > 0 || len(r.Blocked) > 0 }

// decorate maps an outcome to its leading glyph and the style renderer that
// colors it. Glyphs render in both modes (any UTF-8 sink); only color is gated.
func (o Outcome) decorate() (glyph string, paint func(string, bool) string) {
	switch o {
	case OutcomePassed:
		return output.GlyphPass, output.Pass
	case OutcomeFailed:
		return output.GlyphFail, output.Fail
	case OutcomeBlocked:
		return output.GlyphBlocked, output.Dim
	default: // OutcomeSkipped
		return output.GlyphSkip, output.Dim
	}
}

// Write renders the whole `flate test` report to w as one document: the roster
// (one row per case, then one per blocked resource), then the render advisories
// (warnings) and deferred operational logs (notes), then a single verdict —
// composed through the shared report toolkit so the test surface and the
// build/diff report speak one spacing and verdict grammar. Failures already
// appear inline on their roster rows, so this is the complete picture: there is
// no separate failure block to print elsewhere. color gates the ANSI codes — the
// caller decides based on the sink (see cli.test).
func (r Report) Write(w io.Writer, warnings []manifest.Warning, notes []output.Note, color bool, elapsed time.Duration) error {
	doc := output.Document(
		r.rosterBlock(color),
		output.WarningsBlock(warnings, color),
		output.NotesBlock(notes, color),
		r.verdict(color, elapsed),
	)
	_, err := io.WriteString(w, doc)
	return err
}

// rosterBlock renders the roster followed by the blocked cases — every row a
// uniform caseRow, the blocked ones trailing so the actionable failures lead and
// their downstream cascade reads as one group beneath. Empty when the run matched
// nothing, so Document drops it and only the verdict shows.
func (r Report) rosterBlock(color bool) string {
	all := slices.Concat(r.Cases, r.Blocked)
	kindW := 0
	for _, c := range all {
		kindW = max(kindW, len(c.ID.Kind))
	}
	rows := make([]string, len(all))
	for i, c := range all {
		rows[i] = caseRow(c, kindW, color)
	}
	return strings.Join(rows, "\n")
}

// caseRow renders one roster row: status glyph, dimmed kind column, the
// resource's namespace/name, then its dimmed reason — a multi-line reason keeps
// its first line inline and indents the continuation beneath it.
func caseRow(c Case, kindW int, color bool) string {
	glyph, paint := c.Outcome.decorate()
	var row strings.Builder
	fmt.Fprintf(&row, "  %s  %s  %s",
		paint(glyph, color),
		output.Dim(fmt.Sprintf("%-*s", kindW, c.ID.Kind), color),
		c.ID.NamespacedName())
	row.WriteString(output.MessageTail(c.Reason, output.Dim, color))
	return row.String()
}

// verdict renders the summary line: a green ✓ when everything passed, a red ✗
// when anything failed or blocked, leading the colored counts (passed always,
// the rest only when non-zero) and the elapsed clock.
func (r Report) verdict(color bool, elapsed time.Duration) string {
	glyph, paint := output.GlyphPass, output.Pass
	if r.AnyFailed() {
		glyph, paint = output.GlyphFail, output.Fail
	}
	return output.Verdict(color, glyph, paint, elapsed,
		output.Count{N: r.Passed, Label: "passed", Paint: output.Pass},
		output.Count{N: r.Skipped, Label: "skipped", Paint: output.Dim},
		output.Count{N: r.Failed, Label: "failed", Paint: output.Fail},
		output.Count{N: len(r.Blocked), Label: "blocked", Paint: output.Dim},
	)
}

// Run prepares a Report from selected objects and their status snapshot.
func Run(result *orchestrator.Result) Report {
	var rep Report
	seen := make(map[manifest.NamedResource]bool, len(result.Objects))
	for _, obj := range result.Objects {
		id := obj.Named()
		// Bootstrap and foreign objects have no user-facing reconcile verdict.
		if id == manifest.BootstrapSourceID || !manifest.KindOf(obj).Tracked {
			continue
		}
		seen[id] = true
		rep.Matched++
		info, ok := result.Statuses[id]
		c := classify(id, info, ok)
		switch c.Outcome {
		case OutcomePassed:
			rep.Passed++
		case OutcomeSkipped:
			rep.Skipped++
		case OutcomeBlocked:
			rep.Blocked = append(rep.Blocked, c)
			continue
		default:
			rep.Failed++
		}
		rep.Cases = append(rep.Cases, c)
	}

	extra := make([]manifest.NamedResource, 0)
	for id, info := range result.Statuses {
		if info.Status != store.StatusFailed || seen[id] {
			continue
		}
		// A synthetic HelmChart's failure is included in its consuming HR's reason.
		if id == manifest.BootstrapSourceID || id.Kind == manifest.KindHelmChart {
			continue
		}
		extra = append(extra, id)
	}
	slices.SortFunc(extra, manifest.NamedResource.Compare)
	for _, id := range extra {
		c := classify(id, result.Statuses[id], true)
		if c.Outcome == OutcomeBlocked {
			rep.Blocked = append(rep.Blocked, c)
			continue
		}
		rep.Failed++
		rep.Cases = append(rep.Cases, c)
	}
	slices.SortFunc(rep.Blocked, func(a, b Case) int { return a.ID.Compare(b.ID) })
	return rep
}

func classify(id manifest.NamedResource, info store.StatusInfo, ok bool) Case {
	switch {
	case !ok:
		return Case{ID: id, Outcome: OutcomeFailed, Reason: "no status reported"}
	case info.Status == store.StatusFailed:
		if info.IsDependencyFailure() {
			return Case{ID: id, Outcome: OutcomeBlocked, Reason: info.Message}
		}
		// Strip the `flux error: input error:` sentinel chain so the
		// `flate test` table shows the actual cause rather than two
		// layers of bureaucracy. Same treatment the orchestrator gives
		// its aggregated error.
		return Case{ID: id, Outcome: OutcomeFailed, Reason: manifest.TrimSentinelPrefix(info.Message)}
	case store.IsUnchanged(info):
		return Case{ID: id, Outcome: OutcomeSkipped, Reason: store.MsgUnchanged}
	case store.IsSuspended(info):
		// spec.suspend was honored. A user-suspended KS / HR isn't
		// rendered, so reporting PASSED would be misleading — the
		// resource is intentionally inert, not "tests passed."
		return Case{ID: id, Outcome: OutcomeSkipped, Reason: store.MsgSuspended}
	case store.IsSkipped(info):
		// Strip the `skipped: ` convention prefix from the stored
		// message — the column already prints SKIPPED, so leading the
		// reason with "skipped:" again is duplicate labeling. Inner
		// propagated "skipped:" prefixes (a consumer wrapping its
		// source's skip message) survive verbatim — they're load-
		// bearing for the user (KS → which OCIRepo? why?).
		return Case{ID: id, Outcome: OutcomeSkipped,
			Reason: strings.TrimSpace(strings.TrimPrefix(info.Message, store.SkippedPrefix))}
	case info.Status == store.StatusReady:
		return Case{ID: id, Outcome: OutcomePassed}
	default:
		return Case{ID: id, Outcome: OutcomeFailed,
			Reason: "still " + string(info.Status) + ": " + manifest.TrimSentinelPrefix(info.Message)}
	}
}
