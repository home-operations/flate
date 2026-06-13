// Package testrunner implements `flate test`.
//
// The runner takes the post-reconcile state of the orchestrator and
// reports it in a pytest-like progress format. It does NOT shell out
// to the Go toolchain — every check is performed natively against the
// Store. A test "passes" when its Kustomization (and every nested
// HelmRelease) reached Status.Ready; it "fails" otherwise. Resources
// that were skipped by --path-orig change filtering are reported as
// SKIPPED so users see what flate actually did.
package testrunner

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/internal/report"
	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Job collects the orchestrator's post-run state.
type Job struct {
	Store *store.Store
	// Kinds limits the kinds reported on. Empty ↦ both Kustomization
	// and HelmRelease.
	Kinds []string
	// Name optionally narrows the report to a single resource.
	Name string
	// Include optionally narrows the report by resource identity.
	// Nil includes every resource that passes Kinds and Name.
	Include func(manifest.NamedResource) bool
}

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
	// the root cause(s) it traces to (see blockedCases).
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
		return style.GlyphPass, style.Pass
	case OutcomeFailed:
		return style.GlyphFail, style.Fail
	case OutcomeBlocked:
		return style.GlyphBlocked, style.Dim
	default: // OutcomeSkipped
		return style.GlyphSkip, style.Skip
	}
}

// reasonIndent aligns a failure's continuation lines beneath its row.
const reasonIndent = "         "

// Write renders the whole `flate test` report to w as one document: the roster
// (one row per case, then one per blocked resource), then the render advisories
// (warnings) and deferred operational logs (notes), then a single verdict —
// composed through the shared report toolkit so the test surface and the
// build/diff report speak one spacing and verdict grammar. Failures already
// appear inline on their roster rows, so this is the complete picture: there is
// no separate failure block to print elsewhere. color gates the ANSI codes — the
// caller decides based on the sink (see cli.test).
func (r Report) Write(w io.Writer, warnings []manifest.Warning, notes []report.Note, color bool, elapsed time.Duration) error {
	doc := report.Document(
		r.rosterBlock(color),
		report.WarningsBlock(warnings, color),
		report.NotesBlock(notes, color),
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
		style.Dim(fmt.Sprintf("%-*s", kindW, c.ID.Kind), color),
		c.ID.NamespacedName())
	if lines := report.MessageLines(c.Reason); len(lines) > 0 {
		fmt.Fprintf(&row, "  %s", style.Dim(lines[0], color))
		for _, line := range lines[1:] {
			fmt.Fprintf(&row, "\n%s%s", reasonIndent, style.Dim(line, color))
		}
	}
	return row.String()
}

// verdict renders the summary line: a green ✓ when everything passed, a red ✗
// when anything failed or blocked, leading the colored counts (passed always,
// the rest only when non-zero) and the elapsed clock.
func (r Report) verdict(color bool, elapsed time.Duration) string {
	glyph, paint := style.GlyphPass, style.Pass
	if r.AnyFailed() {
		glyph, paint = style.GlyphFail, style.Fail
	}
	return report.Verdict(color, glyph, paint, elapsed,
		report.Count{N: r.Passed, Label: "passed", Paint: style.Pass},
		report.Count{N: r.Skipped, Label: "skipped", Paint: style.Dim},
		report.Count{N: r.Failed, Label: "failed", Paint: style.Fail},
		report.Count{N: len(r.Blocked), Label: "blocked", Paint: style.Dim},
	)
}

// Run inspects the store and produces a Report. When j.Kinds is empty,
// both reconciler-driven kinds (Kustomization, HelmRelease) are
// reported on.
func Run(j Job) Report {
	kinds := j.Kinds
	if len(kinds) == 0 {
		kinds = []string{manifest.KindKustomization, manifest.KindHelmRelease}
	}
	var rep Report
	// id → its immediate blockers, accumulated across kinds so a cross-kind
	// cascade still resolves to its root cause(s) in blockedCases.
	blocked := map[manifest.NamedResource][]manifest.NamedResource{}
	for _, kind := range kinds {
		objs := j.Store.ListObjects(kind)
		slices.SortFunc(objs, func(a, b manifest.BaseManifest) int {
			return a.Named().Compare(b.Named())
		})
		for _, obj := range objs {
			id := obj.Named()
			if j.Name != "" && id.Name != j.Name {
				continue
			}
			if j.Include != nil && !j.Include(id) {
				continue
			}
			// Skip the synthetic bootstrap GitRepository the
			// discovery phase seeds for `spec.path` anchoring — it's
			// always an internal artifact. Discovery initially tags
			// it with the "bootstrap" message, but changed-only mode's
			// PreGate later overwrites that with MsgUnchanged, so the
			// status-message check that PR #212 introduced misses in
			// the typical `flate diff ks --path-orig=...` CI flow.
			// Skip by id alone: a user who explicitly declares a
			// GitRepository/flux-system/flux-system loses report
			// visibility on it (rare; flate would alias to it anyway).
			if id == manifest.BootstrapSourceID {
				continue
			}
			rep.Matched++
			c := classify(j.Store, id)
			switch c.Outcome {
			case OutcomePassed:
				rep.Passed++
			case OutcomeSkipped:
				rep.Skipped++
			case OutcomeBlocked:
				// Resolved to its root cause(s) after the scan; see blockedCases.
				blocked[id] = j.Store.BlockedBy(id)
				continue
			default: // OutcomeFailed
				rep.Failed++
			}
			rep.Cases = append(rep.Cases, c)
		}
	}
	// Surface run failures whose kind isn't in the roster (e.g. a
	// ResourceSet whose template won't parse, or a synthetic HelmChart) so a
	// primary fault isn't silently dropped now that there's no separate
	// build/diff failure block to catch it. The per-kind loop above already
	// covered roster kinds; here we only add previously-unseen failed ids.
	// A victim already classified blocked stays in the blocked set; a failed id
	// that is itself blocked joins the cascade rather than listing on its own row.
	seen := make(map[manifest.NamedResource]bool, len(rep.Cases)+len(blocked))
	for _, c := range rep.Cases {
		seen[c.ID] = true
	}
	for id := range blocked {
		seen[id] = true
	}
	extra := make([]manifest.NamedResource, 0)
	for id := range j.Store.FailedResources() {
		if seen[id] || id == manifest.BootstrapSourceID {
			continue
		}
		// A synthetic HelmChart is internal plumbing flate materializes to
		// drive an HR's chart fetch — never a first-class tested kind (absent
		// from every `test` subcommand's roster). Its failure always re-surfaces
		// inline on the consuming HR's row ("chart source … not ready: <err>"),
		// so listing the hash-suffixed chart id again is pure duplication.
		if id.Kind == manifest.KindHelmChart {
			continue
		}
		if j.Name != "" && id.Name != j.Name {
			continue
		}
		if j.Include != nil && !j.Include(id) {
			continue
		}
		extra = append(extra, id)
	}
	slices.SortFunc(extra, func(a, b manifest.NamedResource) int { return a.Compare(b) })
	for _, id := range extra {
		rep.Matched++
		c := classify(j.Store, id)
		if c.Outcome == OutcomeBlocked {
			blocked[id] = j.Store.BlockedBy(id)
			continue
		}
		rep.Failed++
		rep.Cases = append(rep.Cases, c)
	}
	rep.Blocked = blockedCases(j.Store, blocked)
	return rep
}

// blockedCases turns the blocked-by graph into one roster Case per victim. The
// keys of blocked ARE the resources classified blocked, so iterating them counts
// every victim exactly once — the rendered rows and the verdict's len(r.Blocked)
// are the same set by construction, with none lost to a grouping miss. Each
// victim's reason names its root cause(s) via report.RootsOf; a victim whose
// blockers form a closed cycle reaches no root, so it falls back to naming its
// immediate blockers rather than vanishing from the report.
func blockedCases(s *store.Store, blocked map[manifest.NamedResource][]manifest.NamedResource) []Case {
	cases := make([]Case, 0, len(blocked))
	for victim, blockers := range blocked {
		roots := report.RootsOf(victim, blocked)
		if len(roots) == 0 {
			roots = slices.Clone(blockers)
		}
		slices.SortFunc(roots, manifest.NamedResource.Compare)
		cases = append(cases, Case{ID: victim, Outcome: OutcomeBlocked, Reason: blockedReason(s, roots)})
	}
	slices.SortFunc(cases, func(a, b Case) int { return a.ID.Compare(b.ID) })
	return cases
}

// blockedReason names the root cause(s) a victim traces to, tagging any the
// store never loaded — a dependency that does not exist — as "(not found)" to
// set it apart from a root that failed on its own roster row.
func blockedReason(s *store.Store, roots []manifest.NamedResource) string {
	names := make([]string, len(roots))
	for i, r := range roots {
		names[i] = r.NamespacedName()
		if _, known := s.GetStatus(r); !known {
			names[i] += " (not found)"
		}
	}
	return "blocked by " + strings.Join(names, ", ")
}

func classify(s *store.Store, id manifest.NamedResource) Case {
	info, ok := s.GetStatus(id)
	switch {
	case !ok:
		return Case{ID: id, Outcome: OutcomeFailed, Reason: "no status reported"}
	case info.Status == store.StatusFailed:
		// A failure with recorded blockers never ran its body — it's blocked by
		// a failed/missing dependency, not a primary fault. Report it as blocked
		// (resolved to its root cause(s) by blockedCases) rather than reprinting
		// the nested "dependencies failed:" chain on its own row.
		if len(s.BlockedBy(id)) > 0 {
			return Case{ID: id, Outcome: OutcomeBlocked}
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
