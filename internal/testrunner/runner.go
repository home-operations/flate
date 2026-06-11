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
	// failed or was missing — the body never ran. These are folded under
	// their root cause rather than listed per-resource (see Report.Write).
	OutcomeBlocked
)

// Case is one Kustomization (or HelmRelease) result.
type Case struct {
	ID      manifest.NamedResource
	Outcome Outcome
	Reason  string
}

// BlockedGroup folds the resources that cascaded from one root cause into a
// single reported line: Count resources blocked by Root. Missing is true when
// Root was never loaded (a dependency that does not exist), false when Root is a
// failure reported elsewhere in the table.
type BlockedGroup struct {
	Root    manifest.NamedResource
	Count   int
	Missing bool
}

// Report is the aggregate outcome.
type Report struct {
	Cases   []Case
	Passed  int
	Skipped int
	Failed  int
	// Blocked counts resources folded under a root cause (not listed as
	// Cases); BlockedRoots names those roots with their tallies.
	Blocked      int
	BlockedRoots []BlockedGroup
	Matched      int
}

// AnyFailed reports whether the run should be considered failed: a primary
// failure, or a cascade blocked on one. A broken dependency that blocks
// everything downstream must still flip the exit code even if nothing reported
// a primary failure in scope.
func (r Report) AnyFailed() bool { return r.Failed > 0 || r.Blocked > 0 }

// decorate maps an outcome to its leading glyph and the style renderer that
// colors it. Glyphs render in both modes (any UTF-8 sink); only color is gated.
func (o Outcome) decorate() (glyph string, paint func(string, bool) string) {
	switch o {
	case OutcomePassed:
		return style.GlyphPass, style.Pass
	case OutcomeFailed:
		return style.GlyphFail, style.Fail
	default: // OutcomeSkipped
		return style.GlyphSkip, style.Skip
	}
}

// reasonIndent aligns a failure's continuation lines beneath its row.
const reasonIndent = "         "

// Write renders the report: one row per case (status glyph, dimmed kind column,
// namespace/name, dimmed reason) followed by a summary — an overall verdict
// glyph, the colored counts, and a dim elapsed clock (omitted when zero). color
// gates the ANSI codes — the caller decides based on the sink (see cli.test).
func (r Report) Write(w io.Writer, color bool, elapsed time.Duration) error {
	kindW := 0
	for _, c := range r.Cases {
		kindW = max(kindW, len(c.ID.Kind))
	}

	var b strings.Builder
	b.WriteByte('\n')
	for _, c := range r.Cases {
		glyph, paint := c.Outcome.decorate()
		fmt.Fprintf(&b, "  %s  %s  %s",
			paint(glyph, color),
			style.Dim(fmt.Sprintf("%-*s", kindW, c.ID.Kind), color),
			c.ID.NamespacedName())
		// A single-line reason stays inline; a multi-line one (a kustomize
		// build / helm template diagnostic) prints its first line inline and
		// the rest indented beneath, so the detail survives instead of running
		// the row off the right edge.
		if lines := report.MessageLines(c.Reason); len(lines) > 0 {
			fmt.Fprintf(&b, "  %s", style.Dim(lines[0], color))
			for _, line := range lines[1:] {
				fmt.Fprintf(&b, "\n%s%s", reasonIndent, style.Dim(line, color))
			}
		}
		b.WriteByte('\n')
	}

	// Cascades fold under their root cause: one dim line per root, naming the
	// count it blocks, in place of a per-resource row for each victim.
	for _, g := range r.BlockedRoots {
		root := g.Root.NamespacedName()
		if g.Missing {
			root += " (not found)"
		}
		fmt.Fprintf(&b, "  %s  %s\n",
			style.Dim(style.GlyphBlocked, color),
			style.Dim(fmt.Sprintf("%d blocked by %s", g.Count, root), color))
	}

	// Summary: a verdict glyph (green ✓ / red ✗) leads the colored counts —
	// always the passed count; skipped/failed/blocked only when non-zero — and a
	// dim elapsed clock closes it. An empty run still reads "✓ 0 passed".
	verdict, paintVerdict := style.GlyphPass, style.Pass
	if r.Failed > 0 || r.Blocked > 0 {
		verdict, paintVerdict = style.GlyphFail, style.Fail
	}
	b.WriteString("\n  " + paintVerdict(verdict, color) + " ")
	b.WriteString(style.Pass(fmt.Sprintf("%d passed", r.Passed), color))
	if r.Skipped > 0 {
		b.WriteString(" · " + style.Dim(fmt.Sprintf("%d skipped", r.Skipped), color))
	}
	if r.Failed > 0 {
		b.WriteString(" · " + style.Fail(fmt.Sprintf("%d failed", r.Failed), color))
	}
	if r.Blocked > 0 {
		b.WriteString(" · " + style.Dim(fmt.Sprintf("%d blocked", r.Blocked), color))
	}
	if elapsed > 0 {
		b.WriteString("   " + style.Dim(style.Elapsed(elapsed), color))
	}
	b.WriteByte('\n')

	_, err := io.WriteString(w, b.String())
	return err
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
	// cascade still resolves to one root in blockedGroups.
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
				// Folded under its root cause, not listed per-resource.
				rep.Blocked++
				blocked[id] = j.Store.BlockedBy(id)
				continue
			default: // OutcomeFailed
				rep.Failed++
			}
			rep.Cases = append(rep.Cases, c)
		}
	}
	rep.BlockedRoots = blockedGroups(j.Store, blocked)
	return rep
}

// blockedGroups inverts the blocked graph (id → its immediate blockers) into one
// BlockedGroup per root cause, sorted by root, using the same root-resolution as
// the build/diff report so a cascade folds identically on every surface.
func blockedGroups(s *store.Store, blocked map[manifest.NamedResource][]manifest.NamedResource) []BlockedGroup {
	byRoot := report.Roots(blocked)
	groups := make([]BlockedGroup, 0, len(byRoot))
	for root, ids := range byRoot {
		_, known := s.GetStatus(root)
		groups = append(groups, BlockedGroup{Root: root, Count: len(ids), Missing: !known})
	}
	slices.SortFunc(groups, func(a, b BlockedGroup) int { return a.Root.Compare(b.Root) })
	return groups
}

func classify(s *store.Store, id manifest.NamedResource) Case {
	info, ok := s.GetStatus(id)
	switch {
	case !ok:
		return Case{ID: id, Outcome: OutcomeFailed, Reason: "no status reported"}
	case info.Status == store.StatusFailed:
		// A failure with recorded blockers never ran its body — it's blocked by
		// a failed/missing dependency, not a primary fault. Fold it under the
		// root cause (Report.Write) rather than reprinting the nested
		// "dependencies failed:" chain on its own row.
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
