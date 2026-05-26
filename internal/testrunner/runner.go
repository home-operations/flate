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
	Matched int
}

// AnyFailed reports whether any case failed.
func (r Report) AnyFailed() bool { return r.Failed > 0 }

// Write renders the report in a pytest-like format to w.
func (r Report) Write(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintln(&b, "============================================= test session starts =============================================")
	fmt.Fprintf(&b, "collected %d items\n\n", len(r.Cases))
	for _, c := range r.Cases {
		var status string
		switch c.Outcome {
		case OutcomePassed:
			status = "PASSED"
		case OutcomeSkipped:
			status = "SKIPPED"
		case OutcomeFailed:
			status = "FAILED"
		}
		fmt.Fprintf(&b, "%-60s %s", c.ID.String(), status)
		if c.Reason != "" {
			fmt.Fprintf(&b, " (%s)", c.Reason)
		}
		b.WriteByte('\n')
	}
	fmt.Fprintf(&b, "\n%d passed, %d skipped, %d failed\n", r.Passed, r.Skipped, r.Failed)
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
			case OutcomeFailed:
				rep.Failed++
			}
			rep.Cases = append(rep.Cases, c)
		}
	}
	return rep
}

func classify(s *store.Store, id manifest.NamedResource) Case {
	info, ok := s.GetStatus(id)
	switch {
	case !ok:
		return Case{ID: id, Outcome: OutcomeFailed, Reason: "no status reported"}
	case info.Status == store.StatusFailed:
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
