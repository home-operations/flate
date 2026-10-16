// Package testrunner classifies completed reconcile results for flate test.
package testrunner

import (
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

// Run classifies the objects and reporting statuses prepared by orchestrator.Select.
func Run(result *orchestrator.Result) output.TestReport {
	var rep output.TestReport
	add := func(id manifest.NamedResource, info store.StatusInfo) {
		c := classify(id, info)
		switch c.Outcome {
		case output.OutcomePassed:
			rep.Passed++
		case output.OutcomeSkipped:
			rep.Skipped++
		case output.OutcomeBlocked:
			rep.Blocked = append(rep.Blocked, c)
			return
		default:
			rep.Failed++
		}
		rep.Cases = append(rep.Cases, c)
	}
	seen := make(map[manifest.NamedResource]bool, len(result.Objects))
	for _, obj := range result.Objects {
		id := obj.Named()
		if info, ok := result.Statuses[id]; ok {
			seen[id] = true
			rep.Matched++
			add(id, info)
		}
	}

	extra := make([]manifest.NamedResource, 0)
	for id, info := range result.Statuses {
		if info.Status != store.StatusFailed || seen[id] {
			continue
		}
		extra = append(extra, id)
	}
	slices.SortFunc(extra, manifest.NamedResource.Compare)
	for _, id := range extra {
		add(id, result.Statuses[id])
	}
	slices.SortFunc(rep.Blocked, func(a, b output.Case) int { return a.ID.Compare(b.ID) })
	return rep
}

func classify(id manifest.NamedResource, info store.StatusInfo) output.Case {
	switch {
	case info.Status == store.StatusFailed:
		if info.IsDependencyFailure() {
			return output.Case{ID: id, Outcome: output.OutcomeBlocked, Reason: info.Message}
		}
		// Strip the `flux error: input error:` sentinel chain so the
		// `flate test` table shows the actual cause rather than two
		// layers of bureaucracy. Same treatment the orchestrator gives
		// its aggregated error.
		return output.Case{ID: id, Outcome: output.OutcomeFailed, Reason: manifest.TrimSentinelPrefix(info.Message)}
	case store.IsUnchanged(info):
		return output.Case{ID: id, Outcome: output.OutcomeSkipped, Reason: store.MsgUnchanged}
	case store.IsSuspended(info):
		// spec.suspend was honored. A user-suspended KS / HR isn't
		// rendered, so reporting PASSED would be misleading — the
		// resource is intentionally inert, not "tests passed."
		return output.Case{ID: id, Outcome: output.OutcomeSkipped, Reason: store.MsgSuspended}
	case store.IsSkipped(info):
		// Strip the `skipped: ` convention prefix from the stored
		// message — the column already prints SKIPPED, so leading the
		// reason with "skipped:" again is duplicate labeling. Inner
		// propagated "skipped:" prefixes (a consumer wrapping its
		// source's skip message) survive verbatim — they're load-
		// bearing for the user (KS → which OCIRepo? why?).
		return output.Case{ID: id, Outcome: output.OutcomeSkipped,
			Reason: strings.TrimSpace(strings.TrimPrefix(info.Message, store.SkippedPrefix))}
	case info.Status == store.StatusReady:
		return output.Case{ID: id, Outcome: output.OutcomePassed}
	default:
		return output.Case{ID: id, Outcome: output.OutcomeFailed,
			Reason: "still " + string(info.Status) + ": " + manifest.TrimSentinelPrefix(info.Message)}
	}
}
