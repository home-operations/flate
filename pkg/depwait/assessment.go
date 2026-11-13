package depwait

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// Assess preserves readiness and dependency provenance without publishing live status.
func (w *Waiter) Assess(ctx context.Context, reads *store.ReadSet, level manifest.DrainLevel) error {
	var blocked []manifest.NamedResource
	var failed *manifest.DependencyFailedError
	var cause error
	var causeID manifest.NamedResource
	for dep := range reads.Gates() {
		w.Materializer = dep.Materializer
		cl := w.Classify(ctx, dep.DependencyRef, level)
		if cl.Err != nil {
			if cause == nil || dep.Compare(causeID) < 0 {
				cause, causeID = cl.Err, dep.NamedResource
			}
			continue
		}
		switch cl.Kind {
		case schedule.Terminal:
		case schedule.DependencyFailed:
			if failed == nil {
				failed = &manifest.DependencyFailedError{
					Parent:  w.Parent,
					Subject: manifest.DiagnosticFor(w.Parent, w.Store.GetObject(w.Parent)),
					Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
				}
			}
			failed.Failed = append(failed.Failed, dep.NamedResource)
			reason := cl.DiagnosticReason(dep.NamedResource)
			if prior, exists := failed.Reasons[dep.NamedResource]; !exists || reason.Compare(prior) < 0 {
				failed.Reasons[dep.NamedResource] = reason
			}
		case schedule.Blocked:
			blocked = append(blocked, dep.NamedResource)
		}
	}
	if len(blocked) > 0 {
		slices.SortFunc(blocked, manifest.NamedResource.Compare)
		return &ErrBlocked{Deps: slices.Compact(blocked)}
	}
	if cause != nil {
		return cause
	}
	if failed != nil {
		slices.SortFunc(failed.Failed, manifest.NamedResource.Compare)
		failed.Failed = slices.Compact(failed.Failed)
		return failed
	}
	return nil
}
