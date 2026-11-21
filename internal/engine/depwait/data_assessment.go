package depwait

import (
	"context"
	"errors"
	"slices"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

// DataAssessment retains failures while unresolved values inputs can still settle.
type DataAssessment struct {
	Inputs  []DataDependency
	Blocked []manifest.NamedResource
	Failed  *manifest.DependencyFailedError
	Results []values.Result
	causes  []error
}

// Settle applies the same availability and failure rules in live and detached assessments.
func (a *DataAssessment) Settle(
	ctx context.Context,
	w *Waiter,
	reads *store.ReadSet,
	level manifest.DrainLevel,
) error {
	gateErr := w.Assess(ctx, reads, level)
	a.Failed, _ = errors.AsType[*manifest.DependencyFailedError](gateErr)
	a.Blocked = a.Blocked[:0]
	a.causes = a.causes[:0]
	a.Results = a.Results[:0]
	if pending, ok := errors.AsType[*BlockedError](gateErr); ok {
		a.Blocked = append(a.Blocked, pending.Deps...)
	}
	for i := range a.Inputs {
		input := &a.Inputs[i]
		result := w.Available(ctx, *input, level)
		if result.Err != nil {
			return result.Err
		}
		if err := a.settleInput(w, reads, input, &result); err != nil {
			return err
		}
	}
	if len(a.Blocked) != 0 {
		slices.SortFunc(a.Blocked, manifest.NamedResource.Compare)
		a.Blocked = slices.Compact(a.Blocked)
		return nil
	}
	if len(a.causes) == 1 && !errors.Is(a.causes[0], manifest.ErrObjectNotFound) {
		return a.causes[0]
	}
	if a.Failed != nil {
		slices.SortFunc(a.Failed.Failed, manifest.NamedResource.Compare)
		a.Failed.Failed = slices.Compact(a.Failed.Failed)
		if len(a.causes) == 0 {
			return a.Failed
		}
		// The aggregate MUST precede the original errors in the chain.
		return manifest.NewError(manifest.ErrInput, errors.Join(a.Failed, errors.Join(a.causes...)), a.Failed.Error())
	}
	return gateErr
}

func (a *DataAssessment) settleInput(
	w *Waiter,
	reads *store.ReadSet,
	input *DataDependency,
	result *availability,
) error {
	if result.Result.RequiresData() {
		if !result.Result.Materialized() {
			for _, producer := range input.Input.Materializers {
				reads.RequireData(producer)
			}
		}
		reads.RequireData(input.NamedResource)
	}
	switch result.Kind {
	case schedule.Blocked:
		a.Blocked = append(a.Blocked, result.Blocked...)
	case schedule.DependencyFailed:
		if err := result.Result.MaterialError(); err != nil {
			return err
		}
		a.Failed = w.mergeDataFailure(a.Failed, result)
		a.causes = append(a.causes, result.Result.Err())
	case schedule.Terminal:
		input.Result = result.Result
		a.Results = append(a.Results, result.Result)
	}
	return nil
}

func (w *Waiter) mergeDataFailure(
	failed *manifest.DependencyFailedError,
	result *availability,
) *manifest.DependencyFailedError {
	if failed == nil {
		failed = &manifest.DependencyFailedError{
			Parent:  w.Parent,
			Subject: manifest.DiagnosticFor(w.Parent, w.Store.GetObject(w.Parent)),
			Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
		}
	}
	ids := result.Failed
	if len(ids) == 0 {
		ids = []manifest.NamedResource{result.Reason.Subject.ID}
	}
	for _, id := range ids {
		captured := result.Reason
		if id != captured.Subject.ID {
			for i := range captured.Causes {
				if captured.Causes[i].Subject.ID == id {
					captured = captured.Causes[i]
					break
				}
			}
		}
		previous := failed.Reasons[id]
		captured.Causes = append(slices.Clone(previous.Causes), captured.Causes...)
		slices.SortFunc(captured.Causes, manifest.DiagnosticReason.Compare)
		captured.Causes = slices.CompactFunc(captured.Causes, func(a, b manifest.DiagnosticReason) bool {
			return a.Compare(b) == 0
		})
		failed.Failed = append(failed.Failed, id)
		failed.Reasons[id] = captured
	}
	return failed
}
