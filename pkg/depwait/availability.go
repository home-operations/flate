package depwait

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// DataDependency MUST retain the authored reference with its captured facts.
type DataDependency struct {
	manifest.Reference

	Input  store.ReferenceFacts
	Result values.Result
}

type availability struct {
	Err     error
	Kind    schedule.Kind
	Reason  manifest.DiagnosticReason
	Blocked []manifest.NamedResource
	Failed  []manifest.NamedResource
	Omit    bool
	Result  values.Result
}

// Available waits for offline materializers without making their failure a required cause.
//
//nolint:gocritic // The SDK accepts a value snapshot; a pointer would share caller-owned policy fields.
func (w *Waiter) Available(ctx context.Context, dep DataDependency, level manifest.DrainLevel) availability {
	if message := w.Store.Preflight(); message != "" {
		return availability{Err: manifest.NewError(manifest.ErrInput, nil, message)}
	}
	result, err := values.Classify(new(dep.Reference), new(dep.Input))
	if err != nil {
		return availability{Err: err}
	}
	dep.Result = result
	readable := result.Disposition(w.Missing) == values.UseData
	settledData := dep.Input.Accepted || dep.Input.Original != nil
	var pending []manifest.NamedResource
	if !readable && !settledData {
		pending = w.Store.UnsettledProducers(dep.NamedResource)
	}
	allowed := dep.Optional || result.AdmitMissing(w.Missing)
	var blocked []manifest.NamedResource
	for _, producer := range dep.Input.Materializers {
		if producer == w.Parent || settledData || readable && !w.blocksParent(producer) {
			continue
		}
		cl := w.Classify(ctx, manifest.DependencyRef{NamedResource: producer}, level)
		unpublished := cl.Kind == schedule.Terminal && level < manifest.DrainForce && slices.Contains(pending, producer)
		if cl.Kind == schedule.Blocked && !readable || unpublished {
			blocked = append(blocked, producer)
		}
		if failure, failed := w.materializerFailure(allowed, readable, producer, &cl); failed {
			failure.Result = result
			return failure
		}
	}
	if len(blocked) != 0 {
		return availability{Kind: schedule.Blocked, Blocked: blocked, Result: result}
	}
	if readable {
		return availability{Kind: schedule.Terminal, Result: result}
	}
	return w.availableAbsent(ctx, &dep, level, allowed)
}

func (w *Waiter) materializerFailure(
	allowed, readable bool,
	producer manifest.NamedResource,
	cl *Classification,
) (availability, bool) {
	if cl.Err != nil {
		return availability{Err: cl.Err}, true
	}
	if cl.Kind != schedule.DependencyFailed || allowed {
		return availability{}, false
	}
	status, _ := w.Store.GetStatus(producer)
	if readable && status.Status != store.StatusFailed {
		return availability{}, false
	}
	if status.Status == store.StatusFailed && !w.blocksParent(producer) {
		return availability{}, false
	}
	if status.Status == store.StatusFailed {
		cl.Message, cl.Causes = "not ready", nil
	}
	return availability{
		Kind:   cl.Kind,
		Reason: cl.DiagnosticReason(producer),
		Failed: []manifest.NamedResource{producer},
	}, true
}

func (w *Waiter) availableAbsent(
	ctx context.Context,
	dep *DataDependency,
	level manifest.DrainLevel,
	allowed bool,
) availability {
	if dep.Result.State() == values.Absent && len(dep.Input.Materializers) == 0 && (!allowed || dep.Input.Offline) {
		cl := w.Classify(ctx, dep.DependencyRef, level)
		if cl.Err != nil {
			return availability{Err: cl.Err}
		}
		if cl.Kind == schedule.Blocked {
			return availability{Kind: cl.Kind, Blocked: []manifest.NamedResource{dep.NamedResource}, Result: dep.Result}
		}
	}
	if allowed {
		return availability{Kind: schedule.Terminal, Omit: true, Result: dep.Result}
	}
	return availability{
		Kind: schedule.DependencyFailed,
		Reason: manifest.DiagnosticReason{
			Subject: manifest.DiagnosticFor(dep.NamedResource, w.Store.GetObject(dep.NamedResource)),
			Message: dep.Result.Failure().Cause().Error(),
		},
		Result: dep.Result,
	}
}

func (w *Waiter) blocksParent(id manifest.NamedResource) bool {
	seen := map[manifest.NamedResource]bool{}
	pending := []manifest.NamedResource{id}
	for len(pending) != 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current == w.Parent {
			return true
		}
		if seen[current] {
			continue
		}
		seen[current] = true
		status, _ := w.Store.GetStatus(current)
		pending = append(pending, status.BlockedBy()...)
	}
	return false
}
