package depwait

import (
	"context"
	"errors"
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
	Result  values.Result
}

// Available MUST combine structural settlement with the captured values decision.
//
//nolint:gocritic // The SDK accepts a value snapshot; a pointer would share caller-owned policy fields.
func (w *Waiter) Available(ctx context.Context, dep DataDependency, level manifest.DrainLevel) availability {
	if message := w.Store.Preflight(); message != "" {
		return availability{Err: manifest.NewError(manifest.ErrInput, nil, message)}
	}
	request := values.ResolveRequest{Reference: dep.Reference, Facts: dep.Input, Permission: w.Missing}
	dep.Result = values.Resolve(&request)
	if err := w.settleMaterializers(ctx, &dep, level, &request); err != nil {
		return availability{Err: err}
	}
	if err := w.settleAbsent(ctx, &dep, level, &request); err != nil {
		return availability{Err: err}
	}
	failure, _ := errors.AsType[*manifest.DependencyFailedError](request.Settlement.Failure)
	result := values.Resolve(&request)
	switch result.Effect() {
	case values.Deferred:
		return availability{Kind: schedule.Blocked, Blocked: result.Pending(), Result: result}
	case values.Used, values.Omitted:
		return availability{Kind: schedule.Terminal, Result: result}
	case values.Failed:
		var failed []manifest.NamedResource
		if failure != nil {
			failed = failure.Failed
		}
		return availability{
			Kind:   schedule.DependencyFailed,
			Failed: failed,
			Result: result,
			Reason: manifest.DiagnosticReason{
				Subject: manifest.DiagnosticFor(dep.NamedResource, w.Store.GetObject(dep.NamedResource)),
				Message: result.Cause(),
			},
		}
	default:
		panic("depwait: unresolved values assessment")
	}
}

func (w *Waiter) settleMaterializers(
	ctx context.Context, dep *DataDependency, level manifest.DrainLevel, request *values.ResolveRequest,
) error {
	if dep.Input.Accepted || dep.Input.Original != nil {
		return nil
	}
	materialized := dep.Result.Materialized()
	var unpublished []manifest.NamedResource
	if !materialized {
		unpublished = w.Store.UnsettledProducers(dep.NamedResource)
	}
	for _, producer := range dep.Input.Materializers {
		if producer == w.Parent || materialized && !w.blocksParent(producer) {
			continue
		}
		cl := w.Classify(ctx, manifest.DependencyRef{NamedResource: producer}, level)
		if cl.Err != nil {
			return cl.Err
		}
		pending := cl.Kind == schedule.Terminal && level < manifest.DrainForce && slices.Contains(unpublished, producer)
		if cl.Kind == schedule.Blocked && !materialized || pending {
			request.Settlement.Pending = append(request.Settlement.Pending, producer)
		}
		w.captureMaterializerFailure(producer, dep, &cl, request)
	}
	if len(request.Settlement.Pending) != 0 && !request.Settlement.BlocksConsumer {
		request.Settlement.Failure = nil
	}
	return nil
}

func (w *Waiter) captureMaterializerFailure(
	producer manifest.NamedResource, dep *DataDependency, cl *Classification, request *values.ResolveRequest,
) {
	if cl.Kind != schedule.DependencyFailed {
		return
	}
	status, _ := w.Store.GetStatus(producer)
	blocks := w.blocksParent(producer)
	if !dep.Result.RequiresData() && !blocks {
		return
	}
	materialized := dep.Result.Materialized()
	if materialized && (status.Status != store.StatusFailed || !blocks) {
		return
	}
	if status.Status == store.StatusFailed {
		cl.Message, cl.Causes = "not ready", nil
	}
	failure, _ := errors.AsType[*manifest.DependencyFailedError](request.Settlement.Failure)
	if failure == nil {
		failure = &manifest.DependencyFailedError{
			Parent: w.Parent, Subject: manifest.DiagnosticFor(w.Parent, w.Store.GetObject(w.Parent)),
			Reasons: make(map[manifest.NamedResource]manifest.DiagnosticReason),
		}
	}
	failedID := producer
	reason := cl.DiagnosticReason(producer)
	if !blocks && status.Status == store.StatusFailed {
		failedID = dep.NamedResource
		previous := failure.Reasons[failedID]
		reason = manifest.DiagnosticReason{
			Subject: manifest.DiagnosticFor(failedID, w.Store.GetObject(failedID)),
			Message: dep.Result.Cause(),
			Causes:  append(previous.Causes, reason),
		}
	}
	if _, exists := failure.Reasons[failedID]; !exists {
		failure.Failed = append(failure.Failed, failedID)
	}
	failure.Reasons[failedID] = reason
	request.Settlement.Failure = failure
	request.Settlement.BlocksConsumer = request.Settlement.BlocksConsumer || blocks
}

func (w *Waiter) settleAbsent(
	ctx context.Context, dep *DataDependency, level manifest.DrainLevel, request *values.ResolveRequest,
) error {
	assessment := &dep.Result
	needsInput := assessment.NeedsInput() || assessment.RequiresData() && assessment.Data() == nil
	if !needsInput {
		return nil
	}
	omittedProduction := assessment.Effect() == values.Omitted && len(dep.Input.Materializers) != 0
	//nolint:nilerr // Settlement retains domain failures for the final values decision.
	if omittedProduction || len(request.Settlement.Pending) != 0 || request.Settlement.Failure != nil {
		return nil
	}
	if assessment.Effect() != values.Failed && !dep.Input.Offline {
		return nil
	}
	cl := w.Classify(ctx, dep.DependencyRef, level)
	if cl.Err != nil {
		return cl.Err
	}
	if cl.Kind == schedule.Terminal {
		cl = w.classifyAbsent(&dep.DependencyRef, level)
	}
	if cl.Kind == schedule.Blocked {
		request.Settlement.Pending = append(request.Settlement.Pending, dep.NamedResource)
	}
	if cl.Kind != schedule.DependencyFailed {
		return nil
	}
	cause := assessment.Err()
	if domain, ok := errors.AsType[*manifest.Error](cause); ok {
		cause = domain.Cause()
	}
	message := assessment.Cause()
	if cause != nil {
		message = cause.Error()
	}
	failure := &manifest.DependencyFailedError{
		Parent: w.Parent, Subject: manifest.DiagnosticFor(w.Parent, w.Store.GetObject(w.Parent)),
		Failed: []manifest.NamedResource{dep.NamedResource},
		Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{dep.NamedResource: {
			Subject: manifest.DiagnosticFor(
				dep.NamedResource,
				w.Store.GetObject(dep.NamedResource),
			),
			Message: message,
		}},
	}
	request.Settlement.Failure = manifest.NewError(manifest.ErrObjectNotFound, failure, failure.Error())
	return nil
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
