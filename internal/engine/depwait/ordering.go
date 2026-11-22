package depwait

import (
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type orderingResult struct {
	err    error
	active bool
}

type orderingAssessment struct {
	reads *store.ReadSet
	nodes map[manifest.NamedResource]orderingResult
}

func (a *orderingAssessment) projectReadiness(
	ctx context.Context, waiter *Waiter, observed *dependencyObservation, level manifest.DrainLevel,
) error {
	if !observed.present || observed.info.Status == store.StatusFailed {
		return nil
	}
	if err := a.assess(ctx, waiter, observed.object, level); err != nil {
		return err
	}
	if observed.info.Status != store.StatusReady {
		observed.info.Status = store.StatusReady
		if observed.hasExpr {
			observed.info.Conditions = orderingReadyConditions(observed.info.Conditions)
		}
	}
	return nil
}

func (a *orderingAssessment) assess(
	ctx context.Context, waiter *Waiter, obj manifest.BaseManifest, level manifest.DrainLevel,
) error {
	id := obj.Named()
	if obj.Suspended() {
		return nil
	}
	if a.nodes == nil {
		a.nodes = map[manifest.NamedResource]orderingResult{waiter.Parent: {active: true}}
	}
	if prior, known := a.nodes[id]; known {
		if prior.active {
			if level == manifest.DrainForce {
				return manifest.NewError(manifest.ErrInput, nil, "not ready")
			}
			return &BlockedError{Deps: []manifest.NamedResource{id}}
		}
		return prior.err
	}

	a.nodes[id] = orderingResult{active: true}
	parent, materializer, parentObject := waiter.Parent, waiter.Materializer, waiter.orderingParent
	waiter.Parent, waiter.orderingParent = id, obj
	waiter.Materializer = false
	gates := OrderingGates(obj, nil)
	if slices.ContainsFunc(gates, func(dep manifest.DependencyRef) bool {
		return dep.ReadyExpr != nil && *dep.ReadyExpr != ""
	}) {
		waiter.orderingParent = waiter.Decision.OrderingDeclaration(a.reads, id, true)
	}
	var result gateResult
	for _, dep := range gates {
		var evaluation *orderingAssessment
		if waiter.Decision.EvaluationOnly(dep.NamedResource) {
			evaluation = a
		}
		cl, pending := waiter.classifyCaptured(ctx, &dep, level, evaluation)
		if cl.Kind == schedule.Blocked && evaluation == nil && a.reads.GetDeclaration(dep.NamedResource) == nil {
			pending = a.reads.UnsettledProducers(dep.NamedResource)
		}
		result.add(waiter, dep.NamedResource, &cl, pending)
	}
	waiter.Parent, waiter.Materializer, waiter.orderingParent = parent, materializer, parentObject
	err := result.err()
	a.nodes[id] = orderingResult{err: err}
	return err
}

// An evaluated declaration supplies the Ready condition without publishing a reconcile.
func orderingReadyConditions(conditions []store.Condition) []store.Condition {
	ready := store.Condition{
		Type: store.ConditionReady, Status: "True", Reason: store.ReasonSucceeded, ObservedGeneration: 1,
	}
	projected := slices.Clone(conditions)
	for i := range projected {
		if projected[i].Type == store.ConditionReady {
			projected[i] = ready
			return projected
		}
	}
	return append(projected, ready)
}

func (w *Waiter) orderingProducers(reads *store.ReadSet, id manifest.NamedResource) []manifest.NamedResource {
	unsettled := reads.UnsettledProducers(id)
	var pending []manifest.NamedResource
	for _, owner := range reads.DeclarationProducers(id) {
		reads.ControllerDemand(owner)
		if w.Decision.EvaluationOnly(owner) {
			continue
		}
		status, _ := reads.GetStatus(owner)
		if status.Status == store.StatusPending || slices.Contains(unsettled, owner) {
			pending = append(pending, owner)
		}
	}
	return pending
}
