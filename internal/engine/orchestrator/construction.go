package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	sourcectrl "github.com/home-operations/flate/internal/engine/controllers/source"
	"github.com/home-operations/flate/internal/engine/discovery"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/pkg/manifest"
)

func reconcileConstruction(
	ctx context.Context,
	pending []manifest.NamedResource,
	controller *sourcectrl.Controller,
	tasks *task.Service,
) error {
	controllers := base.Controllers{Source: controller}
	dispatch := constructionDispatcher{controller: controller, controllers: &controllers}
	return dispatch.run(ctx, pending, nil, tasks)
}

type constructionDispatcher struct {
	controller  *sourcectrl.Controller
	controllers *base.Controllers
	scheduler   *schedule.Scheduler
	local       map[manifest.NamedResource]bool
}

func (d *constructionDispatcher) run(
	ctx context.Context,
	pending, materializers []manifest.NamedResource,
	tasks *task.Service,
) error {
	if len(pending) == 0 && len(materializers) == 0 {
		return nil
	}

	phase := tasks.Fork()
	d.local = make(map[manifest.NamedResource]bool, len(pending))
	for _, id := range pending {
		d.local[id] = true
	}
	d.scheduler = schedule.New(phase, d)
	unsubscribe := d.controller.Store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		d.scheduler.OnArrival(id, len(materializers) == 0 && d.controllers.Owns(id))
	}, false)
	defer unsubscribe()
	publication := func(manifest.NamedResource, any) { d.scheduler.OnPublication() }
	offStatus := d.controller.Store.AddListener(store.EventStatusUpdated, publication, false)
	defer offStatus()
	offArtifact := d.controller.Store.AddListener(store.EventArtifactUpdated, publication, false)
	defer offArtifact()
	d.scheduler.Seed(pending)
	d.scheduler.Seed(materializers)
	if err := d.scheduler.Run(ctx); err != nil {
		return err
	}
	if n := phase.Failures(); n != 0 {
		return fmt.Errorf("%d construction task(s) panicked; check logs", n)
	}
	var failures []error
	for _, id := range pending {
		if status, _ := d.controller.Store.GetStatus(id); status.Status == store.StatusFailed {
			failures = append(failures, manifest.NewError(manifest.ErrFlux, nil, status.String()))
		}
	}
	return errors.Join(failures...)
}

// Construction outputs MUST retain their subscriptions after this phase drains.
type constructionExecution struct{ *base.Execution }

func (e constructionExecution) Close() { e.Retire() }

func (d *constructionDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
	previous schedule.Result,
) schedule.Result {
	reconcile := d.controllers.ReconcileNode
	if d.local[id] {
		object := d.controller.Store.GetObject(id)
		if object == nil || !object.Composition().Composed() {
			if err := d.controller.Construct(ctx, id); err != nil {
				d.controller.Store.UpdateFailure(id, err)
			}
			return schedule.Result{}
		}
		reconcile = d.controller.ReconcileNode
	}
	if prior, ok := previous.Assessment.(constructionExecution); ok {
		previous.Assessment = prior.Execution
	}
	execution := reconcile(ctx, id, level, previous)
	if execution == nil {
		return schedule.Result{}
	}
	for _, dependency := range execution.Blocked {
		d.scheduler.OnArrival(dependency, d.controllers.Owns(dependency))
	}
	return schedule.Result{Assessment: constructionExecution{execution}}
}

func (d *constructionDispatcher) Sweep(_ context.Context, _ bool) []schedule.NodeID {
	return slices.Collect(d.controller.Store.Sweep())
}

func (o *Orchestrator) finishInitialAdmission(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := o.installAdmission(ctx); err != nil {
			return err
		}
		if len(o.session.PendingSources()) == 0 {
			return nil
		}
		if err := reconcileConstruction(ctx, o.session.PendingSources(), o.src, o.tasks); err != nil {
			return err
		}
		if err := o.prepareInvocation(ctx); err != nil {
			return err
		}
	}
}

func (o *Orchestrator) constructDiscovery(ctx context.Context) (*discovery.Result, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := reconcileConstruction(ctx, o.session.PendingSources(), o.src, o.tasks); err != nil {
			return nil, err
		}
		var result *discovery.Result
		var err error
		if o.selection.Snapshot().Output(manifest.NamedResource{}) {
			result, err = o.session.Run(ctx)
		} else {
			result, err = o.session.Freeze(ctx)
		}
		if result != nil || err != nil {
			return result, err
		}
	}
}
