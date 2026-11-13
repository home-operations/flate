package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func reconcileConstruction(
	ctx context.Context,
	pending []manifest.NamedResource,
	controller *sourcectrl.Controller,
	tasks *task.Service,
) error {
	if len(pending) == 0 {
		return nil
	}

	phase := tasks.Fork()
	d := &constructionDispatcher{controller: controller, local: make(map[manifest.NamedResource]bool)}
	for _, id := range pending {
		d.local[id] = true
	}
	d.scheduler = schedule.New(phase, d)
	unsubscribe := controller.Store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		d.scheduler.OnArrival(id, controller.Owns(id))
	}, false)
	defer unsubscribe()
	publication := func(manifest.NamedResource, any) { d.scheduler.OnPublication() }
	offStatus := controller.Store.AddListener(store.EventStatusUpdated, publication, false)
	defer offStatus()
	offArtifact := controller.Store.AddListener(store.EventArtifactUpdated, publication, false)
	defer offArtifact()
	d.scheduler.Seed(pending)
	if err := d.scheduler.Run(ctx); err != nil {
		return err
	}
	if n := phase.Failures(); n != 0 {
		return fmt.Errorf("%d construction task(s) panicked; check logs", n)
	}
	var failures []error
	for _, id := range pending {
		if status, _ := controller.Store.GetStatus(id); status.Status == store.StatusFailed {
			failures = append(failures, manifest.NewError(manifest.ErrFlux, nil, status.String()))
		}
	}
	return errors.Join(failures...)
}

type constructionDispatcher struct {
	controller *sourcectrl.Controller
	scheduler  *schedule.Scheduler
	local      map[manifest.NamedResource]bool
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
	object := d.controller.Store.GetObject(id)
	composed := false
	if facts, ok := object.(interface {
		Composition() manifest.ArtifactComposition
	}); ok {
		composed = len(facts.Composition().Includes) != 0
	}
	if d.local[id] && !composed {
		if err := d.controller.Construct(ctx, id); err != nil {
			d.controller.Store.UpdateFailure(id, err)
		}
		return schedule.Result{}
	}
	if prior, ok := previous.Assessment.(constructionExecution); ok {
		previous.Assessment = prior.Execution
	}
	execution := d.controller.ReconcileNode(ctx, id, level, previous)
	for _, dependency := range execution.Blocked {
		d.scheduler.OnArrival(dependency, d.controller.Owns(dependency))
	}
	return schedule.Result{Assessment: constructionExecution{execution}}
}

func (d *constructionDispatcher) Sweep(_ context.Context, _ bool) []schedule.NodeID {
	return slices.Collect(d.controller.Store.Sweep())
}

func (o *Orchestrator) finishInitialAdmission(ctx context.Context) error {
	for {
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
