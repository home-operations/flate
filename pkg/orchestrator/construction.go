package orchestrator

import (
	"context"
	"errors"
	"fmt"

	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
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
	failures := make([]error, len(pending))
	phase := tasks.Fork()
	for i, id := range pending {
		phase.Go(ctx, "construct source", func(ctx context.Context) { failures[i] = controller.Construct(ctx, id) })
	}
	phase.BlockTillDone()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	if n := phase.Failures(); n > 0 {
		return errors.Join(
			errors.Join(failures...),
			fmt.Errorf("%d task(s) panicked without per-resource attribution; check logs", n),
		)
	}
	return errors.Join(failures...)
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
		if o.selection == nil {
			result, err = o.session.Run(ctx)
		} else {
			result, err = o.session.Freeze(ctx)
		}
		if result != nil || err != nil {
			return result, err
		}
	}
}
