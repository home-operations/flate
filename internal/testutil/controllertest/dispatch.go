// Package controllertest drives controller fixtures through their task service.
package controllertest

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Dispatch groups the caller-owned dependencies for a reconciliation.
type Dispatch struct {
	Tasks         *task.Service
	Store         *store.Store
	ID            manifest.NamedResource
	ReconcileNode func(context.Context, manifest.NamedResource, manifest.DrainLevel) *base.Execution
}

// Dispatch captures one reconciliation's assessment for the scheduler.
func (d Dispatch) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	return schedule.Result{Assessment: d.ReconcileNode(ctx, id, level)}
}

// Sweep delivers pending store publications and returns invalidated executions.
func (d Dispatch) Sweep(context.Context, bool) []schedule.NodeID {
	return slices.Collect(d.Store.Sweep())
}

// Reconcile submits one pass and reports whether it has no blocked dependencies.
func Reconcile(ctx context.Context, tb testing.TB, d Dispatch, drain manifest.DrainLevel) bool {
	tb.Helper()
	var terminal bool
	failures := d.Tasks.Failures()
	d.Tasks.Go(ctx, "test/"+d.ID.String(), func(ctx context.Context) {
		assessment := d.ReconcileNode(ctx, d.ID, drain)
		terminal = assessment.Outcome() != schedule.Blocked
		assessment.Close()
	})
	d.Tasks.BlockTillDone()
	if added := d.Tasks.Failures() - failures; added > 0 {
		tb.Errorf("reconcile %s: %d task failures", d.ID, added)
	}
	return terminal
}

// DispatchToFixpoint runs retained assessments to structural settlement.
func DispatchToFixpoint(ctx context.Context, tb testing.TB, d Dispatch) store.StatusInfo {
	tb.Helper()
	failures := d.Tasks.Failures()
	scheduler := schedule.New(d.Tasks, d)
	scheduler.Seed([]schedule.NodeID{d.ID})
	if err := scheduler.Run(ctx); err != nil {
		tb.Errorf("reconcile %s: %v", d.ID, err)
	}
	if added := d.Tasks.Failures() - failures; added > 0 {
		tb.Errorf("reconcile %s: %d task failures", d.ID, added)
	}
	info, _ := d.Store.GetStatus(d.ID)
	return info
}

// NewExecution initializes caller-owned captured reconciliation storage for a fixture.
func NewExecution(c *base.Controller, id manifest.NamedResource) *base.Execution {
	e := new(base.Execution)
	e.Init(c, id)
	return e
}

// Emit installs immutable fixture output without starting a reconciliation.
func Emit(ctx context.Context, c *base.Controller, id manifest.NamedResource, next *store.Emission) error {
	children := next.Children
	cloned := false
	if next.Reads.Valid() {
		for i, child := range children {
			if child.Obj == nil || child.Obj.Named() == id {
				continue
			}
			obj := manifest.MapDependsOn(child.Obj, c.DependencySubstitutions.Resolve, nil)
			if obj != child.Obj {
				if !cloned {
					next.Children = slices.Clone(children)
					cloned = true
				}
				next.Children[i].Obj = obj
			}
			if _, err := c.KeepEmitted(ctx, id, next.Children[i], next.Reads); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Store.RecordEmitted(id, next, nil)
	if cloned && !next.Complete {
		next.Children = children
	}
	if next.Complete {
		if info, ok := c.Store.GetStatus(id); ok && info.Status == store.StatusPending {
			message := store.ApplyingStatus(len(next.Manifests))
			if info != message {
				c.Store.UpdateStatus(id, message)
			}
		}
	}
	return nil
}
