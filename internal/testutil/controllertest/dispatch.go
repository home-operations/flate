// Package controllertest drives controller fixtures through their task service.
package controllertest

import (
	"context"
	"testing"

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
	ReconcileNode func(context.Context, manifest.NamedResource, manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait)
}

// Reconcile submits one pass and reports whether it has no blocked dependencies.
func Reconcile(ctx context.Context, tb testing.TB, d Dispatch, drain manifest.DrainLevel) bool {
	tb.Helper()
	var terminal bool
	failures := d.Tasks.Failures()
	d.Tasks.Go(ctx, "test/"+d.ID.String(), func(ctx context.Context) {
		out, _ := d.ReconcileNode(ctx, d.ID, manifest.ReconcileAttempt{Drain: drain})
		terminal = out.Kind != schedule.Blocked
	})
	d.Tasks.BlockTillDone()
	if added := d.Tasks.Failures() - failures; added > 0 {
		tb.Errorf("reconcile %s: %d task failures", d.ID, added)
	}
	return terminal
}

// DispatchToFixpoint escalates drain levels and returns the final store status.
func DispatchToFixpoint(ctx context.Context, tb testing.TB, d Dispatch) store.StatusInfo {
	tb.Helper()
	for _, drain := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
		if Reconcile(ctx, tb, d, drain) {
			break
		}
	}
	info, _ := d.Store.GetStatus(d.ID)
	return info
}
