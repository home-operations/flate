package base

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestExecution_PreflightInvalidatesSettlementRoutes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"pending", "resume", "gates"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			st, tasks := store.New(), task.NewBounded(2)
			own := &manifest.Kustomization{Name: "a", Namespace: "ns"}
			peer := &manifest.Kustomization{Name: "b", Namespace: "ns"}
			st.AddObjects([]manifest.BaseManifest{own, peer})
			st.UpdateStatus(own.Named(), store.StatusInfo{Status: store.StatusPending})
			c := New(st, tasks)
			e := gateExecution(c, own.Named())
			defer e.Close()
			dep := manifest.DependencyRef{NamedResource: peer.Named()}
			bodyCalls := 0
			if route == "gates" {
				e.Reads.Require(dep)
				e.Settle(t.Context(), manifest.DrainNone)
			} else {
				e.Dispatch(
					t.Context(),
					manifest.DrainNone,
					e,
					func(e *Execution, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
						bodyCalls++
						e.Reads.Require(dep)
						if route == "pending" {
							return e.AwaitData(
								ctx,
								DataWait{
									Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error {
										t.Error("stale pending settlement resumed")
										return nil
									},
								},
								level,
							)
						}
						return e.Require(ctx, []manifest.DependencyRef{dep}, level)
					},
				)
			}
			assert.Equal(t, e.Outcome(), schedule.Blocked)
			switch route {
			case "pending":
				_, ok := errors.AsType[*pendingDataError](e.pending)
				assert.Equal(t, ok, true)
			case "resume":
				assert.Equal(t, e.resume != nil, true)
			default:
				_, settles := e.pending.(interface {
					Settle(context.Context, manifest.DrainLevel) error
				})
				assert.Equal(t, settles || e.resume != nil, false)
			}
			beforeCalls := bodyCalls
			before, _ := st.GetStatus(own.Named())
			const message = "dependency cycle detected: a → b → a"
			published := make(chan struct{})
			tasks.Go(t.Context(), "publish-cycle", func(context.Context) {
				st.SetPreflight(peer.Named(), message)
				st.SetPreflight(own.Named(), message)
				close(published)
			})
			<-published
			tasks.BlockTillDone()
			assert.Equal(t, e.Valid(), false)
			e.Settle(t.Context(), manifest.DrainForce)
			after, _ := st.GetStatus(own.Named())
			assert.Equal(t, after, before)
			assert.Equal(t, e.Outcome(), schedule.Blocked)
			assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), own.Named()), true)
			assert.Equal(t, bodyCalls, beforeCalls)
			fresh := gateExecution(c, own.Named())
			defer fresh.Close()
			fresh.Reads.Require(dep)
			fresh.Settle(t.Context(), manifest.DrainForce)
			info, _ := st.GetStatus(own.Named())
			assert.Equal(t, info.Outcome(), store.OutcomeFailed)
			assert.Equal(t, info.String(), message)
			assert.Equal(t, info.IsDependencyFailure(), false)
			assert.Equal(t, fresh.Valid(), true)
			assert.Equal(t, errors.Is(fresh.Checkpoint(), manifest.ErrInput), true)
		})
	}
}
