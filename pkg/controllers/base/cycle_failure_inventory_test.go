package base_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestExecution_PreflightDuringFailureInventory(t *testing.T) {
	t.Parallel()
	st, tasks := store.New(), task.NewBounded(2)
	own := &manifest.Kustomization{Name: "a", Namespace: "ns"}
	peer := &manifest.Kustomization{Name: "b", Namespace: "ns"}
	input := &manifest.ConfigMap{Name: "input", Namespace: "ns", Data: map[string]any{"value": "before"}}
	child := &manifest.ConfigMap{Name: "output", Namespace: "ns"}
	st.AddObjects([]manifest.BaseManifest{own, peer, input})
	c := base.New(st, tasks)
	prior := controllertest.NewExecution(c, own.Named())
	defer prior.Close()
	prior.Dispatch(t.Context(), manifest.DrainNone, prior,
		func(e *base.Execution, ctx context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
			e.Reads.GetObject(input.Named())
			_, _, next := e.Reads.GetEmission(own.Named())
			defer next.Finish()
			next.Children, next.Complete = []store.Child{{Obj: child}}, true
			return e.Emit(ctx, own.Named(), &next, nil)
		})
	assert.Equal(t, prior.Valid(), true)
	replacement := *input
	replacement.Data = map[string]any{"value": "after"}
	st.AddObject(&replacement)
	assert.Equal(t, prior.Valid(), false)
	st.UpdateStatus(peer.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "peer failed"})
	publishing, published := make(chan struct{}), make(chan struct{})
	const message = "dependency cycle detected: a → b → a"
	tasks.Go(t.Context(), "publish-cycle", func(context.Context) {
		tasks.YieldSlot(func() { <-publishing })
		st.SetPreflight(peer.Named(), message)
		st.SetPreflight(own.Named(), message)
		close(published)
	})
	var crossed atomic.Bool
	stop := st.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == own.Named() && crossed.CompareAndSwap(false, true) {
			close(publishing)
			tasks.YieldSlot(func() { <-published })
		}
	}, false)
	defer stop()
	var first, latest *base.Execution
	attempts := 0
	d := controllertest.Dispatch{
		Tasks: tasks,
		Store: st,
		ID:    own.Named(),
		ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel, _ schedule.Result) *base.Execution {
			e := controllertest.NewExecution(c, id)
			attempts++
			if first == nil {
				first = e
			}
			latest = e
			e.Dispatch(
				ctx,
				level,
				e,
				func(e *base.Execution, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
					assert.Equal(t, e.Reads.Preflight(), "")
					return e.Require(ctx, []manifest.DependencyRef{{NamedResource: peer.Named()}}, level)
				},
			)
			return e
		},
	}
	info := controllertest.DispatchToFixpoint(t.Context(), t, d)
	assert.Equal(t, crossed.Load(), true)
	assert.Equal(t, attempts, 2)
	assert.Equal(t, first.Valid(), false)
	assert.Equal(t, latest.Valid(), true)
	assert.Equal(t, info.Outcome(), store.OutcomeFailed)
	assert.Equal(t, info.String(), message)
	assert.Equal(t, info.IsDependencyFailure(), false)
	assert.Equal(t, st.GetObject(child.Named()) == nil, true)
	output, ok := st.GetArtifact(own.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "missing final failure inventory")
	assert.Equal(t, output.Complete, true)
	assert.Equal(t, output.OutputReads, &latest.Reads)
	assert.Equal(t, len(output.Children), 0)
	assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
}
