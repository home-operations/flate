package schedule_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/schedule"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestScheduler_RegisteredOverlappingLifetimes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		consumer, driver := &manifest.Kustomization{
			Name:      "consumer",
			Namespace: "ns",
		}, &manifest.Kustomization{
			Name:      "driver",
			Namespace: "ns",
		}
		input := &manifest.ConfigMap{Name: "settings", Namespace: "ns", Data: map[string]any{"value": "old"}}
		accepted, entered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var runs atomic.Int64
		var first atomic.Pointer[base.Execution]
		var st *store.Store
		s, current := registeredScheduler(
			t,
			4,
			[]manifest.BaseManifest{consumer, driver, input},
			func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
				if id == driver.Named() {
					e.Tasks.YieldSlot(func() { <-accepted })
					st.AddObject(
						&manifest.ConfigMap{
							Name:      input.Name,
							Namespace: input.Namespace,
							Data:      map[string]any{"value": "new"},
						},
					)
					return nil
				}
				run := runs.Add(1)
				e.Reads.GetObject(input.Named())
				switch run {
				case 1:
					first.Store(e)
				case 2:
					close(entered)
					<-release
				}
				_, _, next := e.Reads.GetEmission(id)
				defer next.Finish()
				child := &manifest.ConfigMap{
					Name:      "rendered",
					Namespace: "ns",
					Data:      map[string]any{"value": "identical"},
				}
				next.Children, next.Manifests, next.Complete = []store.Child{
					{Obj: child, Doc: child.Snapshot()},
				}, testutil.DocumentOccurrences(
					[]map[string]any{child.Snapshot()},
				), true
				e.Emit(t.Context(), id, &next, nil)
				return nil
			},
		)
		st = current
		var signaled atomic.Bool
		u := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
			if id == consumer.Named() && payload.(store.StatusInfo).Status == store.StatusReady &&
				signaled.CompareAndSwap(false, true) {
				close(accepted)
			}
		}, false)
		defer u()
		done := make(chan error, 1)
		go func() { done <- s.Run(t.Context()) }()
		<-entered
		previous := first.Load()
		output, ok := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
		assert.Equal(t, ok, true)
		assert.Equal(t, output.OutputReads == &previous.Reads, true)
		assert.Equal(t, previous.Reads.Valid(), false)
		close(release)
		assert.NoError(t, <-done)
		assert.Equal(t, runs.Load(), int64(2))
		acceptedExecution := s.Results()[consumer.Named()].Assessment.(*base.Execution)
		assert.Equal(t, acceptedExecution == previous, false)
		assert.Equal(t, acceptedExecution.Valid(), true)
	})
}

func TestScheduler_RegisteredLateDeclaration(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		consumer, driver := &manifest.Kustomization{
			Name:      "consumer",
			Namespace: "ns",
		}, &manifest.Kustomization{
			Name:      "driver",
			Namespace: "ns",
		}
		dependency := testutil.KustomizationID("ns", "late")
		read, published, ready := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var renders, dependencies atomic.Int64
		s, st := registeredScheduler(
			t,
			4,
			[]manifest.BaseManifest{consumer, driver},
			func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
				if id == dependency {
					dependencies.Add(1)
					return nil
				}
				if id == driver.Named() {
					e.Tasks.YieldSlot(func() { <-read })
					docs := []map[string]any{
						{
							"apiVersion": "example.test/v1",
							"kind":       manifest.KindKustomization,
							"metadata":   map[string]any{"name": "late", "namespace": "ns"},
						},
						{
							"apiVersion": manifest.FluxKustomizeDomain + "/v1",
							"kind":       manifest.KindKustomization,
							"metadata":   map[string]any{"name": "late", "namespace": "ns"},
							"spec":       map[string]any{"path": "./late"},
						},
					}
					_, _, next := e.Reads.GetEmission(id)
					defer next.Finish()
					late := &manifest.Kustomization{Name: "late", Namespace: "ns"}
					late.Path = "./late"
					foreign := &manifest.RawObject{
						APIVersion: "example.test/v1",
						Kind:       manifest.KindKustomization,
						Namespace:  "ns",
						Name:       "late",
					}
					next.Children = []store.Child{{Obj: foreign, Doc: docs[0]}, {Obj: late, Doc: docs[1]}}
					next.Manifests, next.Complete = testutil.DocumentOccurrences(docs), true
					e.Emit(t.Context(), id, &next, nil)
					e.Tasks.YieldSlot(func() { <-ready })
					close(published)
					return nil
				}
				run := renders.Add(1)
				err := e.Require(ctx, []manifest.DependencyRef{{NamedResource: dependency}}, level)
				if run == 1 {
					close(read)
					e.Tasks.YieldSlot(func() { <-published })
				}
				return err
			},
		)
		u := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
			if id == dependency && payload.(store.StatusInfo).Status == store.StatusReady {
				close(ready)
			}
		}, false)
		defer u()
		assert.NoError(t, s.Run(t.Context()))
		status, _ := st.GetStatus(consumer.Named())
		assert.Equal(t, status.Status, store.StatusReady)
		assert.Equal(t, renders.Load(), int64(2))
		assert.Equal(t, dependencies.Load(), int64(1))
		assert.Equal(t, len(s.Results()), 3)
	})
}

type completedTasks struct {
	*task.Service

	name      string
	completed chan struct{}
	signaled  atomic.Bool
}

func (ts *completedTasks) Go(ctx context.Context, name string, fn func(context.Context)) {
	ts.Service.Go(ctx, name, func(ctx context.Context) {
		fn(ctx)
		if name == ts.name && ts.signaled.CompareAndSwap(false, true) {
			close(ts.completed)
		}
	})
}

func TestScheduler_RegisteredCompletedPublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		consumer, driver := &manifest.Kustomization{
			Name:      "consumer",
			Namespace: "ns",
		}, &manifest.Kustomization{
			Name:      "driver",
			Namespace: "ns",
		}
		input := &manifest.ConfigMap{Name: "input", Namespace: "ns", Data: map[string]any{"value": "old"}}
		st, bodies := store.New(), task.NewBounded(2)
		st.AddObjects([]manifest.BaseManifest{consumer, driver, input})
		controller := base.New(st, bodies)
		controller.StartLifecycle()
		completed, started, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
		tasks := &completedTasks{Service: bodies, name: "schedule/" + consumer.Named().String(), completed: completed}
		var active, renders atomic.Int64
		d := &registeredDispatcher{
			controller: controller,
			reconcile: func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
				active.Add(1)
				defer active.Add(-1)
				if id == driver.Named() {
					close(started)
					e.Tasks.YieldSlot(func() { <-completed })
					st.AddObject(
						&manifest.ConfigMap{
							Name:      input.Name,
							Namespace: input.Namespace,
							Data:      map[string]any{"value": "new"},
						},
					)
					e.Tasks.YieldSlot(func() { <-settled })
					return nil
				}
				if renders.Add(1) == 1 {
					e.Tasks.YieldSlot(func() { <-started })
					assert.Equal(t, active.Load(), int64(2))
				} else {
					assert.Equal(t, active.Load(), int64(2))
					defer close(settled)
				}
				captured, ok := e.Reads.Get[*manifest.ConfigMap](input.Named())
				assert.Equal(t, ok, true)
				e.Reads.RequireData(input.Named())
				_, _, next := e.Reads.GetEmission(id)
				defer next.Finish()
				child := &manifest.ConfigMap{
					Name:      "output",
					Namespace: "ns",
					Data:      map[string]any{"value": captured.Data["value"]},
				}
				next.Children = []store.Child{{Obj: child, Doc: child.Snapshot()}}
				next.Manifests, next.Complete = testutil.DocumentOccurrences([]map[string]any{child.Snapshot()}), true
				return e.Emit(t.Context(), id, &next, nil)
			},
		}
		s := schedule.New(tasks, d)
		defer st.AddListener(
			store.EventObjectAdded,
			func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) },
			true,
		)()
		defer st.AddListener(
			store.EventArtifactUpdated,
			func(manifest.NamedResource, any) { s.OnPublication() },
			true,
		)()
		defer st.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { s.OnPublication() }, true)()
		assert.NoError(t, s.Run(t.Context()))
		{
			got := renders.Load()
			assert.FatalIf(t, got != 2, "completed captured input scheduled %d executions, want exactly 2", got)
		}
		output, ok := st.Get[*manifest.ConfigMap](
			manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "output"},
		)
		assert.Equal(t, ok, true)
		assert.Equal(t, output.Data["value"], any("new"))
		assert.Equal(t, active.Load(), int64(0))
	})
}
