package schedule_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type registeredDispatcher struct {
	controller *base.Controller
	reconcile  func(context.Context, *base.Execution, schedule.NodeID, manifest.DrainLevel) error
}

func (d *registeredDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	e := controllertest.NewExecution(d.controller, id)
	e.Dispatch[*manifest.Kustomization](ctx, level, func(ctx context.Context, obj *manifest.Kustomization, level manifest.DrainLevel) error {
		return d.reconcile(ctx, e, obj.Named(), level)
	})
	return schedule.Result{Assessment: e}
}

func (d *registeredDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	return slices.Collect(d.controller.Store.Sweep())
}

func registeredScheduler(t *testing.T, objects []manifest.BaseManifest, reconcile func(context.Context, *base.Execution, schedule.NodeID, manifest.DrainLevel) error) (*schedule.Scheduler, *store.Store) {
	t.Helper()
	st, tasks := store.New(), task.NewBounded(4)
	st.AddObjects(objects)
	c := base.New(st, tasks, "scheduler-test")
	c.StartLifecycle()
	t.Cleanup(c.Close)
	s := schedule.New(tasks, &registeredDispatcher{controller: c, reconcile: reconcile})
	c.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) })
	for _, event := range []store.EventKind{store.EventArtifactUpdated, store.EventStatusUpdated} {
		c.AddListener(event, func(manifest.NamedResource, any) { s.OnPublication() })
	}
	return s, st
}

func TestScheduler_RegisteredDependencyGraphs(t *testing.T) {
	t.Parallel()
	chain := make(map[string][]string, 40)
	for i := range 40 {
		chain[fmt.Sprintf("node-%02d", i)] = []string{fmt.Sprintf("node-%02d", i+1)}
	}
	for _, tc := range []struct {
		name  string
		graph map[string][]string
		want  store.Status
	}{
		{name: "linear", graph: map[string][]string{"a": {"b"}, "b": {"c"}, "c": nil}, want: store.StatusReady},
		{name: "diamond", graph: map[string][]string{"d": {"b", "c"}, "b": {"a"}, "c": {"a"}, "a": nil}, want: store.StatusReady},
		{name: "dangling", graph: map[string][]string{"a-top": {"z-leaf"}, "z-leaf": {"absent"}}, want: store.StatusFailed},
		{name: "cycle", graph: map[string][]string{"a": {"b"}, "b": {"a"}}, want: store.StatusFailed},
		{name: "long dangling chain", graph: chain, want: store.StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			objects := make([]manifest.BaseManifest, 0, len(tc.graph))
			counts := make(map[string]*atomic.Int64, len(tc.graph))
			for name := range tc.graph {
				objects = append(objects, &manifest.Kustomization{Name: name, Namespace: "ns"})
				counts[name] = &atomic.Int64{}
			}
			s, st := registeredScheduler(t, objects, func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
				deps := make([]manifest.DependencyRef, 0, len(tc.graph[id.Name]))
				for _, dep := range tc.graph[id.Name] {
					deps = append(deps, manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", dep)})
				}
				if err := e.Require(ctx, deps, level); err != nil {
					return err
				}
				counts[id.Name].Add(1)
				return nil
			})
			if tc.name == "cycle" {
				for _, obj := range objects {
					st.SetPreflight(obj.Named(), "dependency cycle detected")
				}
			}
			assert.NoError(t, s.Run(t.Context()))
			for name := range tc.graph {
				status, ok := st.GetStatus(testutil.KustomizationID("ns", name))
				assert.Equal(t, ok, true)
				assert.Equal(t, status.Status, tc.want)
				if tc.want == store.StatusFailed && status.Message == "" {
					t.Errorf("%s: terminal failure has no diagnostic", name)
				}
				wantRenders := int64(0)
				if tc.want == store.StatusReady {
					wantRenders = 1
				}
				assert.Equal(t, counts[name].Load(), wantRenders)
			}
			assert.Equal(t, len(s.Results()), len(tc.graph))
		})
	}
}

func TestDispatchNode_SelfStatusSettlement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, expression string
		wantAssessments  int64
		feedback         bool
	}{
		{name: "bookkeeping", wantAssessments: 1},
		{name: "lazy self", expression: `dep.metadata.name == "dependency"`, wantAssessments: 1},
		{name: "self metadata", expression: `self.metadata.namespace == dep.metadata.namespace`, wantAssessments: 1},
		{name: "self conditions", expression: `self.status.conditions.size() >= 0`, wantAssessments: 2},
		{name: "feedback", expression: `self.status.conditions.size() >= 0`, feedback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}
			dependency := &manifest.Kustomization{Name: "dependency", Namespace: "ns"}
			var assessments, renders atomic.Int64
			s, st := registeredScheduler(t, []manifest.BaseManifest{consumer, dependency}, func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
				if id == dependency.Named() {
					return nil
				}
				count := assessments.Add(1)
				e.SetPendingUnlessReady(id, "working")
				if err := e.Require(ctx, []manifest.DependencyRef{{NamedResource: dependency.Named(), ReadyExpr: tc.expression}}, level); err != nil {
					return err
				}
				if tc.feedback {
					e.Store.UpdateStatus(id, store.StatusReady, "", fmt.Sprintf("feedback %d", count))
					return nil
				}
				fp, reused, err := e.FingerprintDedup(id, "unchanged output")
				if reused || err != nil {
					return err
				}
				renders.Add(1)
				_, output := e.Reads.GetEmission(id)
				output.Fingerprint, output.Complete = fp, true
				e.Emit(id, &output)
				output.Finish()
				return nil
			})
			st.UpdateStatus(dependency.Named(), store.StatusReady, "", "")
			s.Seed([]schedule.NodeID{consumer.Named()})
			err := s.Run(t.Context())
			if tc.feedback {
				if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") {
					t.Fatalf("feedback did not reach its bound: %v", err)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, assessments.Load(), tc.wantAssessments)
			assert.Equal(t, renders.Load(), int64(1))
		})
	}
}

func TestScheduler_RegisteredFailureRecovery(t *testing.T) {
	t.Parallel()
	for _, data := range []bool{false, true} {
		t.Run(fmt.Sprintf("data=%v", data), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				consumer := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}
				driver := &manifest.Kustomization{Name: "driver", Namespace: "ns"}
				dependency := &manifest.Kustomization{Name: "dependency", Namespace: "ns"}
				read, published, failed := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var runs atomic.Int64
				var st *store.Store
				s, current := registeredScheduler(t, []manifest.BaseManifest{consumer, driver, dependency}, func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
					if id == dependency.Named() {
						return fmt.Errorf("%w: initial failure", manifest.ErrInput)
					}
					if id == driver.Named() {
						e.Tasks.YieldSlot(func() { <-read })
						if data {
							st.AddObject(&manifest.ConfigMap{Name: "late", Namespace: "ns", Data: map[string]any{"value": "ready"}})
						} else {
							st.UpdateStatus(dependency.Named(), store.StatusReady, "", "")
						}
						close(published)
						return nil
					}
					if !data {
						e.Tasks.YieldSlot(func() { <-failed })
					}
					run := runs.Add(1)
					var err error
					if data {
						if e.Reads.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "late"}) == nil {
							err = fmt.Errorf("%w: data unavailable", manifest.ErrInput)
						}
					} else {
						err = e.Require(ctx, []manifest.DependencyRef{{NamedResource: dependency.Named()}}, level)
					}
					if run == 1 {
						close(read)
						e.Tasks.YieldSlot(func() { <-published })
					}
					return err
				})
				st = current
				u := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
					if id == dependency.Named() && payload.(store.StatusInfo).Status == store.StatusFailed {
						close(failed)
					}
				}, false)
				defer u()
				assert.NoError(t, s.Run(t.Context()))
				status, _ := st.GetStatus(consumer.Named())
				assert.Equal(t, status.Status, store.StatusReady)
				assert.Equal(t, runs.Load(), int64(2))
			})
		})
	}
}

func TestScheduler_RegisteredIrrelevantPublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		consumer, driver := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}, &manifest.Kustomization{Name: "driver", Namespace: "ns"}
		read, published := make(chan struct{}), make(chan struct{})
		var renders atomic.Int64
		var st *store.Store
		s, current := registeredScheduler(t, []manifest.BaseManifest{consumer, driver}, func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
			if id == driver.Named() {
				e.Tasks.YieldSlot(func() { <-read })
				st.AddObject(&manifest.Secret{Name: "irrelevant", Namespace: "ns"})
				close(published)
				return nil
			}
			renders.Add(1)
			e.Reads.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "optional"})
			close(read)
			e.Tasks.YieldSlot(func() { <-published })
			return nil
		})
		st = current
		assert.NoError(t, s.Run(t.Context()))
		assert.Equal(t, renders.Load(), int64(1))
		assert.Equal(t, len(s.Results()), 2)
	})
}

func TestScheduler_RegisteredFixpointBound(t *testing.T) {
	t.Parallel()
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked=%v", blocked), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				x, y := &manifest.Kustomization{Name: "x", Namespace: "ns"}, &manifest.Kustomization{Name: "y", Namespace: "ns"}
				started, held := make(chan struct{}, 2), make(chan struct{}, 2)
				release := make(chan struct{})
				var active, total atomic.Int64
				counts := map[schedule.NodeID]*atomic.Int64{x.Named(): {}, y.Named(): {}}
				s, st := registeredScheduler(t, []manifest.BaseManifest{x, y}, func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
					active.Add(1)
					defer active.Add(-1)
					run, count := counts[id].Add(1), total.Add(1)
					other := x.Named()
					if id == other {
						other = y.Named()
					}
					e.Reads.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: other.Name})
					if run == 1 {
						started <- struct{}{}
						e.Tasks.YieldSlot(func() { <-release })
					}
					if blocked && count >= 80 {
						held <- struct{}{}
						<-ctx.Done()
					}
					_, next := e.Reads.GetEmission(id)
					defer next.Finish()
					child := &manifest.ConfigMap{Name: id.Name, Namespace: "ns", Data: map[string]any{"value": run}}
					next.Children = []store.Child{{Obj: child, Doc: child.Snapshot()}}
					next.Manifests, next.Complete = []map[string]any{child.Snapshot()}, true
					e.Emit(id, &next)
					if blocked {
						return e.Require(ctx, []manifest.DependencyRef{{NamedResource: other}}, level)
					}
					return nil
				})
				c := base.New(st, nil, "seed")
				for _, obj := range []*manifest.Kustomization{x, y} {
					child := &manifest.ConfigMap{Name: obj.Name, Namespace: "ns", Data: map[string]any{"value": int64(0)}}
					c.Emit(obj.Named(), &store.Emission{Children: []store.Child{{Obj: child, Doc: child.Snapshot()}}, Complete: true})
				}
				done := make(chan error, 1)
				go func() { done <- s.Run(ctx) }()
				<-started
				<-started
				assert.Equal(t, active.Load(), int64(2))
				close(release)
				if blocked {
					<-held
					synctest.Wait()
					if got := active.Load(); got < 1 || got > 2 {
						t.Fatalf("active feedback executions = %d, want one or two", got)
					}
					cancel()
				}
				err := <-done
				assert.Equal(t, active.Load(), int64(0))
				if blocked {
					assert.Equal(t, errors.Is(err, context.Canceled), true)
					if got := total.Load(); got < 80 || got > 81 {
						t.Fatalf("feedback executions = %d, want the threshold plus at most one in-flight peer", got)
					}
				} else {
					if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil {
						t.Fatalf("registered terminal feedback did not hit its bound: error=%v context=%v runs=%d", err, ctx.Err(), total.Load())
					}
					for id, runs := range counts {
						if got := runs.Load(); got > 33 {
							t.Errorf("%s: %d executions exceeds the initial execution plus 32 successors", id, got)
						}
					}
				}
			})
		})
	}
}

func TestScheduler_RegisteredSelection(t *testing.T) {
	t.Parallel()
	for _, matching := range []bool{false, true} {
		t.Run(fmt.Sprintf("matching_bucket=%v", matching), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				selector, driver := &manifest.Kustomization{Name: "selector", Namespace: "ns"}, &manifest.Kustomization{Name: "driver", Namespace: "ns"}
				read, published := make(chan struct{}), make(chan struct{})
				var runs atomic.Int64
				var st *store.Store
				s, current := registeredScheduler(t, []manifest.BaseManifest{selector, driver}, func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
					if id == driver.Named() {
						e.Tasks.YieldSlot(func() { <-read })
						if matching {
							st.AddObject(&manifest.ResourceSetInputProvider{Name: "late", Namespace: "ns"})
						} else {
							st.AddObject(&manifest.Secret{Name: "unrelated", Namespace: "ns"})
						}
						close(published)
						return nil
					}
					run := runs.Add(1)
					providers := e.Reads.ListObjects(manifest.FluxOperatorDomain, manifest.KindResourceSetInputProvider)
					if run == 1 {
						assert.Equal(t, len(providers), 0)
						close(read)
						e.Tasks.YieldSlot(func() { <-published })
					} else {
						assert.Equal(t, len(providers), 1)
					}
					return nil
				})
				st = current
				assert.NoError(t, s.Run(t.Context()))
				want := int64(1)
				if matching {
					want = 2
				}
				assert.Equal(t, runs.Load(), want)
			})
		})
	}
}

func TestScheduler_RegisteredOverlappingLifetimes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		consumer, driver := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}, &manifest.Kustomization{Name: "driver", Namespace: "ns"}
		input := &manifest.ConfigMap{Name: "settings", Namespace: "ns", Data: map[string]any{"value": "old"}}
		accepted, entered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var runs atomic.Int64
		var first atomic.Pointer[base.Execution]
		var st *store.Store
		s, current := registeredScheduler(t, []manifest.BaseManifest{consumer, driver, input}, func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
			if id == driver.Named() {
				e.Tasks.YieldSlot(func() { <-accepted })
				st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"value": "new"}})
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
			_, next := e.Reads.GetEmission(id)
			defer next.Finish()
			child := &manifest.ConfigMap{Name: "rendered", Namespace: "ns", Data: map[string]any{"value": "identical"}}
			next.Children, next.Manifests, next.Complete = []store.Child{{Obj: child, Doc: child.Snapshot()}}, []map[string]any{child.Snapshot()}, true
			e.Emit(id, &next)
			return nil
		})
		st = current
		var signaled atomic.Bool
		u := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
			if id == consumer.Named() && payload.(store.StatusInfo).Status == store.StatusReady && signaled.CompareAndSwap(false, true) {
				close(accepted)
			}
		}, false)
		defer u()
		done := make(chan error, 1)
		go func() { done <- s.Run(t.Context()) }()
		<-entered
		previous := first.Load()
		output, ok := st.GetArtifact(consumer.Named()).(*store.Emission)
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
		consumer, driver := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}, &manifest.Kustomization{Name: "driver", Namespace: "ns"}
		dependency := testutil.KustomizationID("ns", "late")
		read, published, ready := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var renders, dependencies atomic.Int64
		s, st := registeredScheduler(t, []manifest.BaseManifest{consumer, driver}, func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
			if id == dependency {
				dependencies.Add(1)
				return nil
			}
			if id == driver.Named() {
				e.Tasks.YieldSlot(func() { <-read })
				docs := []map[string]any{
					{"apiVersion": "example.test/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "late", "namespace": "ns"}},
					{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "late", "namespace": "ns"}, "spec": map[string]any{"path": "./late"}},
				}
				_, next := e.Reads.GetEmission(id)
				defer next.Finish()
				late := &manifest.Kustomization{Name: "late", Namespace: "ns"}
				late.Path = "./late"
				foreign := &manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindKustomization, Namespace: "ns", Name: "late"}
				next.Children = []store.Child{{Obj: foreign, Doc: docs[0]}, {Obj: late, Doc: docs[1]}}
				next.Manifests, next.Complete = docs, true
				e.Emit(id, &next)
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
		})
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
		consumer, driver := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}, &manifest.Kustomization{Name: "driver", Namespace: "ns"}
		input := &manifest.ConfigMap{Name: "input", Namespace: "ns", Data: map[string]any{"value": "old"}}
		st, bodies := store.New(), task.NewBounded(2)
		st.AddObjects([]manifest.BaseManifest{consumer, driver, input})
		controller := base.New(st, bodies, "completed-publication")
		controller.StartLifecycle()
		defer controller.Close()
		completed, started, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
		tasks := &completedTasks{Service: bodies, name: "schedule/" + consumer.Named().String(), completed: completed}
		var active, renders atomic.Int64
		d := &registeredDispatcher{controller: controller, reconcile: func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
			active.Add(1)
			defer active.Add(-1)
			if id == driver.Named() {
				close(started)
				e.Tasks.YieldSlot(func() { <-completed })
				st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"value": "new"}})
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
			_, next := e.Reads.GetEmission(id)
			defer next.Finish()
			child := &manifest.ConfigMap{Name: "output", Namespace: "ns", Data: map[string]any{"value": captured.Data["value"]}}
			next.Children = []store.Child{{Obj: child, Doc: child.Snapshot()}}
			next.Manifests, next.Complete = []map[string]any{child.Snapshot()}, true
			e.Emit(id, &next)
			return nil
		}}
		s := schedule.New(tasks, d)
		controller.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) })
		for _, event := range []store.EventKind{store.EventArtifactUpdated, store.EventStatusUpdated} {
			controller.AddListener(event, func(manifest.NamedResource, any) { s.OnPublication() })
		}
		assert.NoError(t, s.Run(t.Context()))
		if got := renders.Load(); got != 2 {
			t.Fatalf("completed captured input scheduled %d executions, want exactly 2", got)
		}
		output, ok := st.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "output"})
		assert.Equal(t, ok, true)
		assert.Equal(t, output.Data["value"], any("new"))
		assert.Equal(t, active.Load(), int64(0))
	})
}
