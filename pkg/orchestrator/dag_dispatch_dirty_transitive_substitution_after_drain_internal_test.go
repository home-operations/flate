package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAGDispatch_DirtyTransitiveSubstitutionAfterDrain(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [consumer.yaml, bridge.yaml, pin.yaml]\n")
	testutil.WriteFile(t, root, "consumer.yaml", ksYAML("consumer", "consumer", "")+`  postBuild:
    substituteFrom: [{kind: ConfigMap, name: bridge-settings, optional: true}]
`)
	testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [../shared/consumer.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"shared/consumer.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer, namespace: flux-system}\ndata: {value: '${value}', mode: dirty}\n",
	)
	testutil.WriteFile(t, root, "bridge.yaml", ksYAML("bridge", "bridge", "")+`  postBuild:
    substituteFrom: [{kind: ConfigMap, name: hello, optional: true}]
`)
	testutil.WriteFile(t, root, "bridge/kustomization.yaml", "resources: [settings.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"bridge/settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: bridge-settings, namespace: flux-system}\ndata: {value: '${value}'}\n",
	)
	testutil.WriteFile(t, root, "pin.yaml", ksYAML("pin", "pin", ""))
	testutil.WriteFile(t, root, "pin/kustomization.yaml", "resources: [hello.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"pin/hello.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: flux-system}\ndata: {value: provisional}\n",
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: root, RepoRoot: root, ExternalChanges: change.NewSet([]string{"shared/consumer.yaml"})},
		Config{CacheDir: t.TempDir(), Concurrency: 2},
	)
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	t.Cleanup(func() {
		o.stop()
	})
	{
		{
			err := o.bootstrap(t.Context())
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	reconcileEntryForTest(t, o)
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.StartLifecycle()
	o.rsc.StartLifecycle()
	consumer, pin := testutil.KustomizationID("flux-system", "consumer"), testutil.KustomizationID("flux-system", "pin")
	var blocked, settled atomic.Bool
	dispatch := fixtureDispatcher{
		store: o.store,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			if id != pin {
				result := (dagDispatcher{o}).Dispatch(ctx, id, level)
				if id == consumer && result.Assessment.Outcome() == schedule.Blocked {
					blocked.Store(true)
				}
				return result
			}
			run := controllertest.NewExecution(o.ksc.Controller, id)
			run.Dispatch(
				ctx,
				level,
				run,
				func(run *base.Execution, _ context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
					if level == manifest.DrainNone {
						return &depwait.ErrBlocked{Deps: []manifest.NamedResource{consumer}}
					}
					{
						assert.ErrorIf(
							t,
							!blocked.Load(),
							"%v",
							"producer settled before the transitive consumer parked",
						)
					}
					settled.Store(true)
					obj := &manifest.ConfigMap{
						Name:      "hello",
						Namespace: "flux-system",
						Data:      map[string]any{"value": "pinned"},
					}
					run.Emit(
						t.Context(),
						id,
						&store.Emission{
							Children: []store.Child{{Obj: obj}},
							Manifests: testutil.DocumentOccurrences(
								[]map[string]any{
									{
										"apiVersion": "v1",
										"kind":       "ConfigMap",
										"metadata":   map[string]any{"name": "hello", "namespace": "flux-system"},
										"data":       obj.Data,
									},
								},
							),
							Complete: true,
						},
						nil,
					)
					return nil
				},
			)
			return schedule.Result{Assessment: run}
		},
	}
	scheduler := schedule.New(o.tasks, dispatch)
	unsubscribe := o.watchDAG(scheduler)
	defer unsubscribe()
	scheduler.Seed(o.seedNodes())
	{
		{
			err := scheduler.Run(t.Context())
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	output, ok := o.store.GetArtifact(consumer, store.ArtifactContent).(*store.Emission)
	if !blocked.Load() || !settled.Load() || !ok || len(output.Manifests) != 1 ||
		output.Manifests[0].Document["data"].(map[string]any)["value"] != "pinned" {
		t.Fatalf(
			"after-drain transitive output: blocked=%t settled=%t output=%+v",
			blocked.Load(),
			settled.Load(),
			output,
		)
	}
}

func TestDAGDispatch_CanceledCommittedNotificationHarvest(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		s := store.New()
		tasks := task.NewBounded(2)
		publisher, child := testutil.KustomizationID("apps", "publisher"), testutil.KustomizationID("apps", "child")
		s.AddObject(&manifest.Kustomization{Name: publisher.Name, Namespace: publisher.Namespace})
		s.RecordEmitted(
			publisher,
			captureEmission(
				s,
				publisher,
				&store.Emission{
					Children: []store.Child{
						{Obj: &manifest.Kustomization{Name: child.Name, Namespace: child.Namespace}},
					},
					Complete: true,
				},
			),
			nil,
		)
		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "withdrawn", "namespace": "apps"},
		}
		_, _, accepted := s.GetEmission(child)
		children, retained := testutil.ParseChildren(
			base.New(s, tasks),
			s.GetObject(child),
			[]map[string]any{doc},
			manifest.ParseDocOptions{},
		)
		accepted.Children, accepted.Manifests = children, testutil.DocumentOccurrences(retained)
		accepted.Complete = true
		s.RecordEmitted(child, &accepted, nil)
		accepted.Finish()
		o := &Orchestrator{store: s}
		{
			assert.FatalIf(
				t,
				len(outputByParent(o.collectManifests())[child]) != 1,
				"%v",
				"fixture has no accepted child output",
			)
		}
		entered, release, harvested := make(chan struct{}), make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		defer resume()
		unsubBarrier := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
			if id == child && payload == nil {
				close(entered)
				tasks.YieldSlot(func() { <-release })
			}
		}, false)
		defer unsubBarrier()
		dispatch := fixtureDispatcher{
			store: s,
			dispatch: func(context.Context, schedule.NodeID, manifest.DrainLevel) schedule.Result {
				s.RecordEmitted(publisher, captureEmission(s, publisher, &store.Emission{Complete: true}), nil)
				return schedule.Result{}
			},
		}
		scheduler := schedule.New(tasks, dispatch)
		unsubscribe := o.watchDAG(scheduler)
		defer unsubscribe()
		scheduler.Seed([]schedule.NodeID{publisher})
		var output map[manifest.NamedResource][]map[string]any
		done := make(chan error, 1)
		go func() {
			err := scheduler.Run(ctx)
			close(harvested)
			output = outputByParent(o.collectManifests())
			done <- err
		}()
		<-entered
		{
			assert.FatalIf(
				t,
				s.GetObject(child) != nil || s.GetObject(accepted.Children[0].Obj.Named()) != nil,
				"%v",
				"paused notification precedes the withdrawal commit",
			)
		}
		cancel()
		synctest.Wait()
		select {
		case <-harvested:
			t.Fatal("cancellation harvested before committed notification delivery")
		default:
		}
		resume()
		{
			{
				err := <-done
				assert.FatalIf(t, !errors.Is(err, context.Canceled), "got %v, want cancellation", err)
			}
		}
		if len(output) != 0 || tasks.Failures() != 0 {
			t.Fatalf("withdrawn output survived final harvest: %v; task failures=%d", output, tasks.Failures())
		}
	})
}
