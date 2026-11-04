package orchestrator

import (
	"context"
	"fmt"
	"sync"
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

//nolint:paralleltest // Subtests share a synctest bubble and cannot run in parallel.
func TestDAGDispatch_WithdrawalNotificationDrain(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, lateFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/lateFirst=%t", workers, lateFirst), func(t *testing.T) {
				testDAGDispatchWithdrawalNotificationDrainCase(
					t,
					testDAGDispatchWithdrawalNotificationDrainCaseInputs{workers: &workers, lateFirst: &lateFirst},
				)
			},
			)
		}
	}
}

type testDAGDispatchWithdrawalNotificationDrainCaseInputs struct {
	workers   *int
	lateFirst *bool
}

func testDAGDispatchWithdrawalNotificationDrainCase(
	t *testing.T,
	testCaseInput testDAGDispatchWithdrawalNotificationDrainCaseInputs,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		s := store.New()
		tasks := task.NewBounded((*testCaseInput.workers))
		p, k, consumer := testutil.KustomizationID(
			"flux-system",
			"publisher",
		), testutil.KustomizationID(
			"flux-system",
			"child",
		), testutil.KustomizationID(
			"flux-system",
			"consumer",
		)
		s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
		child := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}
		s.RecordEmitted(p, &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true}, nil)
		_, _, late := s.GetEmission(k)
		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "late", "namespace": "apps"},
			"data":       map[string]any{"value": "private"},
		}
		children, retained := parseChildrenForTest(k, []map[string]any{doc}, manifest.ParseDocOptions{})
		late.Children, late.Manifests = children, testutil.DocumentOccurrences(retained)
		late.Complete = true
		publishRelease, entered, deliveryRelease := make(
			chan struct{},
		), make(
			chan struct{},
		), make(
			chan struct{},
		)
		consumerStarted, woken := make(chan struct{}), make(chan struct{})
		var calls atomic.Int64
		publishLate := func() {
			s.RecordEmitted(k, &late, nil)
		}
		s.AddObject(&manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace})
		releasePublication := sync.OnceFunc(func() { close(publishRelease) })
		releaseDelivery := sync.OnceFunc(func() { close(deliveryRelease) })
		defer releasePublication()
		defer releaseDelivery()
		dispatch := fixtureDispatcher{
			store: s,
			dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				if id == consumer {
					execution := controllertest.NewExecution(base.New(s, tasks), id)
					execution.Dispatch(
						ctx,
						level,
						execution,
						func(execution *base.Execution, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
							if calls.Add(1) == 1 {
								close(consumerStarted)
								return execution.Require(
									ctx,
									[]manifest.DependencyRef{{NamedResource: k}},
									level,
								)
							}
							{
								assert.ErrorIf(
									t,
									execution.Reads.GetObject(k) != nil,
									"%v",
									"withdrawal wake observed child still live",
								)
							}
							close(woken)
							return nil
						},
					)
					return schedule.Result{Assessment: execution}
				}
				tasks.YieldSlot(func() { <-publishRelease })
				if *testCaseInput.lateFirst {
					publishLate()
				}
				s.RecordEmitted(p, &store.Emission{Complete: true}, nil)
				if !(*testCaseInput.lateFirst) {
					publishLate()
				}
				return schedule.Result{}
			},
		}
		scheduler := schedule.New(tasks, dispatch)
		o := &Orchestrator{store: s}
		unsubscribe := o.watchDAG(scheduler)
		defer unsubscribe()
		unsubBarrier := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
			if id == k && payload == nil {
				close(entered)
				tasks.YieldSlot(func() { <-deliveryRelease })
			}
		}, false)
		defer unsubBarrier()
		scheduler.Seed([]schedule.NodeID{p, consumer})
		done := make(chan error, 1)
		go func() { done <- scheduler.Run(t.Context()) }()
		<-consumerStarted
		synctest.Wait()
		releasePublication()
		<-entered
		synctest.Wait()
		assertDAGDrainPending(t, done, &calls)
		releaseDelivery()
		{
			{
				err := <-done
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
		assertDAGWithdrawalWake(t, woken)
		{
			assert.FatalIf(t, calls.Load() != 2 || tasks.Failures() != 0 || s.GetObject(k) != nil ||
				s.GetObject(
					late.Children[0].Obj.Named(),
				) != nil, "%v", "withdrawal lost wake or late publication manufactured support")
		}
	})
}

func assertDAGDrainPending(t *testing.T, done <-chan error, calls *atomic.Int64) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("drain sealed while notification was outstanding: %v", err)
	default:
	}
	{
		assert.FatalIf(t, calls.Load() > 2, "%v", "outstanding publication queued duplicate successors")
	}
}

func assertDAGWithdrawalWake(t *testing.T, woken <-chan struct{}) {
	t.Helper()
	select {
	case <-woken:
	default:
		t.Fatal("withdrawal did not wake the captured consumer")
	}
}
