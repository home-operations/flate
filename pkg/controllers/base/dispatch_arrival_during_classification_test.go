package base_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDispatch_ArrivalDuringClassification(t *testing.T) {
	t.Parallel()
	for _, suspended := range []bool{false, true} {
		t.Run(strconv.FormatBool(suspended), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			parent := &manifest.Kustomization{Name: "publisher", Namespace: "apps"}
			child := &manifest.HelmRelease{
				ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
				Name:      "consumer",
				Namespace: "apps",
			}
			st.AddObject(child)
			st.SetDiscoveryIndex(
				&manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						child.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
					},
				},
			)
			entered, published := make(chan struct{}), make(chan struct{})
			c := base.New(st, tasks)
			c.Configure(base.Options{Existence: declarationPromotion(func(id manifest.NamedResource) bool {
				if id != parent.Named() {
					t.Errorf("unrelated promotion: %s", id)
					return false
				}
				close(entered)
				tasks.YieldSlot(func() { <-published })
				return true
			})})
			var run *base.Execution
			var bodies atomic.Int64
			tasks.Go(t.Context(), "dispatch", func(ctx context.Context) {
				run = controllertest.NewExecution(c, child.Named())
				run.Dispatch(
					ctx,
					manifest.DrainNone,
					struct{}{},
					func(_ struct{}, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
						bodies.Add(1)
						return nil
					},
				)
			})
			tasks.Go(t.Context(), "publish", func(context.Context) {
				tasks.YieldSlot(func() { <-entered })
				st.AddObject(parent)
				accepted := child.Clone()
				accepted.Suspend = suspended
				st.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: accepted}}}, nil)
				st.UpdateStatus(
					parent.Named(),
					store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"},
				)
				close(published)
			})
			tasks.BlockTillDone()
			if tasks.Failures() != 0 || run.Valid() || bodies.Load() != 0 {
				t.Fatalf("stale dispatch continued: bodies=%d valid=%t", bodies.Load(), run.Valid())
			}
			run.Close()
			fresh := controllertest.NewExecution(c, child.Named())
			fresh.Dispatch(
				t.Context(),
				manifest.DrainNone,
				struct{}{},
				func(_ struct{}, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
					bodies.Add(1)
					return nil
				},
			)
			info, _ := st.GetStatus(child.Named())
			if suspended && (bodies.Load() != 0 || info.Outcome() != store.OutcomeSuspended) ||
				!suspended && bodies.Load() != 1 {
				t.Fatalf("fresh declaration gate: bodies=%d status=%+v", bodies.Load(), info)
			}
		})
	}
}

type declarationPromotion func(manifest.NamedResource) bool

func (declarationPromotion) IsFileIndexed(manifest.NamedResource) bool { return false }

func (p declarationPromotion) Promote(_ context.Context, _ *store.ReadSet, id manifest.NamedResource) (bool, error) {
	return p(id), nil
}

func TestRequire_MaterializerPurpose(t *testing.T) {
	t.Parallel()
	for _, materializerFirst := range []bool{false, true} {
		t.Run(strconv.FormatBool(materializerFirst), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			owner := &manifest.ResourceSet{Name: "owner", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{consumer, owner})
			st.UpdateStatus(
				owner.Named(),
				store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing"},
			)
			run := controllertest.NewExecution(base.New(st, nil), consumer.Named())
			deps := []manifest.DependencyRef{
				{NamedResource: owner.Named()},
				{NamedResource: owner.Named(), ReadyExpr: new("false")},
			}
			if materializerFirst {
				run.Reads.RequireMaterializer(owner.Named())
			}
			for _, dep := range deps {
				run.Reads.Require(dep)
			}
			run.Reads.RequireMaterializer(owner.Named())
			run.Dispatch(
				t.Context(),
				manifest.DrainNone,
				struct{}{},
				func(_ struct{}, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
					return run.Require(ctx, deps, level)
				},
			)
			{
				assert.FatalIf(t, run.Outcome() != schedule.Blocked ||
					!slices.Equal(run.Blocked, []manifest.NamedResource{owner.Named()}) ||
					len(
						slices.Collect(run.Reads.Gates()),
					) != 3, "%v", "authored existence gate erased materializer readiness")
			}
			run.Settle(t.Context(), manifest.DrainForce)
			info, _ := st.GetStatus(consumer.Named())
			{
				assert.FatalIf(
					t,
					!info.IsDependencyFailure() ||
						!slices.Equal(info.BlockedBy(), []manifest.NamedResource{owner.Named()}),
					"settlement lost gate purpose: %+v",
					info,
				)
			}
		})
	}
}

func TestExecution_SyntheticDependencyReasons(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"gate", "declaration", "data"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testExecutionSyntheticDependencyReasonsCase(
				t,
				&mode,
			)
		},
		)
	}
}

func testExecutionSyntheticDependencyReasonsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := store.New()
	owner := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		"clusters/test",
		testutil.KustomizationID("flux-system", "sync"),
	)
	child := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	st.AddObject(owner)
	if (*testCaseInput) != "declaration" {
		st.AddObject(child)
	}
	status := store.StatusFailed
	if (*testCaseInput) == "declaration" {
		status = store.StatusReady
	}
	if (*testCaseInput) == "data" {
		status = store.StatusPending
	}
	st.UpdateStatus(owner.Named(), store.StatusInfo{Status: status, Message: "root failure"})
	if (*testCaseInput) == "declaration" {
		st.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					child.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
				},
			},
		)
	}
	run := controllertest.NewExecution(base.New(st, task.NewBounded(2)), child.Named())
	defer run.Close()
	var err error
	switch *testCaseInput {
	case "gate":
		err = run.Require(
			t.Context(),
			[]manifest.DependencyRef{{NamedResource: owner.Named()}},
			manifest.DrainForce,
		)
	case "data":
		err = run.AwaitData(
			t.Context(),
			base.DataWait{
				Inputs: []depwait.DataDependency{
					{
						Kind:      manifest.KindConfigMap,
						Namespace: "apps",
						Name:      "missing",
						Input:     store.ReferenceFacts{Materializers: []manifest.NamedResource{owner.Named()}},
					},
				},
				Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error {
					t.Error("failed input resumed")
					return nil
				},
			},
			manifest.DrainForce,
		)
	case "declaration":
		run.Dispatch(
			t.Context(),
			manifest.DrainForce,
			struct{}{},
			func(struct{}, context.Context, *manifest.Kustomization, manifest.DrainLevel) error {
				t.Error("missing declaration reconciled")
				return nil
			},
		)
		info, _ := st.GetStatus(child.Named())
		{
			assert.FatalIf(t, !info.IsDependencyFailure() ||
				!slices.Equal(
					info.BlockedBy(),
					[]manifest.NamedResource{owner.Named()},
				), "declaration failure=%+v", info)
		}
		if !strings.Contains(info.String(), `entry path "clusters/test"`) ||
			strings.Contains(info.String(), owner.Named().String()) {
			t.Fatalf("declaration context=%s", info.String())
		}
		return
	}
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	{
		assert.FatalIf(t, !ok || !slices.Contains(failed.Failed, owner.Named()) ||
			failed.Reasons[owner.Named()].Subject.Resource != testutil.KustomizationID("flux-system", "sync") ||
			strings.Contains(
				err.Error(),
				owner.Named().String(),
			), "%s lost raw/display context: %#v %v", (*testCaseInput), failed, err)
	}
}
