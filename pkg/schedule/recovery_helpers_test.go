package schedule_test

import (
	"context"
	"errors"
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
)

type runRegisteredDependencyGraphInputs struct {
	testCaseInput **int
	tc            *struct {
		name string

		graph map[string][]string
		want  store.Status
	}
}

func runRegisteredDependencyGraph(
	t *testing.T,
	testCaseInput runRegisteredDependencyGraphInputs,
) {
	t.Helper()

	objects := make([]manifest.BaseManifest, 0, len(testCaseInput.tc.graph))
	counts := make(map[string]*atomic.Int64, len(testCaseInput.tc.graph))
	for name := range testCaseInput.tc.graph {
		objects = append(objects, &manifest.Kustomization{Name: name, Namespace: "ns"})
		counts[name] = &atomic.Int64{}
	}
	s, st := registeredScheduler(
		t, (*(*testCaseInput.testCaseInput)), objects,
		func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
			deps := make([]manifest.DependencyRef, 0, len(testCaseInput.tc.graph[id.Name]))
			for _, dep := range testCaseInput.tc.graph[id.Name] {
				deps = append(
					deps,
					manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", dep)},
				)
			}
			if err := e.Require(ctx, deps, level); err != nil {
				return err
			}
			counts[id.Name].Add(1)
			return nil
		},
	)
	if testCaseInput.tc.name == "cycle" {
		for _, obj := range objects {
			st.SetPreflight(obj.Named(), "dependency cycle detected")
		}
	}
	assertRegisteredGraphResults(
		t,
		&registeredGraphResultState{testCaseInput: &testCaseInput, counts: &counts, s: &s, st: &st},
	)
}

type runRegisteredFailureRecoveryInputs struct {
	testCaseInput **int
	data          *bool
}

func runRegisteredFailureRecovery(
	t *testing.T,
	testCaseInput runRegisteredFailureRecoveryInputs,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		runRegisteredFailureRecoveryCase(t, runRegisteredFailureRecoveryCaseInputs{testCaseInput: &testCaseInput})
	},
	)
}

type runRegisteredFixpointFeedbackInputs struct {
	testCaseInput **bool
}

func runRegisteredFixpointFeedback(
	t *testing.T,
	testCaseInput runRegisteredFixpointFeedbackInputs,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	x, y := &manifest.Kustomization{
		Name:      "x",
		Namespace: "ns",
	}, &manifest.Kustomization{
		Name:      "y",
		Namespace: "ns",
	}
	started, held := make(chan struct{}, 2), make(chan struct{}, 2)
	release := make(chan struct{})
	var active, total atomic.Int64
	counts := map[schedule.NodeID]*atomic.Int64{x.Named(): {}, y.Named(): {}}
	s, st := registeredScheduler(
		t,
		4,
		[]manifest.BaseManifest{x, y},
		func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			active.Add(1)
			defer active.Add(-1)
			run, count := counts[id].Add(1), total.Add(1)
			other := x.Named()
			if id == other {
				other = y.Named()
			}
			e.Reads.GetObject(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: other.Name},
			)
			if run == 1 {
				started <- struct{}{}
				e.Tasks.YieldSlot(func() { <-release })
			}
			if (*(*testCaseInput.testCaseInput)) && count >= 80 {
				held <- struct{}{}
				<-ctx.Done()
			}
			_, _, next := e.Reads.GetEmission(id)
			defer next.Finish()
			child := &manifest.ConfigMap{Name: id.Name, Namespace: "ns", Data: map[string]any{"value": run}}
			next.Children = []store.Child{{Obj: child, Doc: child.Snapshot()}}
			next.Manifests, next.Complete = testutil.DocumentOccurrences(
				[]map[string]any{child.Snapshot()},
			), true
			e.Emit(t.Context(), id, &next, nil)
			if *(*testCaseInput.testCaseInput) {
				return e.Require(ctx, []manifest.DependencyRef{{NamedResource: other}}, level)
			}
			return nil
		},
	)
	assertRegisteredFixpointResults(
		ctx,
		t,
		&registeredFixpointResultState{
			testCaseInput: &testCaseInput,
			cancel:        &cancel,
			x:             &x,
			y:             &y,
			started:       &started,
			held:          &held,
			release:       &release,
			active:        &active,
			total:         &total,
			counts:        &counts,
			s:             &s,
			st:            &st,
		},
	)
}

type registeredGraphResultState struct {
	testCaseInput *runRegisteredDependencyGraphInputs
	counts        *map[string]*atomic.Int64
	s             **schedule.Scheduler
	st            **store.Store
}

func assertRegisteredGraphResults(t *testing.T, scope *registeredGraphResultState) {
	t.Helper()
	assert.NoError(t, (*scope.s).Run(t.Context()))
	for name := range scope.testCaseInput.tc.graph {
		status, ok := (*scope.st).GetStatus(testutil.KustomizationID("ns", name))
		assert.Equal(t, ok, true)
		assert.Equal(t, status.Status, scope.testCaseInput.tc.want)
		var wantBlockers []manifest.NamedResource
		if scope.testCaseInput.tc.want == store.StatusFailed && scope.testCaseInput.tc.name != "cycle" {
			for _, name := range scope.testCaseInput.tc.graph[name] {
				wantBlockers = append(wantBlockers, testutil.KustomizationID("ns", name))
			}
			slices.SortFunc(wantBlockers, func(a, b manifest.NamedResource) int { return a.Compare(b) })
		}
		assert.Diff(t, status.BlockedBy(), wantBlockers)

		{
			assert.ErrorIf(
				t, scope.testCaseInput.tc.
					want == store.StatusFailed && status.Message == "", "%s: terminal failure has no diagnostic",
				name,
			)
		}
		wantRenders := int64(0)
		if scope.testCaseInput.tc.want == store.StatusReady {
			wantRenders = 1
		}
		assert.Equal(t, (*scope.counts)[name].Load(), wantRenders)
	}
	assert.Equal(t, len((*scope.s).Results()), len(scope.testCaseInput.tc.graph))
	assert.Equal(t, t.Context().Err(), nil)
	failures := (*scope.st).FailedResources()
	if scope.testCaseInput.tc.want == store.StatusFailed {
		assert.Equal(t, len(failures), len(scope.testCaseInput.tc.graph))
		for name := range scope.testCaseInput.tc.graph {
			_, ok := failures[testutil.KustomizationID("ns", name)]
			assert.Equal(t, ok, true)
		}
	} else {
		assert.Equal(t, len(failures), 0)
	}
}

type registeredFixpointResultState struct {
	testCaseInput *runRegisteredFixpointFeedbackInputs
	cancel        *context.CancelFunc
	x             **manifest.Kustomization
	y             **manifest.Kustomization
	started       *chan struct{}
	held          *chan struct{}
	release       *chan struct{}
	active        *atomic.Int64
	total         *atomic.Int64
	counts        *map[schedule.NodeID]*atomic.Int64
	s             **schedule.Scheduler
	st            **store.Store
}

func assertRegisteredFixpointResults(ctx context.Context, t *testing.T, scope *registeredFixpointResultState) {
	t.Helper()
	c := base.New((*scope.st), nil)
	for _, obj := range []*manifest.Kustomization{(*scope.x), (*scope.y)} {
		child := &manifest.ConfigMap{
			Name:      obj.Name,
			Namespace: "ns",
			Data:      map[string]any{"value": int64(0)},
		}
		controllertest.Emit(
			t.Context(),
			c,
			obj.Named(),
			&store.Emission{Children: []store.Child{{Obj: child, Doc: child.Snapshot()}}, Complete: true},
		)
	}
	done := make(chan error, 1)
	go func() { done <- (*scope.s).Run(ctx) }()
	<-(*scope.started)
	<-(*scope.started)
	assert.Equal(t, scope.active.Load(), int64(2))
	close((*scope.release))
	if *(*scope.testCaseInput.testCaseInput) {
		<-(*scope.held)
		synctest.Wait()
		{
			got := scope.active.Load()
			assert.FatalIf(t, got < 1 || got > 2, "active feedback executions = %d, want one or two", got)
		}
		(*scope.cancel)()
	}
	err := <-done
	assert.Equal(t, scope.active.Load(), int64(0))
	if *(*scope.testCaseInput.testCaseInput) {
		assert.Equal(t, errors.Is(err, context.Canceled), true)
		{
			got := scope.total.Load()
			assert.FatalIf(
				t,
				got < 80 || got > 81,
				"feedback executions = %d, want the threshold plus at most one in-flight peer",
				got,
			)
		}
	} else {
		if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil {
			t.Fatalf(
				"registered terminal feedback did not hit its bound: error=%v context=%v runs=%d",
				err,
				ctx.Err(),
				scope.total.Load(),
			)
		}
		for id, runs := range *scope.counts {
			{
				got := runs.Load()
				assert.ErrorIf(
					t,
					got > 33,
					"%s: %d executions exceeds the initial execution plus 32 successors",
					id,
					got,
				)
			}
		}
	}
}

type runRegisteredFailureRecoveryCaseInputs struct {
	testCaseInput *runRegisteredFailureRecoveryInputs
}

func runRegisteredFailureRecoveryCase(t *testing.T, testCaseInput runRegisteredFailureRecoveryCaseInputs) {
	t.Helper()
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}
	driver := &manifest.Kustomization{Name: "driver", Namespace: "ns"}
	dependency := &manifest.Kustomization{Name: "dependency", Namespace: "ns"}
	read, published, failed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var runs atomic.Int64
	var st *store.Store
	s, current := registeredScheduler(
		t, (*(*testCaseInput.testCaseInput.testCaseInput)), []manifest.BaseManifest{consumer, driver, dependency},
		func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
			switch id {
			case dependency.Named():
				if e.Reads.GetObject(id).(*manifest.Kustomization).Path == "recovered" {
					return nil
				}
				return manifest.NewError(manifest.ErrInput, nil, "initial failure")
			case driver.Named():
				e.Tasks.YieldSlot(func() { <-read })
				if *testCaseInput.testCaseInput.data {
					st.AddObject(
						&manifest.ConfigMap{
							Name:      "late",
							Namespace: "ns",
							Data:      map[string]any{"value": "ready"},
						},
					)
				} else {
					replacement := dependency.Clone()
					replacement.Path = "recovered"
					st.AddObject(replacement)
					st.UpdateStatus(
						dependency.Named(),
						store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
					)
				}
				close(published)
				return nil
			}
			if !(*testCaseInput.testCaseInput.data) {
				e.Tasks.YieldSlot(func() { <-failed })
			}
			run := runs.Add(1)
			var err error
			if *testCaseInput.testCaseInput.data {
				if e.Reads.GetObject(
					manifest.NamedResource{
						Kind:      manifest.KindConfigMap,
						Namespace: "ns",
						Name:      "late",
					},
				) == nil {
					err = manifest.NewError(manifest.ErrInput, nil, "data unavailable")
				}
			} else {
				err = e.Require(
					ctx,
					[]manifest.DependencyRef{{NamedResource: dependency.Named()}},
					level,
				)
			}
			if run == 1 {
				close(read)
				e.Tasks.YieldSlot(func() { <-published })
			}
			return err
		},
	)
	settleRegisteredRecovery(
		t,
		&registeredRecoveryResultState{
			testCaseInput: &testCaseInput,
			consumer:      &consumer,
			dependency:    &dependency,
			failed:        &failed,
			runs:          &runs,
			st:            &st,
			s:             &s,
			current:       &current,
		},
	)
}

type registeredRecoveryResultState struct {
	testCaseInput *runRegisteredFailureRecoveryCaseInputs
	consumer      **manifest.Kustomization
	dependency    **manifest.Kustomization
	failed        *chan struct{}
	runs          *atomic.Int64
	st            **store.Store
	s             **schedule.Scheduler
	current       **store.Store
}

func settleRegisteredRecovery(t *testing.T, scope *registeredRecoveryResultState) {
	t.Helper()
	(*scope.st) = (*scope.current)
	u := (*scope.st).AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == (*scope.dependency).Named() && payload.(store.StatusInfo).Status == store.StatusFailed {
			close((*scope.failed))
		}
	}, false)
	defer u()
	assert.NoError(t, (*scope.s).Run(t.Context()))
	status, _ := (*scope.st).GetStatus((*scope.consumer).Named())
	assert.Equal(t, status.Status, store.StatusReady)
	assert.Equal(t, scope.runs.Load(), int64(2))
	assert.Equal(t, t.Context().Err(), nil)
	assert.Equal(t, len(status.BlockedBy()), 0)
	assert.Equal(t, len((*scope.s).Results()), 3)
	failures := (*scope.st).FailedResources()
	if *scope.testCaseInput.testCaseInput.data {
		assert.Equal(t, len(failures), 1)
		_, failedDependency := failures[(*scope.dependency).Named()]
		assert.Equal(t, failedDependency, true)
	} else {
		assert.Equal(t, len(failures), 0)
	}
}
