package controllertest

import (
	"context"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type recordingTB struct {
	testing.TB
	errors []string
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.Helper()
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func TestReconcile_TaskFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		dependencies []manifest.NamedResource
		panics       bool
		historical   bool
		wantTerminal bool
		wantFailures int64
	}{
		{name: "terminal", wantTerminal: true},
		{name: "blocked", dependencies: []manifest.NamedResource{testutil.HelmReleaseID("media", "dependency")}},
		{name: "panic", panics: true, wantFailures: 1},
		{name: "historical", historical: true, wantTerminal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tasks := task.NewBounded(2)
			if tc.historical {
				tasks.Go(t.Context(), "historical", func(context.Context) { panic("historical failure") })
				tasks.BlockTillDone()
				assert.Equal(t, tasks.Failures(), int64(1))
			}
			recorder := &recordingTB{TB: t}
			d := Dispatch{
				Tasks: tasks,
				ID:    testutil.HelmReleaseID("media", "sonarr"),
				ReconcileNode: func(context.Context, manifest.NamedResource, manifest.DrainLevel) *base.Execution {
					if tc.panics {
						panic("reconcile failure")
					}
					execution := NewExecution(base.New(store.New(), tasks), testutil.HelmReleaseID("media", "sonarr"))
					object := &manifest.HelmRelease{Name: "sonarr", Namespace: "media"}
					execution.Store.AddObject(object)
					execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(_ *base.Execution, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
						if len(tc.dependencies) != 0 {
							return &depwait.ErrBlocked{Deps: tc.dependencies}
						}
						return nil
					})
					return execution
				},
			}
			before := tasks.Failures()
			terminal := Reconcile(t.Context(), recorder, d, manifest.DrainNone)
			assert.Equal(t, terminal, tc.wantTerminal)
			assert.Equal(t, tasks.Failures()-before, tc.wantFailures)
			assert.Equal(t, len(recorder.errors) > 0, tc.panics)
		})
	}
}

func TestDispatchToFixpoint_PanicWithReadyStatus(t *testing.T) {
	t.Parallel()
	tasks := task.NewBounded(2)
	st := store.New()
	id := testutil.HelmReleaseID("media", "sonarr")
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "ready before reconciliation"})
	recorder := &recordingTB{TB: t}
	d := Dispatch{
		Tasks: tasks,
		Store: st,
		ID:    id,
		ReconcileNode: func(context.Context, manifest.NamedResource, manifest.DrainLevel) *base.Execution {
			panic("reconcile failure")
		},
	}
	before := tasks.Failures()
	info := DispatchToFixpoint(t.Context(), recorder, d)
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, tasks.Failures() > before, true)
	assert.Equal(t, len(recorder.errors) > 0, true)
}

func TestDispatchToFixpoint_SettlesRetainedAssessment(t *testing.T) {
	t.Parallel()
	tasks := task.NewBounded(2)
	st := store.New()
	obj := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	st.AddObject(obj)
	dependency := manifest.DependencyRef{NamedResource: testutil.HelmReleaseID("apps", "absent")}
	dispatches := 0
	info := DispatchToFixpoint(t.Context(), t, Dispatch{
		Tasks: tasks, Store: st, ID: obj.Named(),
		ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
			dispatches++
			execution := NewExecution(base.New(st, tasks), id)
			execution.Dispatch(ctx, level, execution, func(execution *base.Execution, ctx context.Context, _ *manifest.HelmRelease, level manifest.DrainLevel) error {
				return execution.Require(ctx, []manifest.DependencyRef{dependency}, level)
			})
			return execution
		},
	})
	assert.Equal(t, info.Status, store.StatusFailed)
	assert.Equal(t, dispatches, 1)
	assert.Diff(t, info.BlockedBy(), []manifest.NamedResource{dependency.NamedResource})
}
