package controllertest

import (
	"context"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
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
				ReconcileNode: func(context.Context, manifest.NamedResource, manifest.DrainLevel) []manifest.NamedResource {
					if tc.panics {
						panic("reconcile failure")
					}
					return tc.dependencies
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
	st.UpdateStatus(id, store.StatusReady, "", "ready before reconciliation")
	recorder := &recordingTB{TB: t}
	d := Dispatch{
		Tasks: tasks,
		Store: st,
		ID:    id,
		ReconcileNode: func(context.Context, manifest.NamedResource, manifest.DrainLevel) []manifest.NamedResource {
			panic("reconcile failure")
		},
	}
	before := tasks.Failures()
	info := DispatchToFixpoint(t.Context(), recorder, d)
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, tasks.Failures() > before, true)
	assert.Equal(t, len(recorder.errors) > 0, true)
}
