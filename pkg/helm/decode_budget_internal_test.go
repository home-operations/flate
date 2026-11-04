package helm

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

func TestDecodeChart_PreparationBudget(t *testing.T) {
	t.Parallel()
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 1.0.0\n")
	testutil.WriteFile(t, chartDir, "mychart/requirements.yaml", "dependencies: []\n")
	testutil.WriteFile(
		t,
		chartDir,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n",
	)
	owner := cachetest.New(t, t.TempDir(), cache.New)
	assert.FatalIf(t, !owner.CanPersistRenders(), "%v", "history budget proof requires clean stamped compilation")
	client := NewClient(owner)
	hr := newHR()
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", chartDir)}
	_, receipt, err := client.Template(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	assert.NoError(

		t,

		client.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: receipt,
				Records: emptyRenderInventory,
			},
		),
	)
	synctest.Test(t, func(t *testing.T) {
		client := NewClient(owner)
		tasks := task.NewBounded(2)
		entered, release := make(chan struct{}, 2), make(chan struct{})
		for range 2 {
			tasks.Go(t.Context(), "occupied", func(context.Context) {
				entered <- struct{}{}
				<-release
			})
		}
		for range 2 {
			<-entered
		}
		ctx := t.Context()
		var recorder manifest.WorkRecorder
		accounting := manifest.RenderWork{Scope: recorder.Scope()}
		input.Work = &accounting
		prepared, err := client.prepareTemplate(ctx, hr, nil, &input)
		assert.NoError(t, err)
		work := accounting.Decode(prepared.loaded.key)
		joined := make(chan error, 1)
		var found *renderRecord
		go func() {
			var err error
			found, err = client.lookupRender(
				ctx,
				&prepared.loaded,
				prepared.key, &RenderOptions{PreparationTasks: tasks, Work: &accounting},
			)
			joined <- err
		}()
		synctest.Wait()
		before := atomic.LoadUint64(&work.Counts.Attempts)
		close(release)
		assert.NoError(t, <-joined)
		tasks.BlockTillDone()
		assert.Equal(t, before, uint64(0))
		assert.Equal(t, work.Counts, manifest.WorkCounts{Attempts: 1, Completed: 1, PeakConcurrent: 1})
		assert.FatalIf(
			t,
			found == nil || !found.Analysis.RequiresLoader,
			"%v",
			"accepted history did not decode the required chart",
		)
		assert.Equal(t, prepared.loaded.chart.Metadata.Name, "mychart")
		entered, release = make(chan struct{}, 2), make(chan struct{})
		for range 2 {
			tasks.Go(t.Context(), "occupied", func(context.Context) {
				entered <- struct{}{}
				<-release
			})
		}
		for range 2 {
			<-entered
		}
		go func() {
			_, err := client.lookupRender(
				ctx,
				&prepared.loaded,
				prepared.key, &RenderOptions{PreparationTasks: tasks, Work: &accounting},
			)
			joined <- err
		}()
		synctest.Wait()
		served := len(joined)
		close(release)
		assert.NoError(t, <-joined)
		tasks.BlockTillDone()
		assert.Equal(t, served, 1)
		assert.Equal(t, work.Counts, manifest.WorkCounts{Attempts: 1, Completed: 1, PeakConcurrent: 1})
	})
}
