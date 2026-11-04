package manifest

import (
	"runtime"
	"sync"
	"testing"
	"weak"

	"github.com/home-operations/flate/internal/assert"
)

func TestWorkRecorder_ConcurrentCounts(t *testing.T) {
	t.Parallel()
	var root WorkRecorder
	record := root.Scope().Record(WorkKustomize, "engine")
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			peak := root.Scope().StartConcurrent()
			record.Add(&WorkCounts{Attempts: 1, PeakConcurrent: peak})
			entered <- struct{}{}
			<-release
			record.Finish(nil)
			root.Scope().FinishConcurrent()
		})
	}
	for range 2 {
		<-entered
	}
	close(release)
	wg.Wait()
	assert.Equal(t, root.Scope().Snapshot(0)[0].Counts, WorkCounts{Attempts: 2, Completed: 2, PeakConcurrent: 2})
}

func TestWorkScope_DetachedLifetime(t *testing.T) {
	t.Parallel()
	owner := new(struct {
		WorkRecorder

		payload [1024]byte
	})
	scope := owner.Scope()
	scope.Record(WorkEvaluation, "input").Add(&WorkCounts{Attempts: 1})
	scope.Snapshot(0)
	reference := weak.Make(owner)
	runtime.GC()
	if reference.Value() != nil {
		t.Fatal("detached work scope retained its execution")
	}
	runtime.KeepAlive(scope)
}
