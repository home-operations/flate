package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"testing"
	"testing/synctest"
	"time"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAcquire_WorkWinner(t *testing.T) {
	t.Parallel()
	for winner := range 2 {
		t.Run(fmt.Sprintf("winner=%d", winner), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				testAcquireWorkWinner(t, winner)
			})
		})
	}
}

func testAcquireWorkWinner(t *testing.T, winner int) {
	t.Helper()
	c := cachetest.New(t, "", New)
	var work [2]manifest.WorkRecorder
	for side := range work {
		work[side].Key = manifest.WorkKey{
			Tree:   manifest.TreeID{Cluster: "cluster", Side: manifest.TreeSide(side)},
			Owner:  manifest.NamedResource{Kind: "GitRepository", Name: "owner"},
			Source: manifest.NamedResource{Kind: "GitRepository", Name: "source"},
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 2)
	acquire := func(side int) {
		ctx := t.Context()
		accounting := manifest.AcquisitionWork{Scope: work[side].Scope()}
		_, _, _, err := c.Blob(ctx,
			BlobKey{Content: "shared"}, &Policy{MaxAge: time.Hour, Work: &accounting},
			func(_ context.Context, input ProducerInput) (ProducerResult, error) {
				assert.Equal(t, input.Work() == &accounting, true)
				if side != winner {
					t.Error("waiter performed the shared production")
					return ProducerResult{}, nil
				}
				close(entered)
				<-release
				_, err := io.WriteString(input, "accepted")
				return ProducerResult{}, err
			},
		)
		done <- err
	}
	go acquire(winner)
	<-entered
	go acquire(1 - winner)
	synctest.Wait()
	close(release)
	assert.NoError(t, <-done)
	assert.NoError(t, <-done)
	var joint manifest.WorkSummary
	for side := range work {
		summary := work[side].Scope().Snapshot(0).Normalize()
		for recordIndex := range summary {
			record := &summary[recordIndex]
			assert.Equal(t, record.Key.Tree.Side, manifest.TreeSide(side))
			if record.Key.Input == "" {
				t.Fatal("acquisition lost its owner-derived identity")
			}
		}
		want := manifest.WorkCounts{}
		if side == winner {
			want.Attempts, want.Completed = 1, 1
		} else {
			assert.Equal(t, phaseWork(summary, manifest.WorkCache).CacheService, uint64(1))
		}
		assert.Equal(t, phaseWork(summary, manifest.WorkAcquisition), want)
		joint = append(joint, summary...)
	}
	assert.Equal(t, phaseWork(joint, manifest.WorkAcquisition), manifest.WorkCounts{Attempts: 1, Completed: 1})
}

func phaseWork(summary manifest.WorkSummary, phase manifest.WorkPhase) manifest.WorkCounts {
	var counts manifest.WorkCounts
	for recordIndex := range summary {
		record := &summary[recordIndex]
		if record.Key.Phase == phase {
			counts.Add(&record.Counts)
		}
	}
	return counts
}

func TestBlob_ObserveBeforeProduction(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                        string
		warm, handled, fail, reject bool
		productions                 int
	}{
		{name: "cold", productions: 1},
		{name: "pending", warm: true},
		{name: "handled cold", handled: true},
		{name: "handled pending", warm: true, handled: true},
		{name: "admission failure", fail: true},
		{name: "rejected pending", warm: true, reject: true, productions: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, "", New)
			key := BlobKey{Content: "render", Render: RenderPending}
			produce := func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				_, err := io.WriteString(w, "content")
				return ProducerResult{}, err
			}
			if tc.warm {
				_, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, produce)
				assert.NoError(t, err)
			}
			var recorder manifest.WorkRecorder
			accounting := manifest.AcquisitionWork{Scope: recorder.Scope()}
			calls, productions := 0, 0
			failure := errors.New("admission stopped")
			key.Validate = func(context.Context, fs.FS) (bool, error) { return !tc.reject || productions > 0, nil }
			key.Observe = func(_ context.Context, generation fs.FS) (bool, error) {
				calls++
				assert.Equal(t, generation != nil, tc.warm && !tc.reject)
				assert.Equal(t, productions, 0)
				if tc.fail {
					return false, failure
				}
				return tc.handled, nil
			}
			generation, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever, Work: &accounting},
				func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					productions++
					return produce(ctx, w)
				})
			assert.Equal(t, calls, 1)
			assert.Equal(t, errors.Is(err, failure), tc.fail)
			assert.Equal(t, generation == nil, tc.handled || tc.fail)
			assert.Equal(t, productions, tc.productions)
			counts := phaseWork(recorder.Scope().Snapshot(0).Normalize(), manifest.WorkCache)
			assert.Equal(t, counts.Attempts, uint64(1))
			if tc.fail {
				assert.Equal(t, counts.Failures, uint64(1))
			} else {
				assert.Equal(t, counts.Completed, uint64(1))
				assert.Equal(t, counts.Failures, uint64(0))
			}
			key.Observe = nil
			_, _, _, err = c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
			assert.Equal(t, errors.Is(err, errdef.ErrNotFound), !tc.warm && (tc.handled || tc.fail))
		})
	}
}

func TestBlob_ObservationAllowsConcurrentPublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		observing, published := make(chan struct{}), make(chan struct{})
		key := BlobKey{Content: "shared-observation", Render: RenderPending}
		go func() {
			defer close(published)
			<-observing
			_, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever},
				func(_ context.Context, w ProducerInput) (ProducerResult, error) {
					_, err := io.WriteString(w, "shared")
					return ProducerResult{}, err
				})
			assert.NoError(t, err)
		}()
		observed := key
		calls := 0
		observed.Observe = func(_ context.Context, generation fs.FS) (bool, error) {
			calls++
			assert.Equal(t, generation, nil)
			close(observing)
			<-published
			return false, nil
		}
		generation, _, _, err := c.Blob(t.Context(), observed, &Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				t.Error("concurrent publication was not reused")
				return ProducerResult{}, nil
			})
		assert.NoError(t, err)
		data, err := fs.ReadFile(generation, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "shared")
		assert.Equal(t, calls, 1)
	})
}
