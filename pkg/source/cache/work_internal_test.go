package cache

import (
	"context"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"

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
