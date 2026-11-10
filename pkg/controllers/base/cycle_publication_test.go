package base_test

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestExecution_DelayedPreflightPublication(t *testing.T) {
	t.Parallel()
	const message = "dependency cycle detected: a → b → a"
	ids := []manifest.NamedResource{testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b")}
	for first := range ids {
		t.Run(ids[first].Name+"_first", func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			for _, id := range ids {
				st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			}
			captured, release, done := make(chan struct{}), make(chan struct{}), make(chan *base.Execution)
			c := base.New(st, tasks)
			tasks.Go(t.Context(), "delayed", func(context.Context) {
				close(captured)
				tasks.YieldSlot(func() { <-release })
				done <- controllertest.NewExecution(c, ids[1-first])
			})
			<-captured
			for _, id := range ids {
				st.SetPreflight(id, message)
			}
			peerDone := make(chan struct{})
			tasks.Go(t.Context(), "peer", func(ctx context.Context) {
				defer close(peerDone)
				peer := controllertest.NewExecution(base.New(st, tasks), ids[first])
				defer peer.Close()
				peer.Reads.Require(manifest.DependencyRef{NamedResource: ids[1-first]})
				peer.Settle(ctx, manifest.DrainForce)
			})
			<-peerDone
			close(release)
			delayed := <-done
			tasks.BlockTillDone()
			defer delayed.Close()
			delayed.Reads.Require(manifest.DependencyRef{NamedResource: ids[first]})
			delayed.Settle(t.Context(), manifest.DrainForce)
			info, _ := st.GetStatus(ids[1-first])
			t.Logf("delayed=%s preflight=%q valid=%t outcome=%s message=%s",
				ids[1-first], delayed.Reads.Preflight(), delayed.Valid(), info.Outcome(), info.String())
			assert.Equal(t, info.Outcome(), store.OutcomeFailed)
			assert.Equal(t, info.String(), message)
		})
	}
}

func TestExecution_CapturedBeforeCyclePublication(t *testing.T) {
	t.Parallel()
	const message = "dependency cycle detected: a → b → a"
	ids := []manifest.NamedResource{testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b")}
	for first := range ids {
		t.Run(ids[first].Name+"_first", func(t *testing.T) {
			t.Parallel()
			st := store.New()
			for _, id := range ids {
				st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			}
			c := base.New(st, nil)
			early := controllertest.NewExecution(c, ids[1-first])
			defer early.Close()
			assert.Equal(t, early.Reads.Preflight(), "")
			st.SetPreflight(ids[first], message)
			peer := controllertest.NewExecution(c, ids[first])
			peer.Reads.Require(manifest.DependencyRef{NamedResource: ids[1-first]})
			peer.Settle(t.Context(), manifest.DrainForce)
			peer.Close()
			early.Reads.Require(manifest.DependencyRef{NamedResource: ids[first]})
			early.Settle(t.Context(), manifest.DrainForce)
			before, _ := st.GetStatus(ids[1-first])
			assert.Equal(t, before.Outcome(), store.OutcomeBlocked)
			st.SetPreflight(ids[1-first], message)
			assert.Equal(t, early.Valid(), false)
			fresh := controllertest.NewExecution(c, ids[1-first])
			defer fresh.Close()
			fresh.Reads.Require(manifest.DependencyRef{NamedResource: ids[first]})
			fresh.Settle(t.Context(), manifest.DrainForce)
			after, _ := st.GetStatus(ids[1-first])
			assert.Equal(t, after.Outcome(), store.OutcomeFailed)
			assert.Equal(t, after.String(), message)
		})
	}
}
