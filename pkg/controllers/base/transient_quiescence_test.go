package base_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// svcRenders adapts a task.Service into depwait.RenderInflight so a test
// Waiter's quiescence wait is bound to the real pool's active counter.
type svcRenders struct{ svc *task.Service }

func (r svcRenders) QuiescenceCh() <-chan struct{} { return r.svc.QuiescenceCh(0) }

// TestAwait_TransientDrain_DoesNotDropPresentDepChain pins the
// transient-quiescence fix against the real task.Service active counter,
// exercised through base.Await (the fix site).
//
// Chain: src (a present source) ← mid (an HR whose own Await is parked on
// src) ← leaf (an HR whose Await is parked on mid). All present + status-
// bearing. When src's producer task writes Ready and EXITS, decrActive can
// drop the pool to 0 in the handoff window BEFORE mid re-activates and
// renders — a transient drain that, before the fix, fired QuiescenceCh and
// made leaf give up ("not ready") even though mid was mid-resume.
//
// The drain is forced deterministically: src's Ready+exit is gated on
// chSrcGo (after mid+leaf have parked), and mid is held at its post-resume /
// pre-Ready point on chMidGo (so leaf observes mid Pending when the drain
// fires). With the fix, a wait on present status-bearing deps uses YieldSlot
// (stays active) instead of YieldQuiescent, so the pool never hits 0 across
// the chain and leaf waits through to mid's terminal Ready.
func TestAwait_TransientDrain_DoesNotDropPresentDepChain(t *testing.T) {
	svc := task.New()
	s := store.New()
	c := base.New(s, svc)
	c.SetDepwait(nil, svcRenders{svc})

	src := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "storage", Name: "src"}
	mid := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "storage", Name: "mid"}
	leaf := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "storage", Name: "leaf"}
	s.UpdateStatus(src, store.StatusPending, "fetching")
	s.UpdateStatus(mid, store.StatusPending, "rendering")
	s.UpdateStatus(leaf, store.StatusPending, "rendering")

	chSrcGo := make(chan struct{})
	chMidGo := make(chan struct{})
	midResumed := make(chan struct{})
	leafErr := make(chan error, 1)
	var entered sync.WaitGroup
	entered.Add(2)

	// Producer: hold active until released, then write Ready and return
	// (the return's decrActive is what drops the pool).
	svc.Go(context.Background(), "src", func(context.Context) {
		<-chSrcGo
		s.UpdateStatus(src, store.StatusReady, "")
	})
	// mid: Await(src); on resume hold Pending until chMidGo, then Ready.
	svc.Go(context.Background(), "mid", func(ctx context.Context) {
		entered.Done()
		_ = c.Await(ctx, mid, c.NewWaiter(mid, nil), testutil.DepRefs(src), "", base.DepFailed(mid))
		close(midResumed)
		<-chMidGo
		s.UpdateStatus(mid, store.StatusReady, "")
	})
	// leaf: Await(mid) — the present dep whose producer (mid) is resumable.
	svc.Go(context.Background(), "leaf", func(ctx context.Context) {
		entered.Done()
		leafErr <- c.Await(ctx, leaf, c.NewWaiter(leaf, nil), testutil.DepRefs(mid), "", base.DepFailed(leaf))
	})

	entered.Wait()
	time.Sleep(50 * time.Millisecond)  // let mid+leaf reach the WatchReady park
	close(chSrcGo)                     // src → Ready, src task exits → transient active==0
	<-midResumed                       // mid resumed past its wait; mid still Pending
	time.Sleep(100 * time.Millisecond) // window for leaf to (wrongly) act on the drain
	close(chMidGo)                     // mid → Ready

	if err := <-leafErr; err != nil {
		t.Fatalf("leaf dropped by a TRANSIENT pool drain during mid's resume handoff: %v", err)
	}
	svc.BlockTillDone()
}
