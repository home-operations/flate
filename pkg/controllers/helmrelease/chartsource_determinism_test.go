package helmrelease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// tasksRenderInflight mirrors the orchestrator's orchestratorRenderInflight:
// quiescence is "the task pool drained to no productive work" — QuiescenceCh(0),
// because depwait callers reach it from inside YieldQuiescent (already
// decremented their own slot).
type tasksRenderInflight struct{ ts *task.Service }

func (r tasksRenderInflight) QuiescenceCh() <-chan struct{} { return r.ts.QuiescenceCh(0) }

// newChartSourceTestController builds an HR controller wired with a real task
// service exposed to the test, plus a render-quiescence signal backed by that
// service — the production shape, so awaitChartSource's YieldQuiescent +
// QuiescenceCh park behaves exactly as it does under the orchestrator.
func newChartSourceTestController(t *testing.T) (*Controller, *store.Store, *task.Service) {
	t.Helper()
	st := store.New()
	ts := task.New()
	hc, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))
	c := New(st, ts, hc, helm.Options{}, false)
	c.Configure(ReconcileOptions{Renders: tasksRenderInflight{ts}})
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		ts.BlockTillDone()
	})
	return c, st, ts
}

func chartSourceTestHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name: "stunner", Namespace: "stunner-system",
		// 50ms per-dep wall clock — far shorter than the simulated fetch.
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Timeout: &metav1.Duration{Duration: 50 * time.Millisecond},
		},
		Chart: manifest.HelmChart{
			RepoKind:      manifest.KindHelmRepository,
			RepoNamespace: "stunner-system",
			RepoName:      "stunner",
			Name:          "stunner",
			Version:       "1.2.0",
		},
	}
}

func addStunnerRepo(st *store.Store) {
	st.AddObject(&manifest.HelmRepository{
		Name: "stunner", Namespace: "stunner-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "https://l7mp.io/stunner"},
	})
}

// TestAwaitChartSource_ParksOnQuiescenceNotWallClock is the stunner
// determinism regression. A synthetic HelmChart whose fetch finishes only
// AFTER the per-dep DefaultTimeout/spec.timeout must still be observed Ready —
// the wait is bound to fetch-task completion (quiescence), not the wall clock.
//
// Before the fix awaitChartSource rode hr.Timeout (50ms) in watchOne and
// returned "chart source ... not ready: timeout" because the simulated fetch
// only sets Ready at 200ms — exactly the bug that dropped the stunner HR's
// ~478 lines nondeterministically. After the fix it parks on quiescence and
// returns nil every time.
func TestAwaitChartSource_ParksOnQuiescenceNotWallClock(t *testing.T) {
	// Repeat to assert determinism: the outcome must not depend on whether
	// the fetch beats the wall clock (it never does here — 200ms >> 50ms).
	for i := range 25 {
		c, st, ts := newChartSourceTestController(t)
		addStunnerRepo(st)
		hr := chartSourceTestHR()

		hr2, repointed := c.materializeHelmChartSource(hr.Named(), hr)
		if !repointed {
			t.Fatalf("iter %d: materialize did not repoint a HelmRepository chart", i)
		}
		srcID := chartSourceID(hr2)
		if info, ok := st.GetStatus(srcID); !ok || info.Status != store.StatusPending {
			t.Fatalf("iter %d: synthetic chart not seeded Pending (ok=%v status=%v)", i, ok, info.Status)
		}

		ctx := context.Background()
		// Simulated fetch task: holds the pool active (a plain Go body keeps
		// active incremented), sets Ready at 200ms — well past the 50ms
		// per-dep budget — then returns, dropping active so quiescence fires.
		ts.Go(ctx, "fake-fetch", func(context.Context) {
			time.Sleep(200 * time.Millisecond)
			st.UpdateStatus(srcID, store.StatusReady, "")
		})

		// The HR wait must run inside a task body (YieldQuiescent corrupts the
		// active counter if called outside one), exactly as the real reconcile.
		errCh := make(chan error, 1)
		ts.Go(ctx, "hr-wait", func(context.Context) {
			errCh <- c.awaitChartSource(ctx, hr.Named(), hr2)
		})

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("iter %d: awaitChartSource returned %v; want nil (chart became Ready at 200ms, must not time out at the 50ms wall clock)", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("iter %d: awaitChartSource did not return (parked wait never woke)", i)
		}
	}
}

// TestAwaitChartSource_FailedFetchStillFastFails guards the no-regression
// edge: a synthetic chart whose fetch FAILS must surface that fast (via the
// quiesce re-read), not hang to a ceiling.
func TestAwaitChartSource_FailedFetchStillFastFails(t *testing.T) {
	c, st, ts := newChartSourceTestController(t)
	addStunnerRepo(st)
	hr := chartSourceTestHR()
	hr.Timeout = &metav1.Duration{Duration: 5 * time.Second} // long: prove we don't ride it

	hr2, repointed := c.materializeHelmChartSource(hr.Named(), hr)
	if !repointed {
		t.Fatal("materialize did not repoint")
	}
	srcID := chartSourceID(hr2)

	ctx := context.Background()
	start := time.Now()
	ts.Go(ctx, "fake-fetch-fail", func(context.Context) {
		time.Sleep(50 * time.Millisecond)
		st.UpdateStatus(srcID, store.StatusFailed, "fetch failed: boom")
	})
	errCh := make(chan error, 1)
	ts.Go(ctx, "hr-wait", func(context.Context) {
		errCh <- c.awaitChartSource(ctx, hr.Named(), hr2)
	})

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("awaitChartSource returned nil for a Failed chart source; want an error")
		}
		if !errors.Is(err, manifest.ErrObjectNotFound) {
			t.Fatalf("error = %v; want wrapped ErrObjectNotFound", err)
		}
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("fast-fail took %v; expected well under the 5s hr.Timeout", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("awaitChartSource did not return on a Failed fetch")
	}
}

// TestAwaitChartSource_DeclaredGitChartSourceParks is the infisical-shaped
// regression: a DECLARED (non-synthetic) GitRepository-backed chart source —
// the case the old synthetic-only gate missed — must also be waited for by
// fetch outcome, not abandoned at the wall clock. The source is present+Pending
// and flips Ready at 200ms with a 50ms hr.Timeout; the park must hold until the
// fetch completes so awaitChartSource returns nil every run.
func TestAwaitChartSource_DeclaredGitChartSourceParks(t *testing.T) {
	for i := range 25 {
		c, st, ts := newChartSourceTestController(t)
		hr := &manifest.HelmRelease{
			Name: "infisical", Namespace: "infisical",
			HelmReleaseSpec: helmv2.HelmReleaseSpec{
				Timeout: &metav1.Duration{Duration: 50 * time.Millisecond},
			},
			// chartRef → GitRepository: a declared chart source the controller
			// does NOT materialize (materializeHelmChartSource is HelmRepository-only).
			Chart: manifest.HelmChart{
				RepoKind:      manifest.KindGitRepository,
				RepoNamespace: "infisical",
				RepoName:      "infisical",
			},
		}
		srcID := chartSourceID(hr)
		// Present + Pending = fetch in flight (a status seed stands in for the
		// source controller's dispatched git fetch).
		st.UpdateStatus(srcID, store.StatusPending, "fetching")

		ctx := context.Background()
		ts.Go(ctx, "fake-git-fetch", func(context.Context) {
			time.Sleep(200 * time.Millisecond)
			st.UpdateStatus(srcID, store.StatusReady, "")
		})
		errCh := make(chan error, 1)
		ts.Go(ctx, "hr-wait", func(context.Context) {
			errCh <- c.awaitChartSource(ctx, hr.Named(), hr)
		})

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("iter %d: awaitChartSource returned %v; want nil (declared git chart source Ready at 200ms must not time out at 50ms)", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("iter %d: awaitChartSource did not return", i)
		}
	}
}

// TestAwaitChartSource_AbsentSourceFastFails confirms the gate did NOT widen to
// absent/typo'd sources: a chart source with no store entry is a no-op for the
// AwaitFetch park (GetStatus !ok) and still fast-fails via the bounded c.Await,
// rather than parking on quiescence.
func TestAwaitChartSource_AbsentSourceFastFails(t *testing.T) {
	c, _, _ := newChartSourceTestController(t)
	// A direct HelmChart ref that was never added to the store.
	hr := &manifest.HelmRelease{
		Name: "app", Namespace: "apps",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Timeout: &metav1.Duration{Duration: 50 * time.Millisecond},
		},
		Chart: manifest.HelmChart{
			RepoKind:      manifest.KindHelmChart,
			RepoNamespace: "apps",
			RepoName:      "missing-chart",
		},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var err error
	start := time.Now()
	c.Tasks.Go(context.Background(), "absent-wait", func(context.Context) {
		defer wg.Done()
		err = c.awaitChartSource(context.Background(), hr.Named(), hr)
	})
	wg.Wait()
	if err == nil {
		t.Fatal("awaitChartSource returned nil for an absent source; want a not-found error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("absent-source wait took %v; expected to fast-fail near the wall clock", elapsed)
	}
}
