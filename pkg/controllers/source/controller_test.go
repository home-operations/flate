package source

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type fakeFetcher struct {
	calls    int
	artifact *store.SourceArtifact
	err      error
}

func (f *fakeFetcher) Fetch(_ context.Context, _ manifest.BaseManifest) (*store.SourceArtifact, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.artifact, nil
}

func newController(t *testing.T, fetchers map[string]src.Fetcher) (*Controller, *store.Store) {
	t.Helper()
	return newConfiguredController(t, fetchers, FetchOptions{})
}

func newConfiguredController(t *testing.T, fetchers map[string]src.Fetcher, opts FetchOptions) (*Controller, *store.Store) {
	t.Helper()
	st := store.New()
	ts := task.New()
	c := New(st, ts)
	maps.Copy(c.Fetchers, fetchers)
	c.Configure(opts)
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		ts.BlockTillDone()
	})
	return c, st
}

// dispatchToFixpoint drives id through ReconcileNode with escalating drain
// levels (none→cascade→force), mirroring the scheduler's structural fixpoint,
// until the node terminalizes (no blocked deps) or drain is exhausted, then
// returns the final store status. Synchronous — replaces the event engine's
// AddObject→listener→WaitForStatus dispatch in unit tests.
//
// The source reconcile yields its worker slot around the fetch
// (Tasks.YieldSlot), which corrupts semaphore accounting unless it runs inside
// a Service.Go body that owns a slot. So each ReconcileNode call is dispatched
// through c.Tasks.Go exactly as the production scheduler does (schedule.go),
// and we block on its completion to keep the drive synchronous.
func dispatchToFixpoint(t *testing.T, c *Controller, st *store.Store, id manifest.NamedResource) store.StatusInfo {
	t.Helper()
	for _, drain := range []int{0, 1, 2} {
		if reconcileNode(c, id, drain) {
			break
		}
	}
	info, _ := st.GetStatus(id)
	return info
}

// reconcileNode runs one ReconcileNode pass for id inside a Tasks.Go worker
// (so the body's YieldSlot has a slot to release) and reports whether the node
// terminalized (no blocked deps).
func reconcileNode(c *Controller, id manifest.NamedResource, drain int) (terminal bool) {
	done := make(chan struct{})
	c.Tasks.Go(context.Background(), "test/"+id.String(), func(ctx context.Context) {
		blocked, _ := c.ReconcileNode(ctx, id, drain)
		terminal = len(blocked) == 0
		close(done)
	})
	<-done
	return terminal
}

func TestController_FetchesAndStoresArtifact(t *testing.T) {
	f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "u", LocalPath: "/tmp"}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(repo)

	info := dispatchToFixpoint(t, c, st, repo.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if f.calls != 1 {
		t.Errorf("expected 1 fetch call, got %d", f.calls)
	}
	if art := st.GetArtifact(repo.Named()); art == nil {
		t.Errorf("expected artifact set")
	}
}

func TestController_FetchErrorMarksFailed(t *testing.T) {
	f := &fakeFetcher{err: errors.New("boom")}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(repo)

	info := dispatchToFixpoint(t, c, st, repo.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.Message != "boom" {
		t.Errorf("Failed reason = %q, want %q", info.Message, "boom")
	}
}

func TestController_SuspendedShortCircuitsToReady(t *testing.T) {
	f := &fakeFetcher{}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git", Suspend: true},
	}
	st.AddObject(repo)

	info := dispatchToFixpoint(t, c, st, repo.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "suspended" {
		t.Errorf("expected suspended message; got %q", info.Message)
	}
	if f.calls != 0 {
		t.Errorf("suspended source must not fetch; calls=%d", f.calls)
	}
}

func TestController_UnregisteredKindIgnored(t *testing.T) {
	// Register an OCIRepository fetcher so the controller is alive but has no
	// entry for KindGitRepository. Under the dag scheduler the source controller
	// only exposes ReconcileNode for registered kinds (HasFetcher), so the
	// scheduler never dispatches the GitRepository — it must therefore carry no
	// status once the registered kind has been reconciled.
	registered := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindOCIRepository}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: registered})

	unregistered := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(unregistered)

	// Positive control: a registered kind reaches Ready, proving the controller
	// is alive and the unregistered skip is targeted.
	oci := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example/img"},
	}
	st.AddObject(oci)
	if info := dispatchToFixpoint(t, c, st, oci.Named()); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}

	// The unregistered kind was never dispatched, so it must have no status.
	if _, ok := st.GetStatus(unregistered.Named()); ok {
		t.Errorf("expected no status update for unregistered kind")
	}
}

func TestController_AllowMissingSecretsConvertsFailureToSkip(t *testing.T) {
	f := &fakeFetcher{err: fmt.Errorf("%w: OCIRepository ns/r: secret ns/ghcr-creds not found",
		manifest.ErrMissingSecret)}
	c, st := newConfiguredController(t,
		map[string]src.Fetcher{manifest.KindOCIRepository: f},
		FetchOptions{AllowMissingSecrets: true})

	repo := &manifest.OCIRepository{
		Name: "r", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example/img"},
	}
	st.AddObject(repo)

	info := dispatchToFixpoint(t, c, st, repo.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if !store.IsSkipped(info) {
		t.Errorf("expected skipped status, got %+v", info)
	}
	if !strings.Contains(info.Message, "ghcr-creds") {
		t.Errorf("skip message should preserve the underlying error; got %q", info.Message)
	}
	if art := st.GetArtifact(repo.Named()); art != nil {
		t.Errorf("skipped source must not produce an artifact; got %+v", art)
	}
}

func TestController_AllowMissingSecretsOffStillFails(t *testing.T) {
	// Same error, but flag is off — should fail.
	f := &fakeFetcher{err: fmt.Errorf("%w: OCIRepository ns/r: secret ns/ghcr-creds not found",
		manifest.ErrMissingSecret)}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: f})

	repo := &manifest.OCIRepository{
		Name: "r", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example/img"},
	}
	st.AddObject(repo)

	if info := dispatchToFixpoint(t, c, st, repo.Named()); info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
}

func TestController_ChangeFilterSkipsUnaffected(t *testing.T) {
	f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository}}

	// Filter that marks our repo as "no changes" — should short-circuit
	// to Ready without fetching.
	filter := change.NewFilter(
		change.NewSet(nil), // no changed files
		map[manifest.NamedResource]string{},
		"",
		testutil.MapLister{},
	)
	c, st := newConfiguredController(t,
		map[string]src.Fetcher{manifest.KindGitRepository: f},
		FetchOptions{Filter: filter})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(repo)

	info := dispatchToFixpoint(t, c, st, repo.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "unchanged" {
		t.Errorf("expected unchanged short-circuit; got %q", info.Message)
	}
	if f.calls != 0 {
		t.Errorf("filtered-out source must not fetch; calls=%d", f.calls)
	}
}

// TestController_ExistenceFetcher_NoTransientPendingOnReemit hardens the
// source path against the #657-class race. HelmRepository is the lone
// ExistenceFetcher (Fetch returns (nil,nil)), so it never sets an artifact
// and the reconcile's artifact short-circuit never catches its re-runs.
// Without the wasReady guard, a re-emitted HelmRepository flips
// Ready→Pending→Ready, and a consumer's quiescence-bound chart-source wait
// (helmrelease.awaitChartSource, which waits on the DECLARED HelmRepository
// before materializing a synthetic HelmChart) could re-read that transient
// Pending at a transient pool drain and drop the release. The source must
// stay Ready across a no-op re-run.
func TestController_ExistenceFetcher_NoTransientPendingOnReemit(t *testing.T) {
	c, st := newController(t, map[string]src.Fetcher{
		manifest.KindHelmRepository: src.ExistenceFetcher{},
	})
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "https://charts.example", Type: "default"},
	}
	st.AddObject(repo)
	if info := dispatchToFixpoint(t, c, st, repo.Named()); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}

	var mu sync.Mutex
	var sawPending bool
	st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != repo.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	// Re-emit with a benign spec diff (Interval) and drive a second reconcile.
	// ExistenceFetcher ignores the spec, so this re-run is a no-op fetch that
	// must keep the source Ready without a transient Pending write.
	reemit := &manifest.HelmRepository{
		Name: "charts", Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{
			URL: "https://charts.example", Type: "default",
			Interval: metav1.Duration{Duration: time.Hour},
		},
	}
	st.AddObject(reemit)
	reconcileNode(c, repo.Named(), 0)

	mu.Lock()
	defer mu.Unlock()
	if sawPending {
		t.Error("ExistenceFetcher source transiently downgraded Ready→Pending on a no-op re-run (the quiescence-race window)")
	}
	if info, _ := st.GetStatus(repo.Named()); info.Status != store.StatusReady {
		t.Errorf("source not Ready after no-op re-run: %+v", info)
	}
}
