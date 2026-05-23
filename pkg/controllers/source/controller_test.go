package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

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
	st := store.New()
	ts := task.New()
	c := New(st, ts)
	for k, f := range fetchers {
		c.Fetchers[k] = f
	}
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		ts.BlockTillDone()
	})
	return c, st
}

func waitForStatus(t *testing.T, st *store.Store, id manifest.NamedResource, want store.Status) store.StatusInfo {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		info, ok := st.GetStatus(id)
		if ok && info.Status == want {
			return info
		}
		time.Sleep(5 * time.Millisecond)
	}
	info, _ := st.GetStatus(id)
	t.Fatalf("status %v not reached within deadline; last=%+v", want, info)
	return info
}

func TestController_FetchesAndStoresArtifact(t *testing.T) {
	f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "u", LocalPath: "/tmp"}}
	_, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(repo)

	waitForStatus(t, st, repo.Named(), store.StatusReady)
	if f.calls != 1 {
		t.Errorf("expected 1 fetch call, got %d", f.calls)
	}
	if art := st.GetArtifact(repo.Named()); art == nil {
		t.Errorf("expected artifact set")
	}
}

func TestController_FetchErrorMarksFailed(t *testing.T) {
	f := &fakeFetcher{err: errors.New("boom")}
	_, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(repo)

	info := waitForStatus(t, st, repo.Named(), store.StatusFailed)
	if info.Message != "boom" {
		t.Errorf("Failed reason = %q, want %q", info.Message, "boom")
	}
}

func TestController_SuspendedShortCircuitsToReady(t *testing.T) {
	f := &fakeFetcher{}
	_, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git", Suspend: true},
	}
	st.AddObject(repo)

	info := waitForStatus(t, st, repo.Named(), store.StatusReady)
	if info.Message != "suspended" {
		t.Errorf("expected suspended message; got %q", info.Message)
	}
	if f.calls != 0 {
		t.Errorf("suspended source must not fetch; calls=%d", f.calls)
	}
}

func TestController_UnregisteredKindIgnored(t *testing.T) {
	// Register an OCIRepository fetcher so the controller is alive but
	// has no entry for KindGitRepository. The AddObject path dispatches
	// listeners synchronously, so checking status immediately after
	// AddObject proves the unregistered branch returned without writing
	// any status — no sleep needed.
	registered := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindOCIRepository}}
	_, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: registered})

	unregistered := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(unregistered)
	if _, ok := st.GetStatus(unregistered.Named()); ok {
		t.Errorf("expected no status update for unregistered kind")
	}

	// Positive control: a registered kind reaches Ready, proving the
	// dispatcher is alive and the unregistered skip is targeted.
	oci := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example/img"},
	}
	st.AddObject(oci)
	waitForStatus(t, st, oci.Named(), store.StatusReady)
}

func TestController_AllowMissingSecretsConvertsFailureToSkip(t *testing.T) {
	f := &fakeFetcher{err: fmt.Errorf("%w: OCIRepository ns/r: secret ns/ghcr-creds not found",
		manifest.ErrMissingSecret)}

	st := store.New()
	ts := task.New()
	c := New(st, ts)
	c.Fetchers[manifest.KindOCIRepository] = f
	c.Configure(FetchOptions{AllowMissingSecrets: true})
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		ts.BlockTillDone()
	})

	repo := &manifest.OCIRepository{
		Name: "r", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example/img"},
	}
	st.AddObject(repo)

	info := waitForStatus(t, st, repo.Named(), store.StatusReady)
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
	_, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: f})

	repo := &manifest.OCIRepository{
		Name: "r", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example/img"},
	}
	st.AddObject(repo)

	waitForStatus(t, st, repo.Named(), store.StatusFailed)
}

func TestController_ChangeFilterSkipsUnaffected(t *testing.T) {
	f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository}}

	st := store.New()
	ts := task.New()
	// Filter that marks our repo as "no changes" — should short-circuit
	// to Ready without fetching.
	filter := change.NewFilter(
		change.NewSet(nil), // no changed files
		map[manifest.NamedResource]string{},
		"",
		mapLister{},
	)
	c := New(st, ts)
	c.Fetchers[manifest.KindGitRepository] = f
	c.Configure(FetchOptions{Filter: filter})
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		ts.BlockTillDone()
	})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "https://example/r.git"},
	}
	st.AddObject(repo)

	info := waitForStatus(t, st, repo.Named(), store.StatusReady)
	if info.Message != "unchanged" {
		t.Errorf("expected unchanged short-circuit; got %q", info.Message)
	}
	if f.calls != 0 {
		t.Errorf("filtered-out source must not fetch; calls=%d", f.calls)
	}
}

type mapLister map[manifest.NamedResource]manifest.BaseManifest

func (m mapLister) GetObject(id manifest.NamedResource) manifest.BaseManifest { return m[id] }
func (m mapLister) ListObjects(kind string) []manifest.BaseManifest {
	var out []manifest.BaseManifest
	for id, obj := range m {
		if id.Kind == kind {
			out = append(out, obj)
		}
	}
	return out
}
