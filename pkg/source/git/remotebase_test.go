package git

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func newRemoteBaseFetcher(t *testing.T) *Fetcher {
	t.Helper()
	root := t.TempDir()
	return &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
}

func assertWorktree(t *testing.T, input fs.FS, file, wantContent string) {
	t.Helper()
	got, err := fs.ReadFile(input, file)
	if err != nil {
		t.Fatalf("read materialized %s: %v", file, err)
	}
	if string(got) != wantContent {
		t.Errorf("%s = %q, want %q", file, got, wantContent)
	}
	if _, err := fs.Stat(input, ".git"); !os.IsNotExist(err) {
		t.Errorf("materialized base must not contain .git (stat err = %v)", err)
	}
}

func TestFetchRemoteBase_References(t *testing.T) {
	t.Parallel()
	src := filepath.Join(t.TempDir(), "a@b")
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "v1"})
	first := mustTagHEAD(t, src, "v0.2.2")
	mustSetRefToHash(t, src, "refs/heads/feature", first)
	head := mustCommitFile(t, src, "kustomization.yaml", "v2")
	mustSetRefToHash(t, src, "refs/heads/pinned", first)
	mustSetRefToHash(t, src, "refs/remotes/origin/pinned", head)
	for _, tc := range []struct {
		name, url, ref, revision, content string
	}{
		{"tag", "file://" + src, "v0.2.2", first, "v1"},
		{"branch", "file://" + src, "feature", first, "v1"},
		{"divergent named ref", "file://" + src, "refs/heads/pinned", first, "v1"},
		{"commit", "file://" + src, first, first, "v1"},
		{"HEAD file URL", "file://" + src, "", head, "v2"},
		{"HEAD local path", src, "", head, "v2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newRemoteBaseFetcher(t)
			art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: tc.url, Ref: tc.ref, Timeout: 27 * time.Second, Submodules: true}, sourcecache.Policy{MaxAge: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			if art.Revision != tc.revision {
				t.Errorf("revision = %q, want %q", art.Revision, tc.revision)
			}
			assertWorktree(t, art.FS, "kustomization.yaml", tc.content)
		})
	}
}

func TestFetchRemoteBase_Freshness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		hash bool
		age  time.Duration
	}{
		{name: "tag interval", age: time.Hour},
		{name: "due branch"},
		{name: "hash never expires", hash: true, age: time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := t.TempDir()
			mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "x"})
			hash := mustTagHEAD(t, src, "v1.0.0")
			ref := "v1.0.0"
			if tc.hash {
				ref = hash
			}
			if tc.age == 0 {
				ref = "master"
			}
			var advertisements, uploads, connections atomic.Int32
			handler := gitHTTPHandler(t, src)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					advertisements.Add(1)
				} else {
					uploads.Add(1)
				}
				handler.ServeHTTP(w, r)
			}))
			srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			srv.Start()
			t.Cleanup(srv.Close)
			root := t.TempDir()
			f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
			request := RemoteBaseRequest{URL: srv.URL, Ref: ref, Timeout: 27 * time.Second, Submodules: true}
			first, err := f.FetchRemoteBase(t.Context(), request, sourcecache.Policy{MaxAge: tc.age})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("initial: advertisements=%d upload_pack=%d accepted_connections=%d", advertisements.Load(), uploads.Load(), connections.Load())
			wantAdvertisements := int32(2)
			if tc.hash {
				wantAdvertisements = 1
			}
			if advertisements.Load() != wantAdvertisements || uploads.Load() != 1 {
				t.Errorf("initial: advertisements=%d upload_pack=%d, want %d and 1", advertisements.Load(), uploads.Load(), wantAdvertisements)
			}
			beforeAdvertisements, beforeUploads, beforeConnections := advertisements.Load(), uploads.Load(), connections.Load()
			wantContent, wantRevision := "x", first.Revision
			if tc.age == 0 {
				wantContent = "changed"
				wantRevision = mustCommitFile(t, src, "kustomization.yaml", wantContent)
			} else {
				srv.Close()
			}
			if err := f.Cache.Close(); err != nil {
				t.Fatal(err)
			}
			f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
			second, err := f.FetchRemoteBase(t.Context(), request, sourcecache.Policy{MaxAge: tc.age})
			if err != nil {
				t.Fatalf("offline cache hit: %v", err)
			}
			if first.Key != second.Key || wantRevision != second.Revision {
				t.Fatalf("cache identity/revision changed: %+v / %+v", first, second)
			}
			assertWorktree(t, second.FS, "kustomization.yaml", wantContent)
			t.Logf("reopened: advertisements=%d upload_pack=%d new_connections=%d", advertisements.Load()-beforeAdvertisements, uploads.Load()-beforeUploads, connections.Load()-beforeConnections)
			wantRequests := int32(0)
			if tc.age == 0 {
				wantRequests = 1
			}
			if advertisements.Load()-beforeAdvertisements != 2*wantRequests || uploads.Load()-beforeUploads != wantRequests {
				t.Errorf("reopened: advertisements=%d upload_pack=%d, want %d and %d", advertisements.Load()-beforeAdvertisements, uploads.Load()-beforeUploads, 2*wantRequests, wantRequests)
			}
			if wantRequests == 0 && connections.Load() != beforeConnections {
				t.Error("fresh remote base opened a connection")
			}
		})
	}
}

func TestFetchRemoteBase_RequiresCache(t *testing.T) {
	t.Parallel()
	f := &Fetcher{}
	_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file:///nope", Ref: "v1", Timeout: 27 * time.Second, Submodules: true}, sourcecache.Policy{MaxAge: time.Hour})
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("want ErrInput without cache, got %v", err)
	}
}

func TestFetchRemoteBase_RejectsBeforeAcquisition(t *testing.T) {
	t.Parallel()
	for _, url := range []string{"", "./a@b", "file:///a@b", "http://h/x@y"} {
		t.Run(url, func(t *testing.T) {
			t.Parallel()
			// The uninitialized cache panics if acquisition precedes validation.
			f := &Fetcher{Cache: new(sourcecache.Cache), RestrictSchemes: true}
			_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: url, Ref: "v1", Timeout: 27 * time.Second, Submodules: true}, sourcecache.Policy{MaxAge: time.Hour})
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("want ErrInput before acquisition, got %v", err)
			}
		})
	}
}

func TestFetchRemoteBase_TransientRecovery(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustInitRepoWithFiles(t, root, map[string]string{"kustomization.yaml": "content"})
	handler := gitHTTPHandler(t, root)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, "001e# service=git-upload-pack\n00000040abc")
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	f := newRemoteBaseFetcher(t)
	art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: srv.URL, Timeout: 27 * time.Second, Submodules: true}, sourcecache.Policy{MaxAge: time.Hour, Retry: sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}})
	if err != nil {
		t.Fatalf("original caller: %v; requests=%d", err, hits.Load())
	}
	assertWorktree(t, art.FS, "kustomization.yaml", "content")
	if hits.Load() < 3 {
		t.Fatalf("requests=%d, want failed advertisement plus clone requests", hits.Load())
	}
}

func TestFetchRemoteBase_GenerationAdmission(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			t.Parallel()
			src := malformedViewRepo(t)
			root := ""
			if disk {
				root = t.TempDir()
			}
			f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
			request := RemoteBaseRequest{URL: "file://" + src, Timeout: 27 * time.Second, Submodules: false}
			policy := sourcecache.Policy{MaxAge: time.Hour}
			artifact, err := f.FetchRemoteBase(t.Context(), request, policy)
			if !errors.Is(err, manifest.ErrInput) || artifact == nil || artifact.FS != nil || len(artifact.Warnings) != 1 || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule || artifact.Warnings[0].Resource != (manifest.NamedResource{}) {
				t.Fatalf("cold invalid view = %v, %v", artifact, err)
			}
			if err := os.RemoveAll(src); err != nil {
				t.Fatal(err)
			}
			mustInitRepo(t, src)
			artifact, err = f.FetchRemoteBase(t.Context(), request, policy)
			if err != nil || artifact == nil {
				t.Fatalf("repaired generation = %v, %v", artifact, err)
			}
			_, identity, viewErr := source.OperationView(t.Context(), artifact.FS)
			if viewErr != nil || identity == "" || artifact.Content != identity {
				t.Fatalf("returned generation = %q, want %q: %v", artifact.Content, identity, viewErr)
			}
			assertWorktree(t, artifact.FS, "hello.txt", "hi")
			t.Run("waiter and hit", func(t *testing.T) {
				t.Parallel()
				testGenerationWaiter(t, disk, func(ctx context.Context, f *Fetcher, url string) (*store.SourceArtifact, error) {
					return f.FetchRemoteBase(ctx, RemoteBaseRequest{URL: url, Timeout: 27 * time.Second, Submodules: false}, policy)
				}, []string{".sourceignore", "apps/drop.tmp", "apps/keep.yaml", "other/keep.yaml"})
			})
		})
	}
}

func TestFetchRemoteBase_DefaultSubmodules(t *testing.T) {
	t.Parallel()
	module := t.TempDir()
	mustInitRepo(t, module)
	oldModule := mustHead(t, module)
	newModule := mustCommitFile(t, module, "hello.txt", "new module")
	parent := t.TempDir()
	mustInitRepoWithFiles(t, parent, map[string]string{
		"hello.txt": "parent", ".gitmodules": "[submodule \"lib\"]\n\tpath = lib\n\turl = file://" + module + "\n",
	})
	repo, err := git.PlainOpen(parent)
	if err != nil {
		t.Fatal(err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	idx, err := repo.Storer.Index()
	if err != nil {
		t.Fatal(err)
	}
	link := &index.Entry{Name: "lib", Mode: filemode.Submodule}
	idx.Entries = append(idx.Entries, link)
	var pin plumbing.Hash
	for _, hash := range []string{oldModule, newModule} {
		link.Hash = plumbing.NewHash(hash)
		if err := repo.Storer.SetIndex(idx); err != nil {
			t.Fatal(err)
		}
		hash, err := wt.Commit("module revision", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
		if err != nil {
			t.Fatal(err)
		}
		if pin.IsZero() {
			pin = hash
		}
	}
	t.Run("recursion changes content", func(t *testing.T) {
		t.Parallel()
		f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
		for _, recurse := range []bool{false, true} {
			artifact, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + parent, Ref: pin.String(), Timeout: 27 * time.Second, Submodules: recurse}, sourcecache.Policy{MaxAge: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			body, err := fs.ReadFile(artifact.FS, "lib/hello.txt")
			if recurse && (err != nil || string(body) != "hi") || !recurse && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("recurse=%t body=%q error=%v", recurse, body, err)
			}
		}
	})

	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote=%t", remote), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, wave := range []string{"cold", "reopened"} {
				f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
				var artifact *store.SourceArtifact
				var err error
				if remote {
					artifact, err = f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + parent, Ref: pin.String(), Timeout: 27 * time.Second, Submodules: true}, sourcecache.Policy{MaxAge: time.Hour})
				} else {
					r := &manifest.GitRepository{URL: "file://" + parent, Reference: &sourcev1.GitRepositoryRef{Commit: pin.String()}, RecurseSubmodules: true}
					artifact, err = f.Fetch(t.Context(), r, (testInputs{}).readSet(r), sourcecache.Policy{MaxAge: time.Hour})
				}
				if err != nil {
					t.Fatalf("%s fetch: %v", wave, err)
				}
				body, err := fs.ReadFile(artifact.FS, "lib/hello.txt")
				if err != nil || string(body) != "hi" {
					t.Errorf("%s requested-ref submodule: body=%q error=%v, want hi", wave, body, err)
				}
				if err := f.Cache.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type commandTransport struct {
	transport.Transport
	before func(context.Context) error
}

type commandSession struct {
	transport.UploadPackSession
	before func(context.Context) error
}

func (tr commandTransport) NewUploadPackSession(endpoint *transport.Endpoint, auth transport.AuthMethod) (transport.UploadPackSession, error) {
	local := *endpoint
	local.Protocol = "file"
	local.Path = filepath.Join(local.Path, ".git")
	session, err := tr.Transport.NewUploadPackSession(&local, auth)
	if err != nil {
		return nil, err
	}
	return commandSession{UploadPackSession: session, before: tr.before}, nil
}

func (s commandSession) AdvertisedReferencesContext(ctx context.Context) (*packp.AdvRefs, error) {
	if err := s.before(ctx); err != nil {
		return nil, err
	}
	return s.UploadPackSession.AdvertisedReferencesContext(ctx)
}

func installCommandTransport(t *testing.T, before func(context.Context) error) {
	t.Helper()
	client.InstallProtocol("commands", commandTransport{Transport: server.DefaultServer, before: before})
	t.Cleanup(func() { client.InstallProtocol("commands", nil) })
}

func TestFetchRemoteBase_CommandBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := t.TempDir()
		mustInitRepo(t, src)
		calls := 0
		installCommandTransport(t, func(ctx context.Context) error {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) != 5*time.Second {
				t.Errorf("command %d deadline=%v, present=%t", calls, time.Until(deadline), ok)
			}
			select {
			case <-time.After(3 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
		start := time.Now()
		aggregate := time.Second
		artifact, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "commands://" + src, Timeout: 5 * time.Second}, sourcecache.Policy{Timeout: &aggregate})
		if err != nil || artifact == nil || calls < 2 || time.Since(start) < 6*time.Second {
			t.Fatalf("independent commands: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
		}
	})
}

func TestFetchRemoteBase_TimeoutPolicies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := t.TempDir()
		mustInitRepo(t, src)
		started := make(chan struct{}, 1)
		var calls atomic.Int32
		installCommandTransport(t, func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				started <- struct{}{}
			}
			select {
			case <-time.After(2 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		root := t.TempDir()
		f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
		short, long := make(chan error, 1), make(chan error, 1)
		request := RemoteBaseRequest{URL: "commands://" + src, Timeout: time.Second}
		shortRequest := request
		go func() {
			_, err := f.FetchRemoteBase(t.Context(), shortRequest, sourcecache.Policy{MaxAge: time.Hour})
			short <- err
		}()
		<-started
		request.Timeout = 5 * time.Second
		longRequest := request
		go func() {
			_, err := f.FetchRemoteBase(t.Context(), longRequest, sourcecache.Policy{MaxAge: time.Hour})
			long <- err
		}()
		if err := <-short; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("short policy error=%v", err)
		}
		if err := <-long; err != nil {
			t.Fatalf("longer concurrent policy replayed timeout: %v", err)
		}
		if err := f.Cache.Close(); err != nil {
			t.Fatal(err)
		}
		f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
		before := calls.Load()
		request.Timeout = time.Millisecond
		artifact, err := f.FetchRemoteBase(t.Context(), request, sourcecache.Policy{MaxAge: time.Hour})
		if err != nil || artifact == nil || calls.Load() != before {
			t.Fatalf("successful content was keyed by timeout: calls=%d->%d error=%v", before, calls.Load(), err)
		}
	})
}
