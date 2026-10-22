package git

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_PackedContent(t *testing.T) {
	t.Parallel()
	var blocks [8]string
	for i := range blocks {
		var block strings.Builder
		for line := range 200 {
			fmt.Fprintf(&block, "%x\n", sha256.Sum256(fmt.Appendf(nil, "%d-%d", i, line)))
		}
		blocks[i] = block.String()
	}
	files := map[string]string{"base.txt": strings.Join(blocks[:], ""), "reordered.txt": ""}
	for _, i := range []int{3, 0, 5, 1, 7, 2, 6, 4} {
		files["reordered.txt"] += blocks[i]
	}
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, files)
	for _, mode := range []string{"memory", "disk"} {
		for _, attempts := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/attempts-%d", mode, attempts), func(t *testing.T) {
				t.Parallel()
				var acquisitions atomic.Int32
				handler := gitHTTPHandler(t, src)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet && acquisitions.Add(1) == 1 {
						w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
						w.Header().Set("Content-Length", "100")
						_, _ = io.WriteString(w, "001e# service=git-upload-pack\n00000040abc")
						return
					}
					handler.ServeHTTP(w, r)
				}))
				defer srv.Close()
				root := ""
				if mode == "disk" {
					root = t.TempDir()
				}
				f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
				art, err := f.Fetch(t.Context(), &manifest.GitRepository{Name: "packed", Namespace: "flux-system", URL: srv.URL}, (testInputs{}).readSet(&manifest.GitRepository{Name: "packed", Namespace: "flux-system", URL: srv.URL}), sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}})
				if attempts == 1 {
					if err == nil || art != nil || acquisitions.Load() != 1 {
						t.Fatalf("retry-off artifact=%v, error=%v, acquisitions=%d", art, err, acquisitions.Load())
					}
					return
				}
				if err != nil || acquisitions.Load() != 2 {
					t.Fatalf("original caller error=%v, acquisitions=%d, want 2", err, acquisitions.Load())
				}
				var readers sync.WaitGroup
				for range 2 {
					readers.Go(func() {
						for file, want := range files {
							got, err := fs.ReadFile(art.FS, file)
							if err != nil || string(got) != want {
								t.Errorf("packed %s content differs: bytes=%d, err=%v", file, len(got), err)
							}
						}
					})
				}
				readers.Wait()
			})
		}
	}
}

func TestFetcher_SourceLinksOmitted(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"hello.txt": "hi", "apps/cm.yaml": "kind: ConfigMap"})
	for name, target := range map[string]string{"alias": "hello.txt", "dirlink": "apps", ".sourceignore": "hello.txt"} {
		if err := os.Symlink(target, filepath.Join(src, name)); err != nil {
			t.Fatal(err)
		}
	}
	mustCommitFile(t, src, "hello.txt", "hi")
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	art, err := f.Fetch(t.Context(), &manifest.GitRepository{Name: "links", URL: "file://" + src}, (testInputs{}).readSet(&manifest.GitRepository{Name: "links", URL: "file://" + src}), sourcecache.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alias", "dirlink", "dirlink/cm.yaml", ".sourceignore"} {
		if file, err := art.FS.Open(name); !errors.Is(err, fs.ErrPermission) {
			if file != nil {
				_ = file.Close()
			}
			t.Errorf("Open(%s): %v", name, err)
		}
		if _, err := fs.Stat(art.FS, name); !errors.Is(err, fs.ErrPermission) {
			t.Errorf("Stat(%s): %v", name, err)
		}
		if _, err := fs.ReadDir(art.FS, name); !errors.Is(err, fs.ErrPermission) {
			t.Errorf("ReadDir(%s): %v", name, err)
		}
	}
	entries, err := fs.ReadDir(art.FS, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "alias" || entry.Name() == "dirlink" || entry.Name() == ".sourceignore" {
			t.Errorf("listed link %s", entry.Name())
		}
	}
}

func TestFetcher_LocalFileURL(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
	}

	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	_, pin, err := source.OperationView(t.Context(), art.FS)
	assert.NoError(t, err)
	assert.Equal(t, pin == "", false)
	sa := art
	if sa.FS == nil || sa.Key == "" || sa.URL == "" {
		t.Errorf("incomplete artifact: %+v", sa)
	}
	if _, err := fs.Stat(sa.FS, path.Join("hello.txt")); err != nil {
		t.Errorf("expected checked-out file: %v", err)
	}

	art2, err := f.Fetch(context.Background(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	_, nextPin, err := source.OperationView(t.Context(), art2.FS)
	assert.NoError(t, err)
	assert.Equal(t, nextPin, pin)
	if art2.Revision != sa.Revision {
		t.Errorf("unchanged default ref revision changed: %s vs %s", sa.Revision, art2.Revision)
	}
}

func TestFetcher_RestrictedEndpointSchemes(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"https://github.com/o/r":    true,
		"ssh://git@github.com/o/r":  true,
		"git@github.com:o/r.git":    true,
		"user@host.xz:path/to/repo": true,
		"host.xz:path/to/repo":      false,
		"file:///srv/repo":          false,
		"git://github.com/o/r":      false,
		"http://github.com/o/r":     false,
		"/bare/local/path":          false,
		"./relative":                false,
		"./a@b":                     false,
		"ssh-but-not-scheme://x":    false,
		"":                          false,
	}
	for url, want := range cases {
		t.Run(url, func(t *testing.T) {
			t.Parallel()
			f := &Fetcher{RestrictSchemes: true}
			err := f.validateRepo(&manifest.GitRepository{URL: url})
			if want && err != nil || !want && !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("allowed = %t, validateRepo = %v", want, err)
			}
			if !want {
				f.Cache = new(sourcecache.Cache)
				_, err := f.Fetch(t.Context(), &manifest.GitRepository{URL: url}, (testInputs{}).readSet(&manifest.GitRepository{URL: url}), sourcecache.Policy{})
				if !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("public Fetch must reject before acquisition: %v", err)
				}
				if strings.HasPrefix(url, "file://") && !strings.Contains(err.Error(), "scheme not allowed") {
					t.Fatalf("error must name the rejected scheme: %v", err)
				}
			}
		})
	}
}

func TestFetcher_Freshness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		interval              time.Duration
		depth                 int
		recurse, fail, pinned bool
		commit, mixed         bool
	}{
		{name: "mutable default"},
		{name: "fresh interval", interval: time.Hour},
		{name: "commit-only pin", pinned: true},
		{name: "acquisition identity", recurse: true},
		{name: "failed refresh", fail: true},
		{name: "advanced shallow tip", depth: 1},
		{name: "commit retained", commit: true},
		{name: "mixed commit refreshes", commit: true, mixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := t.TempDir()
			mustInitRepo(t, src)
			initial := mustHead(t, src)
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
			cacheDir := t.TempDir()
			f := &Fetcher{Cache: cachetest.New(t, cacheDir, sourcecache.New), Depth: tc.depth}
			repo := &manifest.GitRepository{
				Name: "test", Namespace: "flux-system", URL: srv.URL,
				Interval: metav1.Duration{Duration: tc.interval}, RecurseSubmodules: tc.recurse,
			}
			if tc.pinned || tc.commit {
				repo.Reference = &sourcev1.GitRepositoryRef{Commit: initial}
				if tc.mixed {
					repo.Reference.Tag = "ignored"
				}
			}
			first, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
			assert.NoError(t, err)
			assert.Equal(t, first.Revision, initial)
			t.Logf("initial: advertisements=%d upload_pack=%d accepted_connections=%d", advertisements.Load(), uploads.Load(), connections.Load())
			if advertisements.Load() != 1 || uploads.Load() != 1 {
				t.Errorf("initial clone: advertisements=%d upload_pack=%d, want 1 each", advertisements.Load(), uploads.Load())
			}
			beforeAdvertisements, beforeUploads, beforeConnections := advertisements.Load(), uploads.Load(), connections.Load()
			if tc.fail {
				srv.Close()
				if _, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()}); err == nil {
					t.Fatal("expected refresh to fail after upstream shutdown")
				}
				if _, err := fs.Stat(first.FS, "hello.txt"); err != nil {
					t.Fatalf("failed refresh removed held generation: %v", err)
				}
				data, err := fs.ReadFile(first.FS, "hello.txt")
				assert.NoError(t, err)
				assert.Equal(t, string(data), "hi")
				return
			}

			if tc.commit {
				srv.Close()
				assert.NoError(t, os.RemoveAll(src))
				second, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: tc.interval})
				if tc.mixed {
					if err == nil || second != nil {
						t.Fatal("mixed commit unexpectedly retained forever")
					}
				} else {
					assert.NoError(t, err)
					assert.Equal(t, second.Revision, initial)
				}
				return
			}

			mustCommitFile(t, src, "hello.txt", "updated")
			tip := mustCommitFile(t, src, "later.txt", "new content")
			if tip == initial {
				t.Fatal("tip did not move")
			}
			second, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
			assert.NoError(t, err)
			t.Logf("refresh: advertisements=%d upload_pack=%d new_connections=%d", advertisements.Load()-beforeAdvertisements, uploads.Load()-beforeUploads, connections.Load()-beforeConnections)
			if tc.interval != 0 || tc.pinned {
				if advertisements.Load() != beforeAdvertisements || uploads.Load() != beforeUploads || connections.Load() != beforeConnections {
					t.Error("fresh interval or commit-only pin performed network work")
				}
			}
			if tc.interval == 0 && !tc.pinned && (advertisements.Load()-beforeAdvertisements != 1 || uploads.Load()-beforeUploads != 1) {
				t.Errorf("due changed ref: advertisements=%d upload_pack=%d, want 1 each", advertisements.Load()-beforeAdvertisements, uploads.Load()-beforeUploads)
			}
			want := tip
			if tc.interval != 0 || tc.pinned {
				want = initial
			}
			assert.Equal(t, second.Revision, want)
			assert.Equal(t, second.Key, first.Key)
			_, err = fs.Stat(second.FS, "later.txt")
			if tc.interval != 0 || tc.pinned {
				if !os.IsNotExist(err) {
					t.Fatalf("fresh interval included later commit: %v", err)
				}
			} else {
				assert.NoError(t, err)
			}
			cached := *repo
			cached.Interval.Duration = time.Hour
			beforeAdvertisements, beforeUploads, beforeConnections = advertisements.Load(), uploads.Load(), connections.Load()
			hit, err := f.Fetch(t.Context(), &cached, (testInputs{}).readSet(&cached), sourcecache.Policy{MaxAge: cached.SourceInterval()})
			assert.NoError(t, err)
			assert.Equal(t, hit.Revision, want)
			assert.Equal(t, hit.Key, second.Key)
			if advertisements.Load() != beforeAdvertisements || uploads.Load() != beforeUploads || connections.Load() != beforeConnections {
				t.Error("fresh follow-up performed network work")
			}
			for _, art := range []*store.SourceArtifact{first, second, hit} {
				view, pin, err := source.OperationView(t.Context(), art.FS)
				assert.NoError(t, err)
				assert.Equal(t, pin != "", true)
				assert.Equal(t, pin == "", false)
				assert.Equal(t, view == art.FS, true)
				data, err := fs.ReadFile(view, "hello.txt")
				assert.NoError(t, err)
				content := "updated"
				if art == first || tc.interval != 0 || tc.pinned {
					content = "hi"
				}
				assert.Equal(t, string(data), content)
			}
			assert.NoError(t, f.Cache.Close())
			f = &Fetcher{Cache: cachetest.New(t, cacheDir, sourcecache.New), Depth: tc.depth}
			reopened, err := f.Fetch(t.Context(), &cached, (testInputs{}).readSet(&cached), sourcecache.Policy{MaxAge: cached.SourceInterval()})
			assert.NoError(t, err)
			assert.Equal(t, reopened.Revision, want)
			t.Logf("reopened fresh: advertisements=%d upload_pack=%d new_connections=%d", advertisements.Load()-beforeAdvertisements, uploads.Load()-beforeUploads, connections.Load()-beforeConnections)
			if advertisements.Load() != beforeAdvertisements || uploads.Load() != beforeUploads || connections.Load() != beforeConnections {
				t.Error("reopened fresh owner performed network work")
			}
		})
	}
}

func TestFetcher_References(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"hello.txt": "hi", "apps/a/manifest.yaml": "kind: ConfigMap", "apps/b/manifest.yaml": "kind: Secret",
	})
	first := mustTagHEAD(t, src, "v1.0.0")
	mustSetRefToHash(t, src, "refs/heads/staging", first)
	mustCommitFile(t, src, "second.txt", "two")
	mustCommitFile(t, src, "chart.yaml", "tagged")
	tip := mustCommitFile(t, src, "version.txt", "v2")
	mustTagHEAD(t, src, "v2.0.0")
	mustSetRefToHEAD(t, src, "refs/pull/1/head")
	for _, tc := range []struct {
		name          string
		ref           *sourcev1.GitRepositoryRef
		depth         int
		sparse        []string
		want          string
		files, absent []string
		warmCommit    string
		recurse       bool
		wantErr       bool
		errText       string
	}{
		{name: "HEAD", depth: 1, want: tip, files: []string{"hello.txt", "second.txt"}},
		{name: "tag", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, depth: 1, want: tip, files: []string{"chart.yaml"}},
		{name: "branch", ref: &sourcev1.GitRepositoryRef{Branch: "staging"}, depth: 1, want: first, files: []string{"hello.txt"}, absent: []string{"version.txt"}},
		{name: "branch recursive checkout", ref: &sourcev1.GitRepositoryRef{Branch: "staging"}, recurse: true, want: first, files: []string{"hello.txt"}, absent: []string{"version.txt"}},
		{name: "name", ref: &sourcev1.GitRepositoryRef{Name: "refs/tags/v1.0.0"}, want: first},
		{name: "unresolvable name", ref: &sourcev1.GitRepositoryRef{Name: "refs/heads/does-not-exist"}, wantErr: true},
		{name: "explicit name fallback", ref: &sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"}, sparse: []string{"apps/a"}, want: tip, files: []string{"apps/a/manifest.yaml"}, absent: []string{"apps/b/manifest.yaml"}},
		{name: "semver", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0", SemVer: ">=1.1.0"}, want: tip, files: []string{"version.txt"}},
		{name: "shallow semver", ref: &sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"}, depth: 1, want: tip, files: []string{"version.txt"}},
		{name: "commit", ref: &sourcev1.GitRepositoryRef{Commit: first}, want: first, absent: []string{"version.txt"}},
		{name: "commit precedes name", ref: &sourcev1.GitRepositoryRef{Commit: first, Name: "refs/tags/v2.0.0"}, want: first, absent: []string{"version.txt"}},
		{name: "commit precedes unresolvable name", ref: &sourcev1.GitRepositoryRef{Commit: first, Name: "refs/pull/does-not-exist/head"}, want: first},
		{name: "commit branch reachability", ref: &sourcev1.GitRepositoryRef{Commit: tip, Branch: "staging"}, warmCommit: tip, wantErr: true, errText: "not reachable from branch"},
		{name: "shallow sparse", depth: 1, sparse: []string{"apps/a"}, want: tip, files: []string{"apps/a/manifest.yaml"}, absent: []string{"apps/b/manifest.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Depth: tc.depth}
			repo := &manifest.GitRepository{Name: "test", Namespace: "flux-system", URL: "file://" + src, Reference: tc.ref, SparseCheckout: tc.sparse, RecurseSubmodules: tc.recurse}
			if tc.warmCommit != "" {
				unconstrained := *repo
				unconstrained.Reference = &sourcev1.GitRepositoryRef{Commit: tc.warmCommit}
				_, err := f.Fetch(t.Context(), &unconstrained, (testInputs{}).readSet(&unconstrained), sourcecache.Policy{MaxAge: unconstrained.SourceInterval()})
				assert.NoError(t, err)
			}
			art, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
			if tc.wantErr {
				if err == nil || tc.errText != "" && !strings.Contains(err.Error(), tc.errText) {
					t.Fatalf("Fetch error = %v, want failure containing %q", err, tc.errText)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, art.Revision, tc.want)
			for _, file := range tc.files {
				if _, err := fs.Stat(art.FS, file); err != nil {
					t.Errorf("included file %s missing: %v", file, err)
				}
			}
			for _, file := range tc.absent {
				if _, err := fs.Stat(art.FS, file); !os.IsNotExist(err) {
					t.Errorf("excluded file %s present: %v", file, err)
				}
			}
		})
	}
}

func TestFetcher_SparseCheckout(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/a/manifest.yaml":     "kind: ConfigMap",
		"apps/b/manifest.yaml":     "kind: ConfigMap",
		"apps-extra/manifest.yaml": "kind: ConfigMap",
		"infra/c/manifest.yaml":    "kind: ConfigMap",
	})
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	for _, prefix := range []string{"apps/a", "apps"} {
		t.Run(prefix, func(t *testing.T) {
			t.Parallel()
			art, err := f.Fetch(t.Context(), &manifest.GitRepository{
				Name: "test", Namespace: "flux-system", URL: "file://" + src,
				SparseCheckout: []string{prefix},
			}, (testInputs{}).readSet(&manifest.GitRepository{
				Name: "test", Namespace: "flux-system", URL: "file://" + src,
				SparseCheckout: []string{prefix},
			}), sourcecache.Policy{})
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{"apps/a/manifest.yaml", "apps/b/manifest.yaml", "apps-extra/manifest.yaml", "infra/c/manifest.yaml"} {
				_, err := fs.Stat(art.FS, file)
				if strings.HasPrefix(file, prefix) {
					if err != nil {
						t.Errorf("included %s: %v", file, err)
					}
				} else if !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("excluded %s: %v", file, err)
				}
			}
		})
	}
}

// TestFetcher_AppliesSpecIgnore exercises the spec.ignore wiring: a
// staged tree containing a tracked file matching the user-supplied
// gitignore pattern must have that file deleted post-checkout.
func TestFetcher_AppliesSpecIgnore(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/deployment.yaml": "kind: Deployment",
		"apps/scratch.tmp":     "noise",
		"docs/notes.md":        "drop-me",
	})

	patterns := "*.tmp\ndocs/\n"
	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:    "file://" + src,
		Ignore: &patterns,
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if _, err := fs.Stat(art.FS, path.Join("apps", "scratch.tmp")); !os.IsNotExist(err) {
		t.Errorf("expected *.tmp to be removed by spec.ignore; stat err = %v", err)
	}
	if _, err := fs.Stat(art.FS, path.Join("docs")); !os.IsNotExist(err) {
		t.Errorf("expected docs/ to be removed by spec.ignore; stat err = %v", err)
	}
	if _, err := fs.Stat(art.FS, path.Join("apps", "deployment.yaml")); err != nil {
		t.Errorf("non-ignored file should remain: %v", err)
	}
}

// `.flate-git-revision` marker survives ApplyIgnore (the default
// sourceignore patterns don't match `.flate-*`) and lets the next
// Fetch validate the cache without `.git/` — which the ignore step
// itself wipes via the VCS-excludes preset.
func TestFetcher_CommitHitSurvivesIgnore(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	patterns := "/*\n!/hello.txt\n"
	commit := mustHead(t, src)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Commit: commit},
		Ignore:    &patterns,
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if _, err := fs.Stat(art.FS, ".git"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("git metadata visible: %v", err)
	}
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	art2, err := f.Fetch(context.Background(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.Revision != art.Revision {
		t.Errorf("warm Fetch returned a different revision: %q vs %q", art2.Revision, art.Revision)
	}
}

func mustInitRepoWithFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	r := testutil.GitInit(t, dir)
	for path, content := range files {
		testutil.WriteFile(t, dir, path, content)
	}
	testutil.GitCommitAll(t, r, "init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
}

// mustTagHEAD creates an annotated tag pointing at the worktree HEAD
// and returns the tagged commit SHA.
func mustTagHEAD(t *testing.T, dir, tag string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if _, err := r.CreateTag(tag, head.Hash(), nil); err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	return head.Hash().String()
}

func mustHead(t *testing.T, dir string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	return head.Hash().String()
}

func mustSetRefToHash(t *testing.T, dir, refName, hash string) {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	ref := plumbing.NewHashReference(plumbing.ReferenceName(refName), plumbing.NewHash(hash))
	if err := r.Storer.SetReference(ref); err != nil {
		t.Fatalf("SetReference %s: %v", refName, err)
	}
}

func mustCommitFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	full := filepath.Join(dir, name)
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	hash := testutil.GitCommitAll(t, r, "update "+name, &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(1, 0)},
	})
	return hash.String()
}

func mustInitRepo(t *testing.T, dir string) {
	t.Helper()
	r := testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "hello.txt", "hi")
	testutil.GitCommitAll(t, r, "init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
}

func mustSetRefToHEAD(t *testing.T, dir, refName string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	ref := plumbing.NewHashReference(plumbing.ReferenceName(refName), head.Hash())
	if err := r.Storer.SetReference(ref); err != nil {
		t.Fatalf("SetReference %s: %v", refName, err)
	}
	return head.Hash().String()
}

func malformedViewRepo(t *testing.T) string {
	t.Helper()
	root, repo, _, hash := localFixture(t)
	commit, err := repo.CommitObject(hash)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	tree.Entries[0].Name = "bad/name"
	tree.Entries = append([]object.TreeEntry{{Name: "a-module", Mode: filemode.Submodule, Hash: hash}}, tree.Entries...)
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	hash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	head, err := repo.Head()
	assert.NoError(t, err)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(head.Name(), hash)))
	return root
}

func TestFetcher_GenerationAdmission(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			t.Parallel()
			for _, invalid := range []string{"metadata", "ignore"} {
				t.Run(invalid, func(t *testing.T) {
					t.Parallel()
					src := t.TempDir()
					if invalid == "metadata" {
						src = malformedViewRepo(t)
					} else {
						mustInitRepoWithFiles(t, src, map[string]string{".sourceignore/bad": "not a file"})
					}
					root := ""
					if disk {
						root = t.TempDir()
					}
					f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
					repo := &manifest.GitRepository{URL: "file://" + src}
					policy := sourcecache.Policy{MaxAge: time.Hour}
					artifact, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), policy)
					if !errors.Is(err, manifest.ErrInput) || invalid == "metadata" && (artifact == nil || artifact.FS != nil || len(artifact.Warnings) != 1 || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule || artifact.Warnings[0].Resource != repo.Named()) || invalid == "ignore" && artifact != nil {
						t.Fatalf("cold invalid view = %v, %v", artifact, err)
					}
					assert.NoError(t, os.RemoveAll(src))
					mustInitRepo(t, src)
					artifact, err = f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), policy)
					assert.NoError(t, err)
					assert.Equal(t, artifact.Revision, mustHead(t, src))
					assertWorktree(t, artifact.FS, "hello.txt", "hi")
				})
			}
			t.Run("waiter and hit", func(t *testing.T) {
				t.Parallel()
				testGenerationWaiter(t, disk, func(ctx context.Context, f *Fetcher, url string) (*store.SourceArtifact, error) {
					return f.Fetch(ctx, &manifest.GitRepository{
						URL: url, SparseCheckout: []string{"apps"}, Ignore: new("*.tmp\n"),
					}, (testInputs{}).readSet(&manifest.GitRepository{
						URL: url, SparseCheckout: []string{"apps"}, Ignore: new("*.tmp\n"),
					}), sourcecache.Policy{MaxAge: time.Hour})
				}, []string{"apps/keep.yaml"})
			})
		})
	}
}

type acquisitionWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *acquisitionWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func testGenerationWaiter(t *testing.T, disk bool, fetch func(context.Context, *Fetcher, string) (*store.SourceArtifact, error), want []string) {
	t.Helper()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/keep.yaml": "keep", "apps/drop.tmp": "drop", "other/keep.yaml": "other",
		".hidden/secret.yaml": "hidden", ".sourceignore": "other/\n",
	})
	repository, err := git.PlainOpen(src)
	assert.NoError(t, err)
	head, err := repository.Head()
	assert.NoError(t, err)
	assert.NoError(t, repository.Storer.SetReference(plumbing.NewHashReference(head.Name(), submoduleCommit(t, repository, head.Hash()))))
	revision := mustHead(t, src)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var advertisements atomic.Int32
	handler := gitHTTPHandler(t, src)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && advertisements.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(unblock)
	root := ""
	if disk {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
	type result struct {
		artifact *store.SourceArtifact
		err      error
	}
	results := make(chan result, 2)
	go func() {
		artifact, err := fetch(t.Context(), f, srv.URL)
		results <- result{artifact, err}
	}()
	<-started
	waiter := &acquisitionWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	go func() {
		artifact, err := fetch(waiter, f, srv.URL)
		results <- result{artifact, err}
	}()
	<-waiter.waiting
	assert.Equal(t, advertisements.Load(), 1)
	unblock()
	first, second := <-results, <-results
	assert.NoError(t, first.err)
	assert.NoError(t, second.err)
	if first.artifact.FS == second.artifact.FS {
		t.Fatal("waiter reused the producer's view")
	}
	check := func(artifact *store.SourceArtifact) {
		assert.Equal(t, artifact.Revision, revision)
		assert.Equal(t, artifact.Key, first.artifact.Key)
		if artifact.Key == "" {
			t.Fatal("missing cache identity")
		}
		if len(artifact.Warnings) != 1 || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule || artifact.Warnings[0].Detail[0] != "module" {
			t.Fatalf("generation warning lost: %+v", artifact.Warnings)
		}
		var files []string
		assert.NoError(t, fs.WalkDir(artifact.FS, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				files = append(files, name)
			}
			return nil
		}))
		assert.Diff(t, files, want)
		assertWorktree(t, artifact.FS, "apps/keep.yaml", "keep")
	}
	check(first.artifact)
	check(second.artifact)
	before := advertisements.Load()
	srv.Close()
	if disk {
		assert.NoError(t, f.Cache.Close())
		f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
	}
	hit, err := fetch(t.Context(), f, srv.URL)
	assert.NoError(t, err)
	assert.Equal(t, advertisements.Load(), before)
	check(hit)
}

func TestGitWarnings_ImmutableCoordinates(t *testing.T) {
	t.Parallel()
	raw := manifest.Warning{Category: manifest.WarnSkippedSubmodule, Message: "skipping submodule", Detail: []string{"module", "commit"}}
	input := &gittree.FS{Warnings: []manifest.Warning{raw}}
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
	warnings := gitWarnings(input, id, "https://credential:password@example.test/repository.git?token=private#private", "main")
	assert.Equal(t, warnings[0].Resource, id)
	assert.Diff(t, warnings[0].Detail, []string{"module", "commit", "https://example.test/repository.git", "main"})
	assert.Diff(t, input.Warnings, []manifest.Warning{raw})
}
