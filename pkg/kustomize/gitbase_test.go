package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// fakeWorktree writes files into a fresh dir and returns its path,
// standing in for a materialized git worktree.
func fakeWorktree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range files {
		testutil.WriteFileAt(t, filepath.Join(dir, rel), content)
	}
	return dir
}

// withFakeGitBase installs a GitBaseFetcher returning worktree for every
// call and counting invocations.
func withFakeGitBase(t *testing.T, c *TreeCache, worktree string, calls *atomic.Int32) {
	t.Helper()
	repo := testutil.GitInit(t, worktree)
	commit := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.invalid"}})
	input, _, err := gittree.New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, gittree.ViewOptions{Mode: gittree.RemoteBase})
	if err != nil {
		t.Fatal(err)
	}
	c.SetGitBaseFetcher(func(_ context.Context, _ GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
		if calls != nil {
			calls.Add(1)
		}
		return &store.SourceArtifact{FS: input, Key: "fake-base", Content: "fake-base-generation", Revision: "deadbeef"}, nil
	})
}

func TestPreflightGitBase_RewritesToDirectory(t *testing.T) {
	t.Parallel()
	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml":               "resources:\n  - ./cm.yaml\n",
		"cm.yaml":                          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
		"overlays/prod/kustomization.yaml": "resources:\n  - ../../\n",
	})

	const gitURL = "git::https://github.com/o/r//overlays/prod?ref=v1"
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + gitURL + "\n"})

	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	body, err := fsys.ReadFile("kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	wantDir := remoteResourcePrefix + manifest.Fingerprint(remoteInput{Operation: remoteGit, URL: "https://github.com/o/r", Ref: "v1", Timeout: 27 * time.Second, Submodules: true})
	wantEntry := "./" + wantDir + "/overlays/prod"
	if !strings.Contains(string(body), wantEntry) {
		t.Errorf("entry not rewritten to base subdir %q:\n%s", wantEntry, body)
	}
	if strings.Contains(string(body), gitURL) {
		t.Errorf("git URL still present after preflight:\n%s", body)
	}
	// The whole repo is materialized as a DIRECTORY, not a single .yaml file
	// (that was the #616 bug).
	if !fsys.IsDir(wantDir) {
		t.Fatalf("git base must materialize as a directory, not a single file")
	}
	for _, rel := range []string{"kustomization.yaml", "cm.yaml", "overlays/prod/kustomization.yaml"} {
		if !fsys.Exists(filepath.Join(wantDir, rel)) {
			t.Errorf("whole repo not copied, missing %s", rel)
		}
	}
}

// TestPreflightGitBase_NonResourceBody is the #616 regression guard:
// a git-base URL must take the clone path, never an HTTP GET that returns
// the host's HTML page and gets written as malformed YAML.
func TestPreflightGitBase_NonResourceBody(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("<!DOCTYPE html><html><body>not yaml</body></html>"))
	}))
	t.Cleanup(srv.Close)

	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml": "resources:\n  - ./cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
	})

	// A git URL (carries ?ref=) pointing at the HTML server.
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/o/r?ref=v1\n"})

	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	if n := hits.Load(); n != 1 {
		t.Errorf("resource selection HTTP probes = %d, want 1", n)
	}
	wantDir := remoteResourcePrefix + manifest.Fingerprint(remoteInput{Operation: remoteGit, URL: srv.URL + "/o/r", Ref: "v1", Timeout: 27 * time.Second, Submodules: true})
	if !fsys.IsDir(wantDir) {
		t.Fatalf("git URL not materialized as a base directory (#616)")
	}
}

// TestPreflightGitBase_WholeRepoPreservesParentRefs renders end-to-end and
// proves the whole repo is materialized: a subPath kustomization's in-repo
// ../ reference still resolves (copy-only-subPath would break this).
func TestPreflightGitBase_WholeRepoPreservesParentRefs(t *testing.T) {
	t.Parallel()
	worktree := fakeWorktree(t, map[string]string{
		"apps/foo/kustomization.yaml":        "resources:\n  - ../../shared\n",
		"shared/kustomization.yaml":          "resources:\n  - ./cm.yaml\n",
		"shared/cm.yaml":                     "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: from-parent-ref\n",
		"examples/kustomization.yaml":        "resources:\n  - http://127.0.0.1:1/unreachable.yaml\n",
		"apps/foo/unused/kustomization.yaml": "resources:\n  - http://127.0.0.1:1/unreachable.yaml\n",
	})

	src := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": "resources:\n  - git::https://example.com/o/r//apps/foo?ref=v1\n",
	})

	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)

	out, _, err := Render(context.Background(), cache, RenderRequest{Source: localSource(t, src), Label: src, ApplyIgnore: false, SubPath: ".", Spec: parsedKS(t, map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	})}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !hasRenderField(out, "name", "from-parent-ref") {
		t.Fatalf("in-repo ../ reference inside the base did not resolve:\n%s", out)
	}
}

// TestPreflightGitBase_SourceTreeImmutable confirms a git base referenced from
// a SOURCE kustomization never mutates the on-disk source tree (the in-memory
// render writes everything to a private fs).
func TestPreflightGitBase_SourceTreeImmutable(t *testing.T) {
	t.Parallel()
	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml": "resources:\n  - ./cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
	})

	original := "resources:\n  - git::https://example.com/o/r?ref=v1\n"
	src := testutil.WriteTree(t, map[string]string{"kustomization.yaml": original})

	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)

	if _, _, err := Render(context.Background(), cache, RenderRequest{Source: localSource(t, src), Label: src, ApplyIgnore: false, SubPath: ".", Spec: parsedKS(t, map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	})}, sourcecache.RetryConfig{}); err != nil {
		t.Fatalf("Render: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(src, "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != original {
		t.Errorf("source kustomization mutated:\nwant %q\ngot  %q", original, got)
	}
	if matches, _ := filepath.Glob(filepath.Join(src, remoteResourcePrefix+"*")); len(matches) != 0 {
		t.Errorf("git base materialized into source tree: %v", matches)
	}
}

// TestPreflightGitBase_NilFetcherErrors: with no GitBaseFetcher wired, a git
// base surfaces a clear error rather than silently HTTP-GETting the URL.
func TestPreflightGitBase_NilFetcherErrors(t *testing.T) {
	t.Parallel()
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - https://github.com/o/r?ref=v1\n"})
	err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
	if err == nil {
		t.Fatal("expected error when no git fetcher is wired")
	}
	if !strings.Contains(err.Error(), "no git fetcher is wired") {
		t.Errorf("error should explain the missing fetcher; got %q", err)
	}
}

func TestPreflightGitBase_ReusesMountWithinRender(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)
	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml":     "resources:\n  - ./cm.yaml\n",
		"cm.yaml":                "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
		"one/kustomization.yaml": "resources:\n  - " + srv.URL + "/one.yaml\n",
		"two/kustomization.yaml": "resources:\n  - " + srv.URL + "/two.yaml\n",
	})

	fsys := memFSWith(t, map[string]string{
		"kustomization.yaml":   "resources: [a, b, c]\n",
		"a/kustomization.yaml": "resources:\n  - git::https://example.com/o/r//one?ref=v1\n",
		"b/kustomization.yaml": "resources:\n  - git::https://example.com/o/r//two?ref=v1\n",
		"c/kustomization.yaml": "resources:\n  - git::https://example.com/o/r//one?ref=v1\n",
	})

	var calls atomic.Int32
	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, &calls)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	wantDir := remoteResourcePrefix + manifest.Fingerprint(remoteInput{Operation: remoteGit, URL: "https://example.com/o/r", Ref: "v1", Timeout: 27 * time.Second, Submodules: true})
	if calls.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", calls.Load())
	}
	if hits.Load() != 2 {
		t.Fatalf("selected package URL fetches = %d, want 2", hits.Load())
	}
	if !fsys.Exists(filepath.Join("a", wantDir, "cm.yaml")) {
		t.Fatal("base mount is missing")
	}
	for _, sub := range []string{"b", "c"} {
		body, err := fsys.ReadFile(filepath.Join(sub, "kustomization.yaml"))
		if err != nil || !strings.Contains(string(body), "../a/"+wantDir) {
			t.Fatalf("%s did not reuse base mount: %s, %v", sub, body, err)
		}
	}
}

// TestPreflightGitBase_SymlinkEscapeNotMaterialized is the #741 end-to-end
// regression: a fork-PR-controlled remote base whose worktree contains a symlink
// pointing at a host file (here a valid ConfigMap so it WOULD render unfixed)
// must not have that host content copied into the render fs. Legit base files
// are still materialized; only the escaping symlink is dropped.
func TestPreflightGitBase_SymlinkEscapeNotMaterialized(t *testing.T) {
	t.Parallel()
	// A host file OUTSIDE the base root, shaped as a valid manifest so an
	// unfixed copy would surface it straight into the rendered diff.
	outside := t.TempDir()
	const sentinel = "host-secret-exfil"
	hostFile := filepath.Join(outside, "host.yaml")
	testutil.WriteFileAt(t, hostFile, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: "+sentinel+"\n")

	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml":  "resources:\n  - ./cm.yaml\n  - ./creds.yaml\n",
		".github/x.yaml":      "hidden",
		"node_modules/y.yaml": "hidden",
		"cm.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
	})
	// The attacker's symlink: a referenced resource whose target escapes the base.
	if err := os.Symlink(hostFile, filepath.Join(worktree, "creds.yaml")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}

	for name, target := range map[string]string{
		"alias.yaml": "cm.yaml", "missing.yaml": "absent.yaml", "dirlink": "node_modules",
		".github/a": "b", ".github/b": "a", "node_modules/a": "b", "node_modules/b": "a",
	} {
		if err := os.Symlink(target, filepath.Join(worktree, name)); err != nil {
			t.Fatal(err)
		}
	}

	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - git::https://example.com/o/r?ref=v1\n"})
	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	wantDir := remoteResourcePrefix + manifest.Fingerprint(remoteInput{Operation: remoteGit, URL: "https://example.com/o/r", Ref: "v1", Timeout: 27 * time.Second, Submodules: true})
	if data, err := fsys.ReadFile(filepath.Join(wantDir, "alias.yaml")); err != nil || !strings.Contains(string(data), "name: base") {
		t.Fatalf("regular alias: %s, %v", data, err)
	}
	for _, name := range []string{".github", "node_modules", "missing.yaml", "dirlink"} {
		if fsys.Exists(filepath.Join(wantDir, name)) {
			t.Errorf("hidden entry %s", name)
		}
	}
	if !fsys.Exists(filepath.Join(wantDir, "cm.yaml")) {
		t.Error("legit base resource should still be materialized")
	}
	if fsys.Exists(filepath.Join(wantDir, "creds.yaml")) {
		t.Fatal("symlink escaping the base root must NOT be materialized into the render fs (#741)")
	}
}

func TestRender_NestedGitBasesAndIntervals(t *testing.T) {
	t.Parallel()
	const parentURL = "file:///example.invalid/o/parent.git"
	const childURL = "file:///example.invalid/o/child.git"
	for _, fixture := range []struct {
		name, kustomization, resource, marker string
	}{
		{
			name:          "resources",
			kustomization: "resources:\n  - package\n",
			resource:      "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n",
			marker:        "name: ",
		},
		{
			name:          "transformers",
			kustomization: "resources:\n  - root.yaml\ntransformers:\n  - package\n",
			resource: "apiVersion: builtin\nkind: LabelTransformer\nmetadata: {name: labels}\n" +
				"labels: {nested: first}\nfieldSpecs:\n  - path: metadata/labels\n    create: true\n",
			marker: "nested: ",
		},
		{
			name:          "generators",
			kustomization: "generators:\n  - package\n",
			resource:      "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: first}\nliterals: [nested=gitbase]\n",
			marker:        "name: ",
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			parent := fakeWorktree(t, map[string]string{
				"kustomization.yaml":         fixture.kustomization,
				"package/kustomization.yaml": "resources:\n  - " + childURL + "?timeout=9s&submodules=true\n",
				"root.yaml":                  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root}\n",
			})
			child := fakeWorktree(t, map[string]string{
				"kustomization.yaml": "resources:\n  - cm.yaml\n", "cm.yaml": fixture.resource,
			})
			for _, dir := range []string{parent, child} {
				r := testutil.GitInit(t, dir)
				testutil.GitCommitAll(t, r, "seed", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}})
			}
			sources := cachetest.New(t, "", sourcecache.New)
			fetcher := &sourcegit.Fetcher{Cache: sources}
			c := newTestTreeCache(t, "")
			var requests []GitBaseRequest
			c.SetGitBaseFetcher(func(ctx context.Context, request GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				wantTimeout, wantSubmodules := 5*time.Second, false
				if request.URL == childURL {
					wantTimeout, wantSubmodules = 9*time.Second, true
				}
				if request.Timeout != wantTimeout || request.Submodules != wantSubmodules {
					t.Errorf("nested policy=%+v", request)
				}
				requests = append(requests, request)
				dir := parent
				if request.URL == childURL {
					dir = child
				}
				return fetcher.FetchRemoteBase(ctx, sourcegit.RemoteBaseRequest{URL: "file://" + dir, Ref: request.Ref, Timeout: request.Timeout, Submodules: request.Submodules}, sourcecache.Policy{MaxAge: request.MaxAge, Retry: retry})
			})
			input := &store.SourceArtifact{Key: "root", FS: fstest.MapFS{"kustomization.yaml": &fstest.MapFile{Data: []byte("resources:\n  - " + parentURL + "?timeout=5s&submodules=false\n")}}}
			render := func(interval string) ([]map[string]any, error) {
				duration, err := time.ParseDuration(interval)
				if err != nil {
					return nil, err
				}
				docs, _, err := Render(t.Context(), c, RenderRequest{Source: input, Spec: &manifest.Kustomization{Interval: metav1.Duration{Duration: duration}}}, sourcecache.RetryConfig{})
				return docs, err
			}
			update := func(name, body string) {
				t.Helper()
				testutil.WriteFileAt(t, filepath.Join(child, name), body)
				r, err := git.PlainOpen(child)
				if err != nil {
					t.Fatal(err)
				}
				testutil.GitCommitAll(t, r, name, &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}})
			}
			if output, err := render("1h"); err != nil || !hasRenderField(output, strings.TrimSuffix(fixture.marker, ": "), "first") && (fixture.name != "generators" || !strings.HasPrefix(output[0]["metadata"].(map[string]any)["name"].(string), "first-")) {
				t.Fatalf("nested base: %s, %v", output, err)
			}
			update("cm.yaml", strings.ReplaceAll(fixture.resource, "first", "second"))
			for _, tc := range []struct {
				interval, want string
				maxAge         time.Duration
			}{
				{"1h", "first", time.Hour}, {"1ns", "second", time.Nanosecond},
			} {
				requests = nil
				output, err := render(tc.interval)
				if err != nil || !hasRenderField(output, strings.TrimSuffix(fixture.marker, ": "), tc.want) && (fixture.name != "generators" || !strings.HasPrefix(output[0]["metadata"].(map[string]any)["name"].(string), tc.want+"-")) {
					t.Fatalf("interval %s: %s, %v", tc.interval, output, err)
				}
				wantRequests := 2
				if tc.maxAge == time.Nanosecond {
					wantRequests = 4
				}
				if len(requests) != wantRequests {
					t.Fatalf("nested requests: %v", requests)
				}
				for _, request := range requests {
					if request.MaxAge != tc.maxAge {
						t.Fatalf("nested interval = %v, want %v", request.MaxAge, tc.maxAge)
					}
				}
			}
			update("kustomization.yaml", "resources:\n  - "+parentURL+"?timeout=5s&submodules=false\n")
			if _, err := render("1ns"); err == nil || !strings.Contains(err.Error(), "cycle") {
				t.Fatalf("upstream cycle error: %v", err)
			}
		})
	}
}

func TestRender_RemoteRetryPolicies(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse-%t", concurrency, reverse), func(t *testing.T) {
				var httpCalls [4]atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					attempts := 1
					if strings.HasSuffix(r.URL.Path, "/3") {
						attempts = 3
					}
					n := httpCalls[attempts].Add(1)
					if attempts == 1 || n == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					_, _ = io.WriteString(w, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: retried}\n")
				}))
				defer srv.Close()
				c := newTestTreeCache(t, "")
				entered := make(chan int, concurrency)
				release := make(chan struct{})
				released := false
				defer func() {
					if !released {
						close(release)
					}
				}()
				c.SetGitBaseFetcher(func(ctx context.Context, request GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					if request.MaxAge != time.Hour {
						t.Errorf("remote interval=%v", request.MaxAge)
					}
					select {
					case <-release:
					default:
						entered <- retry.Attempts
					}
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					body := fmt.Sprintf("resources:\n  - %s/%d\n", srv.URL, retry.Attempts)
					return &store.SourceArtifact{Key: "family", Content: fmt.Sprint(retry.Attempts), Revision: fmt.Sprint(retry.Attempts), FS: fstest.MapFS{"kustomization.yaml": {Data: []byte(body)}}}, nil
				})
				type result struct {
					attempts int
					body     []map[string]any
					err      error
				}
				done := make(chan result, concurrency)
				launch := func(i int) {
					go func() {
						attempts := 1
						if i%2 != 0 {
							attempts = 3
						}
						retry := sourcecache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
						input := &store.SourceArtifact{Key: "root", FS: fstest.MapFS{"kustomization.yaml": {Data: []byte("resources:\n  - https://example.invalid/owner/repo?ref=main\n")}}}
						body, _, err := Render(t.Context(), c, RenderRequest{Source: input, Spec: &manifest.Kustomization{Interval: metav1.Duration{Duration: time.Hour}}}, retry)
						done <- result{attempts, body, err}
					}()
				}
				first := 0
				if reverse {
					first = concurrency - 1
				}
				launch(first)
				select {
				case <-entered:
				case early := <-done:
					t.Fatalf("render before Git callback: %v", early.err)
				}
				for i := range concurrency {
					if i != first {
						launch(i)
					}
				}
				close(release)
				released = true
				for range concurrency {
					got := <-done
					if got.attempts == 1 {
						terminal, ok := errors.AsType[*sourcecache.TerminalError](got.err)
						if !ok || !terminal.Attempted {
							t.Fatalf("one-attempt result=%v", got.err)
						}
					} else if got.err != nil || !hasRenderField(got.body, "name", "retried") {
						t.Fatalf("three-attempt result=%s, %v", got.body, got.err)
					}
				}
				if httpCalls[3].Load() != 2 || httpCalls[1].Load() < 1 || httpCalls[1].Load() > int32(concurrency/2) {
					t.Fatalf("HTTP attempts=%d/%d", httpCalls[1].Load(), httpCalls[3].Load())
				}
			})
		}
	}
}

func TestPreflightGitBase_UpstreamSelection(t *testing.T) {
	for _, tc := range []struct {
		name, role, raw, body string
		status, http, git     int
	}{
		{"markerless repository", "resources", "https://github.com/org/repo/base", "", http.StatusNotFound, 1, 1},
		{"resource before directory", "resources", "https://github.com/org/repo//base?ref=v1", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: file}\n", http.StatusOK, 1, 0},
		{"component directory", "components", "https://github.com/org/repo/base", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: file}\n", http.StatusOK, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var httpCalls, gitCalls atomic.Int32
			stubHTTPTransport(t, remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
				httpCalls.Add(1)
				return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
			}))
			fsys := memFSWith(t, map[string]string{"kustomization.yaml": tc.role + ":\n  - " + tc.raw + "\n"})
			cache := newTestTreeCache(t, "")
			withFakeGitBase(t, cache, fakeWorktree(t, map[string]string{"base/kustomization.yaml": "resources: []\n"}), &gitCalls)
			err := preflightRemoteResources(t.Context(), cache, fsys, ".")
			if err != nil || int(httpCalls.Load()) != tc.http || int(gitCalls.Load()) != tc.git {
				t.Errorf("preflight: HTTP=%d Git=%d error=%v; want HTTP=%d Git=%d and success", httpCalls.Load(), gitCalls.Load(), err, tc.http, tc.git)
			}
		})
	}
}

func TestPreflightGitBase_PolicyMounts(t *testing.T) {
	t.Parallel()
	c := newTestTreeCache(t, "")
	var requests []GitBaseRequest
	c.SetGitBaseFetcher(func(_ context.Context, r GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
		requests = append(requests, r)
		return &store.SourceArtifact{Key: "same", Content: "same-content", Revision: "same", FS: memoryTree(t, map[string]string{"one/kustomization.yaml": "resources: []\n", "two/kustomization.yaml": "resources: []\n"})}, nil
	})
	view := memFSWith(t, map[string]string{"kustomization.yaml": `resources:
- file:///org/repo//one?timeout=1s&submodules=false
- file:///org/repo//two?timeout=1s&submodules=false
- file:///org/repo//one?timeout=2s&submodules=false
- file:///org/repo//one?timeout=2s&submodules=true
`})
	if err := preflightRemoteResources(t.Context(), c, view, "."); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 3 || len(view.bases) != 3 {
		t.Fatalf("policy acquisitions=%+v mounts=%+v", requests, view.bases)
	}
	for i, want := range []GitBaseRequest{{URL: "file:///org/repo", Timeout: time.Second}, {URL: "file:///org/repo", Timeout: 2 * time.Second}, {URL: "file:///org/repo", Timeout: 2 * time.Second, Submodules: true}} {
		if requests[i] != want {
			t.Fatalf("request %d=%+v, want %+v", i, requests[i], want)
		}
	}
}

func TestPreflightGitBase_ErrorPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path string
		status     int
		timeout    bool
	}{
		{"malformed resource", "/org/repo/base", http.StatusOK, false},
		{"directory timeout", "/org/repo/base", http.StatusOK, true},
		{"HTTP failure", "/resource.yaml", http.StatusNotFound, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, "apiVersion: [")
			}))
			t.Cleanup(srv.Close)
			c := newTestTreeCache(t, "")
			var calls atomic.Int32
			c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				calls.Add(1)
				if tc.timeout {
					return nil, context.DeadlineExceeded
				}
				return nil, errors.New("directory failed")
			})
			input := &store.SourceArtifact{FS: memoryTree(t, map[string]string{"kustomization.yaml": "resources: ['" + srv.URL + tc.path + "']\n"})}
			_, _, err := Render(t.Context(), c, RenderRequest{Source: input, Spec: demoSpec}, sourcecache.RetryConfig{})
			if err == nil {
				t.Fatal("invalid input rendered")
			}
			if tc.status != http.StatusOK {
				if calls.Load() != 0 || !strings.Contains(err.Error(), "HTTP 404") {
					t.Fatalf("HTTP error precedence: calls=%d error=%v", calls.Load(), err)
				}
			} else if tc.timeout {
				if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "accumulation err=") {
					t.Fatalf("directory timeout lost file diagnostic: %v", err)
				}
			} else if calls.Load() != 1 || strings.Contains(err.Error(), "directory failed") || !strings.Contains(err.Error(), "yaml") {
				t.Fatalf("malformed resource precedence: calls=%d error=%v", calls.Load(), err)
			}
		})
	}
}
