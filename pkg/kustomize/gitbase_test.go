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

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestIsGitRemoteBase(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		url     string
		ok      bool
		repoURL string
		subPath string
		ref     string
	}{
		// --- git bases ---
		{name: "ref query", url: "https://github.com/o/r?ref=v0.2.2",
			ok: true, repoURL: "https://github.com/o/r", ref: "v0.2.2"},
		{name: "ref query with default subpath", url: "https://github.com/o/r/sub/dir?ref=main",
			ok: true, repoURL: "https://github.com/o/r", subPath: "sub/dir", ref: "main"},
		{name: "version query", url: "https://github.com/o/r?version=v0.2.2",
			ok: true, repoURL: "https://github.com/o/r", ref: "v0.2.2"},
		{name: "ref beats version", url: "https://github.com/o/r?version=v1&ref=v2",
			ok: true, repoURL: "https://github.com/o/r", ref: "v2"},
		{name: "double-slash subpath", url: "https://github.com/o/r//sub/dir?ref=main",
			ok: true, repoURL: "https://github.com/o/r", subPath: "sub/dir", ref: "main"},
		{name: "double-slash no ref", url: "https://github.com/o/r//sub",
			ok: true, repoURL: "https://github.com/o/r", subPath: "sub"},
		{name: "git suffix with subpath", url: "https://github.com/o/r.git/sub?ref=abc",
			ok: true, repoURL: "https://github.com/o/r.git", subPath: "sub", ref: "abc"},
		{name: "git suffix root", url: "https://github.com/o/r.git?ref=v1",
			ok: true, repoURL: "https://github.com/o/r.git", ref: "v1"},
		{name: "azure _git", url: "https://dev.azure.com/org/proj/_git/repo/sub?ref=v1",
			ok: true, repoURL: "https://dev.azure.com/org/proj/_git/repo", subPath: "sub", ref: "v1"},
		{name: "git:: forced no ref", url: "git::https://x.example/o/r",
			ok: true, repoURL: "https://x.example/o/r"},
		{name: "git:: forced with ref", url: "git::https://x.example/o/r?ref=v1",
			ok: true, repoURL: "https://x.example/o/r", ref: "v1"},
		{name: "self-hosted gitea, no allowlist", url: "https://gitea.internal/o/r?ref=v1",
			ok: true, repoURL: "https://gitea.internal/o/r", ref: "v1"},
		{name: "http scheme", url: "http://h.example/o/r?ref=v1",
			ok: true, repoURL: "http://h.example/o/r", ref: "v1"},

		// --- not git bases ---
		{name: "plain http file", url: "https://raw.githubusercontent.com/o/r/main/cm.yaml"},
		{name: "plain http file short", url: "https://example.com/foo.yaml"},
		{name: "marker-less ref-less repo (documented limit)", url: "https://github.com/o/r"},
		{name: "oci ref", url: "oci://ghcr.io/o/r"},
		{name: "ssh left to kustomize", url: "ssh://git@github.com/o/r?ref=v1"},
		{name: "local relative", url: "./local"},
		{name: "parent relative", url: "../shared"},
		{name: "host only with ref", url: "https://github.com?ref=v1"},
		{name: "single segment with ref", url: "https://github.com/onlyorg?ref=v1"},
		{name: "empty org with ref", url: "https://github.com//repo?ref=v1"},
		{name: "empty repo with ref", url: "https://github.com/org/?ref=v1"},
		{name: "subpath escapes repo", url: "https://github.com/o/r//../escape?ref=v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec, ok := isGitRemoteBase(tc.url)
			if ok != tc.ok {
				t.Fatalf("isGitRemoteBase(%q) ok = %v, want %v (spec %+v)", tc.url, ok, tc.ok, spec)
			}
			if !ok {
				return
			}
			if spec.repoURL != tc.repoURL || spec.subPath != tc.subPath || spec.ref != tc.ref {
				t.Errorf("isGitRemoteBase(%q) = {repoURL:%q subPath:%q ref:%q}, want {repoURL:%q subPath:%q ref:%q}",
					tc.url, spec.repoURL, spec.subPath, spec.ref, tc.repoURL, tc.subPath, tc.ref)
			}
		})
	}
}

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
		return &store.SourceArtifact{FS: input, Key: "fake-base", Revision: "deadbeef"}, nil
	})
}

func TestPreflightGitBase_RewritesToDirectory(t *testing.T) {
	t.Parallel()
	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml":               "resources:\n  - ./cm.yaml\n",
		"cm.yaml":                          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
		"overlays/prod/kustomization.yaml": "resources:\n  - ../../\n",
	})

	const gitURL = "https://github.com/o/r//overlays/prod?ref=v1"
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
	wantDir := remoteResourcePrefix + manifest.Fingerprint("https://github.com/o/r"+"@"+"v1")
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

// TestPreflightGitBase_DoesNotHTTPGetGitURL is the #616 regression guard:
// a git-base URL must take the clone path, never an HTTP GET that returns
// the host's HTML page and gets written as malformed YAML.
func TestPreflightGitBase_DoesNotHTTPGetGitURL(t *testing.T) {
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

	if n := hits.Load(); n != 0 {
		t.Errorf("git URL was HTTP-GETted %d time(s); it must be cloned instead (#616)", n)
	}
	wantDir := remoteResourcePrefix + manifest.Fingerprint(srv.URL+"/o/r"+"@"+"v1")
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
		"kustomization.yaml": "resources:\n  - https://example.com/o/r//apps/foo?ref=v1\n",
	})

	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)

	out, err := RenderFlux(context.Background(), cache, RenderRequest{Source: localSource(t, src), Label: src, ApplyIgnore: false, SubPath: ".", Spec: map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	}}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("RenderFlux: %v", err)
	}
	if !strings.Contains(string(out), "from-parent-ref") {
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

	original := "resources:\n  - https://example.com/o/r?ref=v1\n"
	src := testutil.WriteTree(t, map[string]string{"kustomization.yaml": original})

	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)

	if _, err := RenderFlux(context.Background(), cache, RenderRequest{Source: localSource(t, src), Label: src, ApplyIgnore: false, SubPath: ".", Spec: map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	}}, sourcecache.RetryConfig{}); err != nil {
		t.Fatalf("RenderFlux: %v", err)
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
		"a/kustomization.yaml": "resources:\n  - https://example.com/o/r//one?ref=v1\n",
		"b/kustomization.yaml": "resources:\n  - https://example.com/o/r//two?ref=v1\n",
		"c/kustomization.yaml": "resources:\n  - https://example.com/o/r//one?ref=v1\n",
	})

	var calls atomic.Int32
	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, &calls)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	wantDir := remoteResourcePrefix + manifest.Fingerprint("https://example.com/o/r@v1")
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

	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - https://example.com/o/r?ref=v1\n"})
	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	wantDir := remoteResourcePrefix + manifest.Fingerprint("https://example.com/o/r@v1")
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

func TestRenderFlux_NestedGitBasesAndIntervals(t *testing.T) {
	t.Parallel()
	const parentURL = "https://example.invalid/o/parent.git"
	const childURL = "https://example.invalid/o/child.git"
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
				"package/kustomization.yaml": "resources:\n  - " + childURL + "\n",
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
				requests = append(requests, request)
				dir := parent
				if request.URL == childURL {
					dir = child
				}
				return fetcher.FetchRemoteBase(ctx, sourcegit.RemoteBaseRequest{URL: "file://" + dir, Ref: request.Ref}, sourcecache.Policy{MaxAge: request.MaxAge, Retry: retry})
			})
			input := &store.SourceArtifact{Key: "root", FS: fstest.MapFS{"kustomization.yaml": &fstest.MapFile{Data: []byte("resources:\n  - " + parentURL + "\n")}}}
			render := func(interval string) ([]byte, error) {
				return RenderFlux(t.Context(), c, RenderRequest{Source: input, Spec: map[string]any{"spec": map[string]any{"interval": interval}}}, sourcecache.RetryConfig{})
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
			if output, err := render("1h"); err != nil || !strings.Contains(string(output), fixture.marker+"first") {
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
				if err != nil || !strings.Contains(string(output), fixture.marker+tc.want) {
					t.Fatalf("interval %s: %s, %v", tc.interval, output, err)
				}
				if len(requests) != 2 {
					t.Fatalf("nested requests: %v", requests)
				}
				for _, request := range requests {
					if request.MaxAge != tc.maxAge {
						t.Fatalf("nested interval = %v, want %v", request.MaxAge, tc.maxAge)
					}
				}
			}
			update("kustomization.yaml", "resources:\n  - "+parentURL+"\n")
			if _, err := render("1ns"); err == nil || !strings.Contains(err.Error(), "cycle") {
				t.Fatalf("upstream cycle error: %v", err)
			}
		})
	}
}

func TestRenderFlux_RemoteRetryPolicies(t *testing.T) {
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
					entered <- retry.Attempts
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					body := fmt.Sprintf("resources:\n  - %s/%d\n", srv.URL, retry.Attempts)
					return &store.SourceArtifact{Key: "family", Revision: fmt.Sprint(retry.Attempts), FS: fstest.MapFS{"kustomization.yaml": {Data: []byte(body)}}}, nil
				})
				type result struct {
					attempts int
					body     []byte
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
						body, err := RenderFlux(t.Context(), c, RenderRequest{Source: input, Spec: map[string]any{"spec": map[string]any{"interval": "1h"}}}, retry)
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
				for range concurrency - 1 {
					select {
					case <-entered:
					case early := <-done:
						t.Fatalf("render before Git callback: %v", early.err)
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
					} else if got.err != nil || !strings.Contains(string(got.body), "name: retried") {
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
