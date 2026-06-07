package kustomize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestIsGitRemoteBase(t *testing.T) {
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
		{name: "subpath escapes repo", url: "https://github.com/o/r//../escape?ref=v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
func withFakeGitBase(c *TreeCache, worktree string, calls *atomic.Int32) {
	c.SetGitBaseFetcher(func(_ context.Context, _, _ string) (string, string, error) {
		if calls != nil {
			calls.Add(1)
		}
		return worktree, "deadbeef", nil
	})
}

func TestPreflightGitBase_RewritesToDirectory(t *testing.T) {
	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml":               "resources:\n  - ./cm.yaml\n",
		"cm.yaml":                          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
		"overlays/prod/kustomization.yaml": "resources:\n  - ../../\n",
	})

	const gitURL = "https://github.com/o/r//overlays/prod?ref=v1"
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + gitURL + "\n"})

	cache := newPreflightCache(t)
	withFakeGitBase(cache, worktree, nil)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	body, err := fsys.ReadFile("kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	wantDir := remoteResourcePrefix + urlHash("https://github.com/o/r"+"@"+"v1")
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

	cache := newPreflightCache(t)
	withFakeGitBase(cache, worktree, nil)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	if n := hits.Load(); n != 0 {
		t.Errorf("git URL was HTTP-GETted %d time(s); it must be cloned instead (#616)", n)
	}
	wantDir := remoteResourcePrefix + urlHash(srv.URL+"/o/r"+"@"+"v1")
	if !fsys.IsDir(wantDir) {
		t.Fatalf("git URL not materialized as a base directory (#616)")
	}
}

// TestPreflightGitBase_WholeRepoPreservesParentRefs renders end-to-end and
// proves the whole repo is materialized: a subPath kustomization's in-repo
// ../ reference still resolves (copy-only-subPath would break this).
func TestPreflightGitBase_WholeRepoPreservesParentRefs(t *testing.T) {
	worktree := fakeWorktree(t, map[string]string{
		"apps/foo/kustomization.yaml": "resources:\n  - ../../shared\n",
		"shared/kustomization.yaml":   "resources:\n  - ./cm.yaml\n",
		"shared/cm.yaml":              "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: from-parent-ref\n",
	})

	src := writeTree(t, map[string]string{
		"kustomization.yaml": "resources:\n  - https://example.com/o/r//apps/foo?ref=v1\n",
	})

	cache := NewTreeCache()
	withFakeGitBase(cache, worktree, nil)

	out, err := RenderFlux(context.Background(), cache, src, false, ".", map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	})
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
	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml": "resources:\n  - ./cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
	})

	original := "resources:\n  - https://example.com/o/r?ref=v1\n"
	src := writeTree(t, map[string]string{"kustomization.yaml": original})

	cache := NewTreeCache()
	withFakeGitBase(cache, worktree, nil)

	if _, err := RenderFlux(context.Background(), cache, src, false, ".", map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	}); err != nil {
		t.Fatalf("RenderFlux: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(src, "kustomization.yaml")) //nolint:gosec // src is t.TempDir
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
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - https://github.com/o/r?ref=v1\n"})
	err := preflightRemoteResources(context.Background(), newPreflightCache(t), fsys, ".")
	if err == nil {
		t.Fatal("expected error when no git fetcher is wired")
	}
	if !strings.Contains(err.Error(), "no git fetcher is wired") {
		t.Errorf("error should explain the missing fetcher; got %q", err)
	}
}

// TestPreflightGitBase_EachKSGetsOwnCopy: multiple kustomizations referencing
// the same base each get their own self-contained .flate-remote-<hash>/ dir.
func TestPreflightGitBase_EachKSGetsOwnCopy(t *testing.T) {
	worktree := fakeWorktree(t, map[string]string{
		"kustomization.yaml": "resources:\n  - ./cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\n",
	})

	fsys := memFSWith(t, map[string]string{
		"a/kustomization.yaml": "resources:\n  - https://example.com/o/r?ref=v1\n",
		"b/kustomization.yaml": "resources:\n  - https://example.com/o/r?ref=v1\n",
		"c/kustomization.yaml": "resources:\n  - https://example.com/o/r?ref=v1\n",
	})

	var calls atomic.Int32
	cache := newPreflightCache(t)
	withFakeGitBase(cache, worktree, &calls)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	wantDir := remoteResourcePrefix + urlHash("https://example.com/o/r@v1")
	for _, sub := range []string{"a", "b", "c"} {
		if !fsys.Exists(filepath.Join(sub, wantDir, "cm.yaml")) {
			t.Errorf("%s: base not copied into its own dir", sub)
		}
	}
}
