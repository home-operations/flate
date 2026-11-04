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
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/assert"

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
	commit := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.invalid"}},
	)
	input, _, err := gittree.New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		gittree.ViewOptions{Mode: gittree.RemoteBase},
	)
	assert.NoError(t, err)
	c.SetGitBaseFetcher(
		func(_ context.Context, _ GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			if calls != nil {
				calls.Add(1)
			}
			return &store.SourceArtifact{
				FS:       input,
				Key:      "fake-base",
				Content:  "fake-base-generation",
				Revision: "deadbeef",
			}, nil
		},
	)
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
	{
		err := preflightRemoteResources(context.Background(), cache, fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}

	body, err := fsys.ReadFile("kustomization.yaml")
	assert.NoError(t, err)
	wantDir := remoteResourcePrefix + manifest.Fingerprint(
		remoteInput{
			Operation:  remoteGit,
			URL:        "https://github.com/o/r",
			Ref:        "v1",
			Timeout:    27 * time.Second,
			Submodules: true,
		},
	)
	wantEntry := "./" + wantDir + "/overlays/prod"
	assert.ErrorIf(
		t,
		!strings.Contains(string(body), wantEntry),
		"entry not rewritten to base subdir %q:\n%s",
		wantEntry,
		body,
	)
	assert.ErrorIf(t, strings.Contains(string(body), gitURL), "git URL still present after preflight:\n%s", body)
	// The whole repo is materialized as a DIRECTORY, not a single .yaml file
	// (that was the #616 bug).
	assert.FatalIf(t, !fsys.IsDir(wantDir), "git base must materialize as a directory, not a single file")
	for _, rel := range []string{"kustomization.yaml", "cm.yaml", "overlays/prod/kustomization.yaml"} {
		assert.ErrorIf(t, !fsys.Exists(filepath.Join(wantDir, rel)), "whole repo not copied, missing %s", rel)
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
	{
		err := preflightRemoteResources(context.Background(), cache, fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}

	{
		n := hits.Load()
		assert.ErrorIf(t, n != 1, "resource selection HTTP probes = %d, want 1", n)
	}
	wantDir := remoteResourcePrefix + manifest.Fingerprint(
		remoteInput{
			Operation:  remoteGit,
			URL:        srv.URL + "/o/r",
			Ref:        "v1",
			Timeout:    27 * time.Second,
			Submodules: true,
		},
	)
	assert.FatalIf(t, !fsys.IsDir(wantDir), "git URL not materialized as a base directory (#616)")
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

	out, _, err := testutil.RenderMaps(
		Render(
			context.Background(),
			cache,
			RenderRequest{
				Source:      localSource(t, src),
				Label:       src,
				ApplyIgnore: false,
				SubPath:     ".",
				Spec: parsedKS(t, map[string]any{
					"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
					"kind":       "Kustomization",
					"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
					"spec":       map[string]any{"path": "."},
				}),
			},
			sourcecache.RetryConfig{},
		),
	)
	assert.FatalIf(t, err != nil, "Render: %v", err)
	assert.FatalIf(
		t,
		!hasRenderField(out, "name", "from-parent-ref"),
		"in-repo ../ reference inside the base did not resolve:\n%s",
		out,
	)
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

	{
		_, _, err := testutil.RenderMaps(
			Render(
				context.Background(),
				cache,
				RenderRequest{
					Source:      localSource(t, src),
					Label:       src,
					ApplyIgnore: false,
					SubPath:     ".",
					Spec: parsedKS(t, map[string]any{
						"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
						"kind":       "Kustomization",
						"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
						"spec":       map[string]any{"path": "."},
					}),
				},
				sourcecache.RetryConfig{},
			),
		)
		assert.FatalIf(t, err != nil, "Render: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(src, "kustomization.yaml"))
	assert.NoError(t, err)
	assert.ErrorIf(t, string(got) != original, "source kustomization mutated:\nwant %q\ngot  %q", original, got)
	{
		matches, _ := filepath.Glob(filepath.Join(src, remoteResourcePrefix+"*"))
		assert.ErrorIf(t, len(matches) != 0, "git base materialized into source tree: %v", matches)
	}
}

// TestPreflightGitBase_NilFetcherErrors: with no GitBaseFetcher wired, a git
// base surfaces a clear error rather than silently HTTP-GETting the URL.
func TestPreflightGitBase_NilFetcherErrors(t *testing.T) {
	t.Parallel()
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - https://github.com/o/r?ref=v1\n"})
	err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
	assert.FatalIf(t, err == nil, "%s", "expected error when no git fetcher is wired")
	assert.ErrorIf(
		t,
		!strings.Contains(err.Error(), "no git fetcher is wired"),
		"error should explain the missing fetcher; got %q",
		err,
	)
}

func TestPreflightGitBase_ReusesMountWithinRender(t *testing.T) {
	t.Parallel()
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
	{
		err := preflightRemoteResources(context.Background(), cache, fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}

	wantDir := remoteResourcePrefix + manifest.Fingerprint(
		remoteInput{
			Operation:  remoteGit,
			URL:        "https://example.com/o/r",
			Ref:        "v1",
			Timeout:    27 * time.Second,
			Submodules: true,
		},
	)
	if calls.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", calls.Load())
	}
	if hits.Load() != 2 {
		t.Fatalf("selected package URL fetches = %d, want 2", hits.Load())
	}
	assert.FatalIf(t, !fsys.Exists(filepath.Join("a", wantDir, "cm.yaml")), "%s", "base mount is missing")
	for _, sub := range []string{"b", "c"} {
		body, err := fsys.ReadFile(filepath.Join(sub, "kustomization.yaml"))
		assert.FatalIf(
			t,
			err != nil || !strings.Contains(string(body), "../a/"+wantDir),
			"%s did not reuse base mount: %s, %v",
			sub,
			body,
			err,
		)
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
		{
			err := os.Symlink(target, filepath.Join(worktree, name))
			assert.NoError(t, err)
		}
	}

	fsys := memFSWith(
		t,
		map[string]string{"kustomization.yaml": "resources:\n  - git::https://example.com/o/r?ref=v1\n"},
	)
	cache := newTestTreeCache(t, "")
	withFakeGitBase(t, cache, worktree, nil)
	{
		err := preflightRemoteResources(context.Background(), cache, fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}

	wantDir := remoteResourcePrefix + manifest.Fingerprint(
		remoteInput{
			Operation:  remoteGit,
			URL:        "https://example.com/o/r",
			Ref:        "v1",
			Timeout:    27 * time.Second,
			Submodules: true,
		},
	)
	{
		data, err := fsys.ReadFile(
			filepath.Join(wantDir, "alias.yaml"),
		)
		assert.FatalIf(t, err != nil ||
			!strings.Contains(string(data), "name: base"), "regular alias: %s, %v", data, err)
	}
	for _, name := range []string{".github", "node_modules", "missing.yaml", "dirlink"} {
		assert.ErrorIf(t, fsys.Exists(filepath.Join(wantDir, name)), "hidden entry %s", name)
	}
	assert.ErrorIf(
		t,
		!fsys.Exists(filepath.Join(wantDir, "cm.yaml")),
		"%s",
		"legit base resource should still be materialized",
	)
	assert.FatalIf(
		t,
		fsys.Exists(filepath.Join(wantDir, "creds.yaml")),
		"%s",
		"symlink escaping the base root must NOT be materialized into the render fs (#741)",
	)
}

func TestRender_NestedGitBasesAndIntervals(t *testing.T) {
	t.Parallel()
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
			testRenderNestedGitBasesAndIntervalsCase(
				t,
				&fixture,
			)
		},
		)
	}
}

func testRenderNestedGitBasesAndIntervalsCase(
	t *testing.T,
	testCaseInput *struct {
		name          string
		kustomization string
		resource      string
		marker        string
	},
) {
	t.Helper()
	const parentURL = "file:///example.invalid/o/parent.git"
	const childURL = "file:///example.invalid/o/child.git"

	parent := fakeWorktree(t, map[string]string{
		"kustomization.yaml":         testCaseInput.kustomization,
		"package/kustomization.yaml": "resources:\n  - " + childURL + "?timeout=9s&submodules=true\n",
		"root.yaml":                  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root}\n",
	})
	child := fakeWorktree(t, map[string]string{
		"kustomization.yaml": "resources:\n  - cm.yaml\n", "cm.yaml": testCaseInput.resource,
	})
	for _, dir := range []string{parent, child} {
		r := testutil.GitInit(t, dir)
		testutil.GitCommitAll(
			t,
			r,
			"seed",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}},
		)
	}
	sources := cachetest.New(t, "", sourcecache.New)
	fetcher := &sourcegit.Fetcher{Cache: sources}
	c := newTestTreeCache(t, "")
	var requests []GitBaseRequest
	c.SetGitBaseFetcher(
		func(ctx context.Context, request GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			wantTimeout, wantSubmodules := 5*time.Second, false
			if request.URL == childURL {
				wantTimeout, wantSubmodules = 9*time.Second, true
			}
			assert.ErrorIf(
				t,
				request.Timeout != wantTimeout || request.Submodules != wantSubmodules,
				"nested policy=%+v",
				request,
			)
			requests = append(requests, request)
			dir := parent
			if request.URL == childURL {
				dir = child
			}
			return fetcher.FetchRemoteBase(
				ctx,
				&sourcegit.RemoteBaseRequest{
					URL:        "file://" + dir,
					Ref:        request.Ref,
					Timeout:    request.Timeout,
					Submodules: request.Submodules,
				},
				&sourcecache.Policy{MaxAge: request.MaxAge, Retry: retry},
			)
		},
	)
	input := &store.SourceArtifact{
		Key: "root",
		FS: fstest.MapFS{
			"kustomization.yaml": &fstest.MapFile{
				Data: []byte("resources:\n  - " + parentURL + "?timeout=5s&submodules=false\n"),
			},
		},
	}
	render := func(interval string) ([]map[string]any, error) {
		duration, err := time.ParseDuration(interval)
		if err != nil {
			return nil, err
		}
		docs, _, err := testutil.RenderMaps(
			Render(
				t.Context(),
				c,
				RenderRequest{
					Source: input,
					Spec:   &manifest.Kustomization{Interval: metav1.Duration{Duration: duration}},
				},
				sourcecache.RetryConfig{},
			),
		)
		return docs, err
	}
	update := func(name, body string) {
		t.Helper()
		testutil.WriteFileAt(t, filepath.Join(child, name), body)
		r, err := git.PlainOpen(child)
		assert.NoError(t, err)
		testutil.GitCommitAll(
			t,
			r,
			name,
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}},
		)
	}
	{
		output, err := render(
			"1h",
		)
		assert.FatalIf(t, err != nil ||
			!hasRenderField(output, strings.TrimSuffix(testCaseInput.marker, ": "), "first") &&
				(testCaseInput.name != "generators" || !strings.HasPrefix(output[0]["metadata"].(map[string]any)["name"].(string), "first-")), "nested base: %s, %v", output, err)
	}
	update("cm.yaml", strings.ReplaceAll(testCaseInput.resource, "first", "second"))
	for _, tc := range []struct {
		interval, want string
		maxAge         time.Duration
	}{
		{"1h", "first", time.Hour}, {"1ns", "second", time.Nanosecond},
	} {
		requests = nil
		output, err := render(tc.interval)
		assert.FatalIf(t, err != nil ||
			!hasRenderField(output, strings.TrimSuffix(testCaseInput.marker, ": "), tc.want) &&
				(testCaseInput.name != "generators" || !strings.HasPrefix(output[0]["metadata"].(map[string]any)["name"].(string), tc.want+"-")), "interval %s: %s, %v", tc.interval, output, err)
		wantRequests := 2
		if tc.maxAge == time.Nanosecond {
			wantRequests = 4
		}
		assert.FatalIf(t, len(requests) != wantRequests, "nested requests: %v", requests)
		for _, request := range requests {
			assert.FatalIf(t, request.MaxAge != tc.maxAge, "nested interval = %v, want %v", request.MaxAge, tc.maxAge)
		}
	}
	update("kustomization.yaml", "resources:\n  - "+parentURL+"?timeout=5s&submodules=false\n")
	{
		_, err := render("1ns")
		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), "cycle"),
			"upstream cycle error: %v",
			err,
		)
	}
}
