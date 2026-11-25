package kustomize

import (
	"cmp"
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/networktest"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	sourcegit "github.com/home-operations/flate/internal/engine/source/git"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
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
		"apps/foo/kustomization.yaml":        "resources:\n  - ../../crds/shared\n",
		"crds/shared/kustomization.yaml":     "resources:\n  - ./cm.yaml\n",
		"crds/shared/cm.yaml":                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: from-parent-ref\n",
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
				Source: localSource(t, src),
				Label:  src,

				SubPath: ".",
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
					Source: localSource(t, src),
					Label:  src,

					SubPath: ".",
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
	for _, name := range []string{".github/x.yaml", "node_modules/y.yaml"} {
		assert.ErrorIf(t, !fsys.Exists(filepath.Join(wantDir, name)), "missing base entry %s", name)
	}
	for _, name := range []string{"missing.yaml", "dirlink"} {
		assert.ErrorIf(t, fsys.Exists(filepath.Join(wantDir, name)), "invalid base entry %s", name)
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
	fetcher := &sourcegit.Fetcher{Network: networktest.New(t), Cache: sources}
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

func TestRender_RemoteLinkPolicies(t *testing.T) {
	t.Parallel()
	const base = "apps/base"
	const child = base + "/child"
	const control = "resources: [cm.yaml]\n"
	const generator = "apiVersion: builtin\nkind: ConfigMapGenerator\n" +
		"metadata: {name: child}\noptions: {disableNameSuffixHash: true}\nliterals: [key=value]\n"
	const transformer = "apiVersion: builtin\nkind: ValueAddTransformer\nmetadata: {name: loader}\n" +
		"targets:\n  - fieldPath: data/loader\n    selector: {kind: ConfigMap}\n"
	linkedControl := [2]string{child + "/kustomization.yaml", "../control.yaml"}
	alternate := map[string]string{child + "/kustomization.yml": control}
	multiple := map[string]string{
		child + "/kustomization.yml": control,
		child + "/Kustomization":     control,
	}
	generated := map[string]any{"key": "value"}
	for _, fixture := range []struct {
		name       string
		entry      string
		link       [2]string
		files      map[string]string
		data       map[string]any
		generators bool
		artifact   bool
		cacheHit   bool
		wantError  string
		wantPath   string
	}{
		{
			name: "unused-link", entry: child, link: [2]string{"unused", "missing"}, artifact: true, cacheHit: true,
			files: map[string]string{child + "/kustomization.yaml": control},
		},
		{
			name: "sole-resources", link: linkedControl, artifact: true,
			wantError: "unable to find one of", wantPath: child,
		},
		{name: "alternate-control-resources", link: linkedControl, files: alternate, artifact: true},
		{
			name: "multiple-resources", link: linkedControl, files: multiple, artifact: true,
			wantError: "Found multiple kustomization files", wantPath: child,
		},
		{
			name: "sole-generators", link: linkedControl, generators: true, artifact: true,
			wantError: "unable to find one of", wantPath: child,
		},
		{
			name: "alternate-control-generators", link: linkedControl, files: alternate, generators: true, data: generated,
		},
		{
			name: "multiple-generators", link: linkedControl, files: multiple, generators: true, artifact: true,
			wantError: "Found multiple kustomization files", wantPath: child,
		},
		{
			name: "cycle", link: linkedControl, artifact: true,
			wantError: "cycle detected", wantPath: child + "/descendant",
			files: map[string]string{
				child + "/kustomization.yml":             "resources: [descendant]\n",
				child + "/descendant/kustomization.yaml": "resources: [..]\n",
			},
		},
		{
			name: "duplicate", link: linkedControl, artifact: true,
			wantError: "already registered id", wantPath: base + "/local",
			files: map[string]string{
				base + "/kustomization.yaml":       "resources: [child, local]\n",
				child + "/kustomization.yml":       control,
				child + "/cm.yaml":                 cmYAML("shared"),
				base + "/local/kustomization.yaml": control,
				base + "/local/cm.yaml":            cmYAML("shared"),
			},
		},
		{
			name: "canonical-child", artifact: true, data: map[string]any{"loader": "child"},
			files: map[string]string{
				child + "/kustomization.yaml": control + "transformers: [loader.yaml]\n",
				child + "/loader.yaml":        transformer,
			},
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			field, input := "resources", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n"
			if fixture.generators {
				field, input = "generators", generator
			}
			files := map[string]string{
				base + "/kustomization.yaml": field + ": [child]\n",
				base + "/control.yaml":       control,
				child + "/cm.yaml":           input,
			}
			maps.Copy(files, fixture.files)
			checkout := newRemoteLinkCheckout(t, files, fixture.link)
			entry := cmp.Or(fixture.entry, base)
			check := func(t *testing.T, cache *TreeCache, source *store.SourceArtifact, subpath string) {
				t.Helper()
				output, _, err := testutil.RenderMaps(Render(t.Context(), cache, RenderRequest{
					Source: source, SubPath: subpath, Spec: &manifest.Kustomization{},
				}, sourcecache.RetryConfig{}))
				if fixture.wantError != "" {
					assert.FatalIf(t, err == nil, "expected render failure")
					assert.ErrorIf(t, !strings.Contains(err.Error(), fixture.wantError) ||
						!strings.Contains(err.Error(), fixture.wantPath), "render failure: %v", err)
					return
				}
				assert.NoError(t, err)
				assert.FatalIf(t, len(output) != 1, "got %d rendered objects, want 1", len(output))
				want := map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "child"},
				}
				if fixture.data != nil {
					want["data"] = fixture.data
				}
				assert.Diff(t, output[0], want)
			}
			t.Run("clone", func(t *testing.T) {
				t.Parallel()
				cache := newTestTreeCache(t, "")
				fetch := remoteLinkFixtureFetcher(t)
				cache.SetGitBaseFetcher(fetch)
				url, ref := "file://"+checkout.dir, checkout.commit.String()
				if fixture.cacheHit {
					request := GitBaseRequest{URL: url, Ref: ref, Submodules: true}
					first, err := fetch(t.Context(), request, sourcecache.RetryConfig{})
					assert.NoError(t, err)
					cached, err := fetch(t.Context(), request, sourcecache.RetryConfig{})
					assert.NoError(t, err)
					assert.FatalIf(t, first.Content == "", "fresh source content identity is empty")
					assert.Equal(t, cached.Content, first.Content)
					assert.Equal(t, cached.Key, first.Key)
					assert.Diff(t, cached.Warnings, first.Warnings)
				}
				source := &store.SourceArtifact{Key: "root", FS: fstest.MapFS{"kustomization.yaml": {
					Data: []byte("resources:\n  - " + url + "//" + entry + "?ref=" + ref + "\n"),
				}}}
				check(t, cache, source, ".")
			})
			if fixture.artifact {
				t.Run("artifact", func(t *testing.T) {
					t.Parallel()
					input, content, err := gittree.New(t.Context(),
						checkout.repo.Storer.(*filesystem.Storage).Filesystem(), checkout.commit,
						gittree.ViewOptions{Mode: gittree.Source},
					)
					assert.NoError(t, err)
					source := &store.SourceArtifact{Key: "root", FS: input, Content: content}
					check(t, newTestTreeCache(t, ""), source, entry)
				})
			}
		})
	}
}

func TestRender_DuplicateID_RemoteControlView(t *testing.T) {
	t.Parallel()
	const control = "resources: [cm.yaml]\n"
	cm := cmYAML("shared")
	checkout := newRemoteLinkCheckout(t, map[string]string{
		"apps/base/kustomization.yaml":      "resources: [child]\n",
		"apps/base/control.yaml":            control,
		"apps/base/child/kustomization.yml": control,
		"apps/base/child/cm.yaml":           cm,
	}, [2]string{"apps/base/child/kustomization.yaml", "../control.yaml"})
	remote := "file://" + checkout.dir + "//apps/base?ref=" + checkout.commit.String()
	cache := newTestTreeCache(t, "")
	cache.SetGitBaseFetcher(remoteLinkFixtureFetcher(t))
	source := &store.SourceArtifact{Key: "root", FS: fstest.MapFS{
		"kustomization.yaml":       {Data: []byte("resources:\n  - " + remote + "\n  - local\n")},
		"local/kustomization.yaml": {Data: []byte(control)},
		"local/cm.yaml":            {Data: []byte(cm)},
	}}
	_, _, err := Render(t.Context(), cache, RenderRequest{Source: source, Spec: &manifest.Kustomization{}},
		sourcecache.RetryConfig{})
	assert.FatalIf(t, err == nil || !isDuplicateIDError(err), "expected duplicate resource error: %v", err)
	_, diagnostic, found := strings.Cut(
		err.Error(),
		"\n  duplicate resource(s) produced by multiple accumulated paths:\n",
	)
	assert.FatalIf(t, !found, "missing diagnostic attribution: %v", err)
	lines := strings.Split(diagnostic, "\n")
	assert.FatalIf(t, len(lines) != 4, "unexpected attribution: %s", diagnostic)
	assert.Equal(t, lines[0], "    ConfigMap.v1.[noGrp]/shared.[noNs]")
	assert.FatalIf(t, !strings.HasPrefix(lines[1], "      - ./.flate-remote-") ||
		!strings.HasSuffix(lines[1], "/apps/base"), "missing remote producer: %s", diagnostic)
	assert.Equal(t, lines[2], "      - ./local")
	assert.Equal(t, lines[3], "select an individual overlay with -p")
}

type remoteLinkCheckout struct {
	dir    string
	repo   *git.Repository
	commit plumbing.Hash
}

func newRemoteLinkCheckout(t *testing.T, files map[string]string, link [2]string) remoteLinkCheckout {
	t.Helper()
	dir := fakeWorktree(t, files)
	if link[0] != "" {
		assert.NoError(t, os.Symlink(link[1], filepath.Join(dir, link[0])))
	}
	repo := testutil.GitInit(t, dir)
	commit := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
		Author: &object.Signature{Name: "fixture", Email: "fixture@example.invalid"},
	})
	return remoteLinkCheckout{dir: dir, repo: repo, commit: commit}
}

func remoteLinkFixtureFetcher(t *testing.T) GitBaseFetcher {
	t.Helper()
	sources := cachetest.New(t, t.TempDir(), sourcecache.New)
	fetcher := &sourcegit.Fetcher{Network: networktest.New(t), Cache: sources}
	return func(ctx context.Context, request GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
		input := &sourcegit.RemoteBaseRequest{
			URL: request.URL, Ref: request.Ref, Timeout: request.Timeout, Submodules: request.Submodules,
		}
		policy := &sourcecache.Policy{MaxAge: request.MaxAge, Retry: retry}
		return fetcher.FetchRemoteBase(ctx, input, policy)
	}
}
