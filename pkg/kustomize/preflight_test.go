package kustomize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// memFSWith builds an in-memory filesystem from a map of path -> contents,
// the shape preflight now operates on (a render's private fs).
func memFSWith(t *testing.T, files map[string]string) filesys.FileSystem {
	t.Helper()
	fsys := filesys.MakeFsInMemory()
	for p, body := range files {
		if err := fsys.WriteFile(p, []byte(body)); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	return fsys
}

// remoteFiles lists the pre-fetched remote-resource files preflight wrote into
// dir (the .flate-remote-*.yaml form), so a test can assert one landed.
func remoteFiles(t *testing.T, fsys filesys.FileSystem, dir string) []string {
	t.Helper()
	names, err := fsys.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, n := range names {
		if strings.HasPrefix(n, remoteResourcePrefix) {
			out = append(out, n)
		}
	}
	return out
}

// newPreflightCache returns a fresh TreeCache so each test gets its own
// remote-fetch dedup table (no cross-test cache hits).
func newPreflightCache(t *testing.T) *TreeCache {
	t.Helper()
	return NewTreeCache()
}

// TestPreflightRemoteResources_RewritesSuccessfulFetch confirms the happy
// path: a kustomization listing an HTTP resource → flate fetches it → the
// kustomization gets the URL replaced with the local file → the fetched body
// lives next to the kustomization in the fs. The point is to make
// kustomize.Build see local files only so it never invokes the git fallback.
func TestPreflightRemoteResources_RewritesSuccessfulFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/foo.yaml\n"})
	if err := preflightRemoteResources(context.Background(), newPreflightCache(t), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	body, err := fsys.ReadFile("kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), srv.URL) {
		t.Errorf("URL still present after preflight:\n%s", body)
	}
	if !strings.Contains(string(body), remoteResourcePrefix) {
		t.Errorf("expected rewritten path with %s prefix:\n%s", remoteResourcePrefix, body)
	}

	fetched := remoteFiles(t, fsys, ".")
	if len(fetched) != 1 {
		t.Fatalf("expected one fetched file, got %v", fetched)
	}
	cached, err := fsys.ReadFile(fetched[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cached), "kind: ConfigMap") {
		t.Errorf("fetched body lost: %q", cached)
	}
}

// TestPreflightRemoteResources_PropagatesFailure locks the contract: a URL
// fetch failure returns an error to the caller — the KS controller surfaces it
// as a real reconcile failure rather than silently tombstoning. The error
// wraps the URL and the underlying status so `flate test` shows the user what's
// actually broken in their repo.
func TestPreflightRemoteResources_PropagatesFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/missing.yaml\n"})
	err := preflightRemoteResources(context.Background(), newPreflightCache(t), fsys, ".")
	if err == nil {
		t.Fatal("expected error on 404; got nil (preflight would silently swallow the failure)")
	}
	if !strings.Contains(err.Error(), srv.URL) {
		t.Errorf("error should name the URL; got %q", err)
	}
	if !strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("error should name the status code; got %q", err)
	}
}

// TestPreflightRemoteResources_IgnoresLocalEntries guards the no-op path: a
// kustomization with only local resources must be untouched.
func TestPreflightRemoteResources_IgnoresLocalEntries(t *testing.T) {
	body := "resources:\n  - ./local.yaml\n  - ../shared/cm.yaml\n"
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": body})
	if err := preflightRemoteResources(context.Background(), newPreflightCache(t), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	got, _ := fsys.ReadFile("kustomization.yaml")
	if string(got) != body {
		t.Errorf("local-only kustomization was modified:\nwant %q\ngot  %q", body, got)
	}
}

// TestPreflightRemoteResources_WalksNestedKustomizations covers the recursive
// case: a Components / overlay layout where the URL resource hides inside a
// subdir's kustomization.yaml.
func TestPreflightRemoteResources_WalksNestedKustomizations(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: nested}\n"))
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{
		"kustomization.yaml":              "resources:\n  - ./components/x\n",
		"components/x/kustomization.yaml": "resources:\n  - " + srv.URL + "/nested.yaml\n",
	})
	if err := preflightRemoteResources(context.Background(), newPreflightCache(t), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	body, _ := fsys.ReadFile("components/x/kustomization.yaml")
	if strings.Contains(string(body), srv.URL) {
		t.Errorf("nested URL not rewritten:\n%s", body)
	}
}

// TestRenderFlux_RemotePreflightDoesNotMutateSource proves the in-memory render
// never touches the user's working tree: after a render that pre-fetches a
// remote resource, the on-disk source kustomization is byte-for-byte unchanged
// and no fetched files were written to disk.
func TestRenderFlux_RemotePreflightDoesNotMutateSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)

	child := "resources:\n  - " + srv.URL + "/remote.yaml\n"
	src := writeTree(t, map[string]string{
		"kustomization.yaml":       "resources:\n  - ./child\n",
		"child/kustomization.yaml": child,
	})

	_, err := RenderFlux(context.Background(), NewTreeCache(), src, false, ".", map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	})
	if err != nil {
		t.Fatalf("RenderFlux: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(src, "child", "kustomization.yaml")) //nolint:gosec // src is t.TempDir
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != child {
		t.Fatalf("source nested kustomization was mutated:\nwant %q\ngot  %q", child, got)
	}
	matches, _ := filepath.Glob(filepath.Join(src, "child", remoteResourcePrefix+"*.yaml"))
	if len(matches) != 0 {
		t.Fatalf("remote preflight wrote fetched files into source tree: %v", matches)
	}
}

// TestPreflightRemoteResources_HonorsAlternateFilenames sanity-checks the
// filename matcher: kustomize accepts kustomization.yml and Kustomization in
// addition to kustomization.yaml.
func TestPreflightRemoteResources_HonorsAlternateFilenames(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\n"))
	}))
	t.Cleanup(srv.Close)

	for _, name := range []string{"kustomization.yml", "Kustomization"} {
		t.Run(name, func(t *testing.T) {
			fsys := memFSWith(t, map[string]string{name: "resources:\n  - " + srv.URL + "/x.yaml\n"})
			if err := preflightRemoteResources(context.Background(), newPreflightCache(t), fsys, "."); err != nil {
				t.Fatalf("preflight: %v", err)
			}
			body, _ := fsys.ReadFile(name)
			if strings.Contains(string(body), srv.URL) {
				t.Errorf("%s URL not rewritten:\n%s", name, body)
			}
		})
	}
}

// TestPreflightRemoteResources_FetchesEachURLOnce pins the dedup contract: a
// URL referenced from N kustomization files (parent + nested child + sibling
// overlay …) must hit the network exactly once per run — the fix for the
// m00nwtchr report where the same broken URL emitted 6 WARNs per `flate test
// all`.
func TestPreflightRemoteResources_FetchesEachURLOnce(t *testing.T) {
	var hits int32
	mu := &sync.Mutex{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n"))
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{
		"a/kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n",
		"b/kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n",
		"c/kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n",
	})

	cache := newPreflightCache(t)
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if hits != 1 {
		t.Errorf("expected 1 network call, got %d", hits)
	}
	for _, sub := range []string{"a", "b", "c"} {
		if got := remoteFiles(t, fsys, sub); len(got) != 1 {
			t.Errorf("%s: expected one local copy, got %v", sub, got)
		}
	}

	// A second preflight against the same cache (production runs one preflight
	// per RenderFlux) must NOT re-fetch.
	fsys2 := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n"})
	if err := preflightRemoteResources(context.Background(), cache, fsys2, "."); err != nil {
		t.Fatalf("second preflight: %v", err)
	}
	if hits != 1 {
		t.Errorf("expected 1 network call across both runs, got %d", hits)
	}
}

// TestHttpGetURL_RejectsOversizedBody pins the OOM guard: an endpoint returning
// more than remoteFetchMaxBytes must fail fast rather than reading the whole
// body into memory.
func TestHttpGetURL_RejectsOversizedBody(t *testing.T) {
	huge := strings.Repeat("a", remoteFetchMaxBytes+1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(huge))
	}))
	t.Cleanup(srv.Close)

	_, err := httpGetURL(context.Background(), srv.URL+"/big.yaml")
	if err == nil {
		t.Fatalf("expected oversized-body error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected 'exceeds ... cap' error, got %q", err.Error())
	}
}

// TestRewriteURLResources_PreservesCommentsAndOrder pins the yaml.Node
// node-level edit: only the rewritten resource entry changes; every other byte
// (comments, key ordering, trailing content) survives intact.
func TestRewriteURLResources_PreservesCommentsAndOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("kind: ConfigMap\n"))
	}))
	t.Cleanup(srv.Close)

	original := `# Owner comment that MUST survive.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# why we have this list
resources:
  # remote shared fragment
  - ` + srv.URL + `/x.yaml
  - ./local.yaml  # keep this comment
namespace: my-app
`
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": original})
	if err := rewriteURLResources(context.Background(), newPreflightCache(t), fsys, "kustomization.yaml"); err != nil {
		t.Fatalf("rewriteURLResources: %v", err)
	}

	got, err := fsys.ReadFile("kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	out := string(got)

	for _, expect := range []string{
		"# Owner comment that MUST survive.",
		"# why we have this list",
		"# remote shared fragment",
		"# keep this comment",
	} {
		if !strings.Contains(out, expect) {
			t.Errorf("comment dropped on rewrite: %q\nfile is:\n%s", expect, out)
		}
	}
	if !strings.Contains(out, "- ./local.yaml") {
		t.Errorf("local resource path mangled:\n%s", out)
	}
	if !strings.Contains(out, remoteResourcePrefix) {
		t.Errorf("URL entry not rewritten to local file:\n%s", out)
	}
	if strings.Contains(out, srv.URL) {
		t.Errorf("URL still present after rewrite:\n%s", out)
	}
	if idxRes, idxNs := strings.Index(out, "\nresources:"), strings.Index(out, "\nnamespace:"); idxRes < 0 || idxNs < 0 || idxRes >= idxNs {
		t.Errorf("key ordering not preserved (resources should precede namespace):\n%s", out)
	}
}

// TestHttpGetURL_AcceptsBodyAtCap is the boundary check: a body exactly at the
// cap must succeed. Guards against off-by-one in the LimitReader+overflow
// pattern.
func TestHttpGetURL_AcceptsBodyAtCap(t *testing.T) {
	atCap := strings.Repeat("b", remoteFetchMaxBytes)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(atCap))
	}))
	t.Cleanup(srv.Close)

	body, err := httpGetURL(context.Background(), srv.URL+"/atcap.yaml")
	if err != nil {
		t.Fatalf("body exactly at cap should succeed, got: %v", err)
	}
	if len(body) != remoteFetchMaxBytes {
		t.Errorf("expected %d bytes, got %d", remoteFetchMaxBytes, len(body))
	}
}
