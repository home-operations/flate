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
)

// TestPreflightRemoteResources_RewritesSuccessfulFetch confirms the
// happy path: a kustomization listing an HTTP resource → flate fetches
// it via Go's http client → kustomization.yaml gets the URL replaced
// with the local file → the fetched body lives on disk next to the
// kustomization. The point is to make kustomize.Build see local files
// only so it never invokes the git fallback.
func TestPreflightRemoteResources_RewritesSuccessfulFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)

	stage := t.TempDir()
	ks := filepath.Join(stage, "kustomization.yaml")
	mustWriteFile(t, ks, "resources:\n  - "+srv.URL+"/foo.yaml\n")

	if err := preflightRemoteResources(context.Background(), newPreflightCache(t), stage); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	body, err := os.ReadFile(ks) //nolint:gosec // ks is inside t.TempDir
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), srv.URL) {
		t.Errorf("URL still present after preflight:\n%s", body)
	}
	if !strings.Contains(string(body), ".flate-remote-") {
		t.Errorf("expected rewritten path with .flate-remote prefix:\n%s", body)
	}

	// Verify the fetched body landed on disk.
	matches, _ := filepath.Glob(filepath.Join(stage, ".flate-remote-*.yaml"))
	if len(matches) != 1 {
		t.Fatalf("expected one fetched file, got %v", matches)
	}
	cached, err := os.ReadFile(matches[0]) //nolint:gosec // matches[0] is t.TempDir
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cached), "kind: ConfigMap") {
		t.Errorf("fetched body lost: %q", cached)
	}
}

// TestPreflightRemoteResources_PropagatesFailure locks the contract:
// a URL fetch failure returns an error to the caller — the KS
// controller surfaces it as a real reconcile failure rather than
// silently tombstoning and letting the build pass with a missing
// resource. The error wraps the URL and the underlying status/IO
// error so `flate test` shows the user what's actually broken in
// their repo.
func TestPreflightRemoteResources_PropagatesFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	stage := t.TempDir()
	ks := filepath.Join(stage, "kustomization.yaml")
	mustWriteFile(t, ks, "resources:\n  - "+srv.URL+"/missing.yaml\n")

	err := preflightRemoteResources(context.Background(), newPreflightCache(t), stage)
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

// TestPreflightRemoteResources_IgnoresLocalEntries guards the no-op
// path: a kustomization with only local resources must be untouched.
func TestPreflightRemoteResources_IgnoresLocalEntries(t *testing.T) {
	stage := t.TempDir()
	ks := filepath.Join(stage, "kustomization.yaml")
	body := "resources:\n  - ./local.yaml\n  - ../shared/cm.yaml\n"
	mustWriteFile(t, ks, body)

	if err := preflightRemoteResources(context.Background(), newPreflightCache(t), stage); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	got, _ := os.ReadFile(ks) //nolint:gosec // ks is t.TempDir
	if string(got) != body {
		t.Errorf("local-only kustomization was modified:\nwant %q\ngot  %q", body, got)
	}
}

// TestPreflightRemoteResources_WalksNestedKustomizations covers the
// recursive case: a Components / overlay layout where the URL
// resource hides inside a subdir's kustomization.yaml.
func TestPreflightRemoteResources_WalksNestedKustomizations(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: nested}\n"))
	}))
	t.Cleanup(srv.Close)

	stage := t.TempDir()
	nested := filepath.Join(stage, "components", "x")
	if err := os.MkdirAll(nested, 0o750); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(stage, "kustomization.yaml"),
		"resources:\n  - ./components/x\n")
	mustWriteFile(t, filepath.Join(nested, "kustomization.yaml"),
		"resources:\n  - "+srv.URL+"/nested.yaml\n")

	if err := preflightRemoteResources(context.Background(), newPreflightCache(t), stage); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	body, _ := os.ReadFile(filepath.Join(nested, "kustomization.yaml")) //nolint:gosec // t.TempDir
	if strings.Contains(string(body), srv.URL) {
		t.Errorf("nested URL not rewritten:\n%s", body)
	}
}

// TestPreflightRemoteResources_HonorsAlternateFilenames sanity-
// checks the filename matcher: kustomize accepts kustomization.yml
// and Kustomization in addition to kustomization.yaml.
func TestPreflightRemoteResources_HonorsAlternateFilenames(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\n"))
	}))
	t.Cleanup(srv.Close)

	for _, name := range []string{"kustomization.yml", "Kustomization"} {
		t.Run(name, func(t *testing.T) {
			stage := t.TempDir()
			mustWriteFile(t, filepath.Join(stage, name),
				"resources:\n  - "+srv.URL+"/x.yaml\n")
			if err := preflightRemoteResources(context.Background(), newPreflightCache(t), stage); err != nil {
				t.Fatalf("preflight: %v", err)
			}
			body, _ := os.ReadFile(filepath.Join(stage, name)) //nolint:gosec // t.TempDir
			if strings.Contains(string(body), srv.URL) {
				t.Errorf("%s URL not rewritten:\n%s", name, body)
			}
		})
	}
}

// TestPreflightRemoteResources_FetchesEachURLOnce pins the dedup
// contract: a URL referenced from N kustomization files (parent +
// nested child + sibling overlay …) must hit the network exactly
// once per orchestrator run. The fix for the m00nwtchr report
// where the same broken URL emitted 6 WARNs per `flate test all`.
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

	stage := t.TempDir()
	// Three kustomization files, all referencing the same URL.
	for _, sub := range []string{"a", "b", "c"} {
		dir := filepath.Join(stage, sub)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		mustWriteFile(t, filepath.Join(dir, "kustomization.yaml"),
			"resources:\n  - "+srv.URL+"/shared.yaml\n")
	}

	cache := newPreflightCache(t)
	if err := preflightRemoteResources(context.Background(), cache, stage); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if hits != 1 {
		t.Errorf("expected 1 network call, got %d", hits)
	}

	// Each kustomization still got its own local copy written.
	for _, sub := range []string{"a", "b", "c"} {
		matches, _ := filepath.Glob(filepath.Join(stage, sub, ".flate-remote-*.yaml"))
		if len(matches) != 1 {
			t.Errorf("%s: expected one local copy, got %v", sub, matches)
		}
	}

	// Running preflight again on the same cache must NOT re-fetch.
	// (Tests the dedup spans across multiple preflight calls, which
	// is the actual production pattern — one preflight per
	// RenderFlux invocation.)
	stage2 := t.TempDir()
	mustWriteFile(t, filepath.Join(stage2, "kustomization.yaml"),
		"resources:\n  - "+srv.URL+"/shared.yaml\n")
	if err := preflightRemoteResources(context.Background(), cache, stage2); err != nil {
		t.Fatalf("second preflight: %v", err)
	}
	if hits != 1 {
		t.Errorf("expected 1 network call across both runs, got %d", hits)
	}
}

func mustWriteFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// newPreflightCache returns a fresh StagingCache so each test gets
// its own remote-fetch dedup table (no cross-test cache hits).
func newPreflightCache(t *testing.T) *StagingCache {
	t.Helper()
	c, err := NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestHttpGetURL_RejectsOversizedBody pins the round-4 OOM
// guard: a malicious or accidentally-huge endpoint returning more
// than remoteFetchMaxBytes must fail fast rather than reading the
// whole body into memory. Reads MaxBytes+1 via LimitReader and
// errors on overflow.
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

// TestRewriteURLResources_PreservesCommentsAndOrder pins the
// yaml.Node node-level edit fix: only the rewritten resource entry
// changes, every other byte (comments, other-key ordering, trailing
// content) survives intact. The previous map-decode + re-marshal
// round-trip silently destroyed all of that.
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
	dir := t.TempDir()
	ks := filepath.Join(dir, "kustomization.yaml")
	mustWriteFile(t, ks, original)

	if err := rewriteURLResources(context.Background(), newPreflightCache(t), ks); err != nil {
		t.Fatalf("rewriteURLResources: %v", err)
	}

	got, err := os.ReadFile(ks) //nolint:gosec // ks under t.TempDir
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
	if !strings.Contains(out, ".flate-remote-") {
		t.Errorf("URL entry not rewritten to local file:\n%s", out)
	}
	if strings.Contains(out, srv.URL) {
		t.Errorf("URL still present after rewrite:\n%s", out)
	}
	if idxRes, idxNs := strings.Index(out, "\nresources:"), strings.Index(out, "\nnamespace:"); idxRes < 0 || idxNs < 0 || idxRes >= idxNs {
		t.Errorf("key ordering not preserved (resources should precede namespace):\n%s", out)
	}
}

// TestHttpGetURL_AcceptsBodyAtCap is the boundary check: a body
// exactly at the cap must succeed. Guards against off-by-one in the
// LimitReader+overflow-detection pattern.
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
