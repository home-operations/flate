package kustomize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

	if err := preflightRemoteResources(context.Background(), stage); err != nil {
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

// TestPreflightRemoteResources_TombstoneOnFailure locks the
// fail-fast behavior: a URL returning 404 → tombstone written →
// kustomization.yaml points at the tombstone (not at the URL, not
// at the git fallback). Verifies the fix for m00nwtchr's case where
// a broken URL used to cascade 10+ seconds through kustomize's
// HTTP-then-git path.
func TestPreflightRemoteResources_TombstoneOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	stage := t.TempDir()
	ks := filepath.Join(stage, "kustomization.yaml")
	mustWriteFile(t, ks, "resources:\n  - "+srv.URL+"/missing.yaml\n")

	if err := preflightRemoteResources(context.Background(), stage); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	body, _ := os.ReadFile(ks) //nolint:gosec // ks is t.TempDir
	if strings.Contains(string(body), srv.URL) {
		t.Errorf("failed URL still present (should be tombstone):\n%s", body)
	}
	if !strings.Contains(string(body), ".flate-tombstone-") {
		t.Errorf("expected tombstone rewrite:\n%s", body)
	}

	matches, _ := filepath.Glob(filepath.Join(stage, ".flate-tombstone-*.yaml"))
	if len(matches) != 1 {
		t.Fatalf("expected one tombstone, got %v", matches)
	}
	tomb, _ := os.ReadFile(matches[0]) //nolint:gosec // matches[0] is t.TempDir
	if !strings.Contains(string(tomb), "remote resource fetch failed") ||
		!strings.Contains(string(tomb), "HTTP 404") {
		t.Errorf("tombstone missing diagnostic info:\n%s", tomb)
	}
}

// TestPreflightRemoteResources_IgnoresLocalEntries guards the no-op
// path: a kustomization with only local resources must be untouched.
func TestPreflightRemoteResources_IgnoresLocalEntries(t *testing.T) {
	stage := t.TempDir()
	ks := filepath.Join(stage, "kustomization.yaml")
	body := "resources:\n  - ./local.yaml\n  - ../shared/cm.yaml\n"
	mustWriteFile(t, ks, body)

	if err := preflightRemoteResources(context.Background(), stage); err != nil {
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

	if err := preflightRemoteResources(context.Background(), stage); err != nil {
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
			if err := preflightRemoteResources(context.Background(), stage); err != nil {
				t.Fatalf("preflight: %v", err)
			}
			body, _ := os.ReadFile(filepath.Join(stage, name)) //nolint:gosec // t.TempDir
			if strings.Contains(string(body), srv.URL) {
				t.Errorf("%s URL not rewritten:\n%s", name, body)
			}
		})
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
