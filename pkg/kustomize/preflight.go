package kustomize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v4"
)

// remoteFetchTimeout caps each pre-flight HTTP GET. Kustomize's
// built-in loader has no timeout knob; we want broken URLs to fail
// in seconds, not minutes.
const remoteFetchTimeout = 5 * time.Second

// remoteFetchClient is the package-level client used by the
// pre-flight pass. Distinct from the helm/oci clients so resource-
// fetch latency stays observable.
var remoteFetchClient = &http.Client{Timeout: remoteFetchTimeout}

// preflightRemoteResources walks every kustomization file under root
// and pre-fetches HTTP/HTTPS `resources:` entries via flate's own
// HTTP client so kustomize.Build sees only local files — never
// invoking its built-in `exec.Command("git", "fetch", ...)` fallback
// (which silently adds a git-binary dependency and a 10s+ timeout
// on broken URLs).
//
// root is the **subPath dir**, not the whole stage. Scoping the walk
// makes URL-fetch failures localized: a broken URL in one
// Kustomization's tree fails only that Kustomization's reconcile
// without poisoning unrelated ones. Components that reference `../`
// paths outside subPath are an acknowledged blind spot — uncommon
// in practice and easy to extend later if needed.
//
// On a URL fetch failure (timeout, 4xx, 5xx, DNS, connection refused)
// preflight returns the error immediately. The KS controller's
// reconcile propagates it through RunWithStatus, which surfaces it
// in `flate test` as a real reconcile failure — no silent tombstone
// that lets the build pass with a missing resource.
//
// Walks all three valid kustomization filenames (kustomization.yaml,
// kustomization.yml, Kustomization). Only mutates the staged copy
// the caller owns; the user's source tree is never touched.
func preflightRemoteResources(ctx context.Context, cache *StagingCache, root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if name != "kustomization.yaml" && name != "kustomization.yml" && name != "Kustomization" {
			return nil
		}
		return rewriteURLResources(ctx, cache, path)
	})
}

// rewriteURLResources rewrites URL entries in one kustomization file.
// Reads the file as a generic YAML doc to preserve unknown fields,
// rewrites the resources list, writes the file back. Returns the
// first URL fetch failure encountered so the caller can fail the
// Kustomization's reconcile rather than silently dropping the
// missing resource.
func rewriteURLResources(ctx context.Context, cache *StagingCache, ksFile string) error {
	body, err := os.ReadFile(ksFile) //nolint:gosec // ksFile is a tree-walk result under our staged copy
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(body, &doc); err != nil {
		// Some kustomization files use unusual shapes (YAML anchors,
		// strict-mode fields) that our generic decode can't round-trip.
		// Skip them silently — kustomize will load them via its own
		// parser, and if any of those carry URL resources we fall back
		// to kustomize's HTTP-then-git path. Better to render imperfectly
		// than fail loud on a doc kustomize itself handles.
		return nil
	}
	rawRes, _ := doc["resources"].([]any)
	if len(rawRes) == 0 {
		return nil
	}
	changed := false
	dir := filepath.Dir(ksFile)
	for i, entry := range rawRes {
		urlStr, ok := entry.(string)
		if !ok {
			continue
		}
		if !isHTTPURL(urlStr) {
			continue
		}
		localFile, fetchErr := fetchRemoteResource(ctx, cache, dir, urlStr)
		if fetchErr != nil {
			return fmt.Errorf("remote resource %s: %w", urlStr, fetchErr)
		}
		rawRes[i] = "./" + localFile
		changed = true
	}
	if !changed {
		return nil
	}
	doc["resources"] = rawRes
	out, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(ksFile, out, 0o600)
}

func isHTTPURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// fetchRemoteResource fetches the URL into a .flate-remote-<hash>.yaml
// file next to the kustomization that referenced it. The actual HTTP
// fetch is deduped via cache.FetchRemote — multiple kustomization
// files referencing the same URL share one network call and one
// cached result. The local filename is deterministic per URL so
// re-runs reuse the same on-disk file (kustomize sees a stable input).
func fetchRemoteResource(ctx context.Context, cache *StagingCache, dir, urlStr string) (string, error) {
	body, err := cache.FetchRemote(ctx, urlStr)
	if err != nil {
		return "", err
	}
	name := ".flate-remote-" + urlHash(urlStr) + ".yaml"
	if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
		return "", err
	}
	return name, nil
}

// httpGetURL is the actual network call cache.FetchRemote dispatches
// through OnceValues. Lives here (not on the StagingCache) because
// it's a preflight detail — the cache only owns the dedup discipline.
func httpGetURL(ctx context.Context, urlStr string) ([]byte, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, remoteFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	resp, err := remoteFetchClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func urlHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}
