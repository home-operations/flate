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
	"slices"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/manifest"
)

// remoteFetchTimeout caps each pre-flight HTTP GET. Kustomize's
// built-in loader has no timeout knob; we want broken URLs to fail
// in seconds, not minutes.
const remoteFetchTimeout = 5 * time.Second

// remoteFetchMaxBytes caps each pre-flight response body. A
// kustomization resource is almost always under a megabyte of YAML;
// 64 MiB is several orders of magnitude of headroom that still
// bounds the OOM blast radius from a malicious or accidentally-huge
// URL endpoint.
const remoteFetchMaxBytes = 64 << 20 // 64 MiB

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
		if !slices.Contains(manifest.KustomizeBuilderFilenames, d.Name()) {
			return nil
		}
		return rewriteURLResources(ctx, cache, path)
	})
}

// rewriteURLResources rewrites URL entries in one kustomization file
// via yaml.Node node-level editing. Previous implementations decoded
// to map[string]any and re-marshaled, which round-tripped destroyed
// YAML comments, key ordering, anchors/aliases, and block-vs-flow
// distinctions — any downstream diff against the staged tree saw
// noise unrelated to a user's edit, and re-marshal of map-decoded
// values changed scalar quoting in ways kustomize can interpret
// differently.
//
// Node-level editing modifies ONLY the resources sequence entries
// that match HTTP/HTTPS URLs; every other byte in the file (comments,
// other-key ordering, anchors) survives the round-trip intact.
//
// Returns the first URL fetch failure encountered so the caller can
// fail the Kustomization's reconcile rather than silently dropping
// the missing resource.
func rewriteURLResources(ctx context.Context, cache *StagingCache, ksFile string) error {
	body, err := os.ReadFile(ksFile) //nolint:gosec // ksFile is a tree-walk result under our staged copy
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		// Some kustomization files use unusual shapes (YAML anchors,
		// strict-mode fields) that decode can't handle. Skip silently
		// — kustomize will load them via its own parser, and if any
		// carry URL resources we fall back to kustomize's
		// HTTP-then-git path. Better to render imperfectly than fail
		// loud on a doc kustomize itself handles.
		return nil
	}
	resourcesNode := findMappingValue(&doc, "resources")
	if resourcesNode == nil || resourcesNode.Kind != yaml.SequenceNode || len(resourcesNode.Content) == 0 {
		return nil
	}
	changed := false
	dir := filepath.Dir(ksFile)
	for _, entry := range resourcesNode.Content {
		if entry.Kind != yaml.ScalarNode {
			continue
		}
		if !isHTTPURL(entry.Value) {
			continue
		}
		localFile, fetchErr := fetchRemoteResource(ctx, cache, dir, entry.Value)
		if fetchErr != nil {
			return fmt.Errorf("remote resource %s: %w", entry.Value, fetchErr)
		}
		entry.Value = "./" + localFile
		entry.Tag = "!!str"
		entry.Style = 0 // plain scalar; preserve other entries' styles
		changed = true
	}
	if !changed {
		return nil
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	return os.WriteFile(ksFile, out, 0o600)
}

// findMappingValue returns the value node for the first mapping
// entry with the given key inside the document. Returns nil when
// the document is not a single-mapping document or the key is
// absent. Used by rewriteURLResources to locate the "resources:"
// sequence without round-tripping the whole document.
func findMappingValue(doc *yaml.Node, key string) *yaml.Node {
	if doc == nil || doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil
	}
	// MappingNode.Content is [key, value, key, value, ...].
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == key {
			return root.Content[i+1]
		}
	}
	return nil
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
	// Cap with LimitReader +1 so we can detect overflow precisely:
	// read up to remoteFetchMaxBytes+1, and if we actually got
	// MaxBytes+1 bytes the body is larger than the cap and we fail
	// fast instead of OOMing on an attacker-controlled endpoint.
	body, err := io.ReadAll(io.LimitReader(resp.Body, remoteFetchMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > remoteFetchMaxBytes {
		return nil, fmt.Errorf("response body exceeds %d-byte cap", remoteFetchMaxBytes)
	}
	return body, nil
}

func urlHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}
