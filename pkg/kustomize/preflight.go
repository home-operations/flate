package kustomize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"sigs.k8s.io/kustomize/kyaml/filesys"

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

// remoteResourcePrefix names the in-memory entries preflight writes beside a
// kustomization for a pre-fetched remote resource: a `<prefix><hash>.yaml`
// file for an HTTP single-file resource, or a `<prefix><hash>/` directory for
// a cloned git base. These live only in the render's private in-memory fs (see
// tree.go) — never on disk — so they are an implementation detail, not an
// on-disk marker. The walk skips the directory form so a cloned base's own
// nested kustomizations aren't re-rewritten.
const remoteResourcePrefix = ".flate-remote-"

// preflightRemoteResources walks every kustomization file under subPath in the
// render's private in-memory fs and pre-resolves remote `resources:` entries so
// kustomize.Build sees only local (in-fs) files — never invoking its built-in
// `exec.Command("git", "fetch", ...)` / HTTP fallback (which would reach the
// real network/disk, outside the fs sandbox, on a 10s+ timeout for a broken
// URL). HTTP/HTTPS single-file entries are fetched via flate's own HTTP client;
// git bases (URLs carrying kustomize's git markers / ?ref=) are cloned via the
// injected GitBaseFetcher and materialized into the fs. See gitbase.go.
//
// Scoped to subPath (not the whole fs) so a broken URL in one Kustomization's
// tree fails only that Kustomization's reconcile. Components reaching `../`
// paths outside subPath are an acknowledged blind spot.
//
// On a fetch failure (timeout, 4xx, 5xx, DNS, connection refused) preflight
// returns the error immediately, so the KS controller surfaces it as a real
// reconcile failure rather than a silent missing resource.
func preflightRemoteResources(ctx context.Context, cache *TreeCache, memFS filesys.FileSystem, subPath string) error {
	return memFS.Walk(subPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Skip a git base materialized by a prior resources: entry — its
			// own nested kustomization.yaml files must NOT be re-rewritten here
			// (depth-1 limit). The kustomize build of that base resolves its URLs.
			if strings.HasPrefix(filepath.Base(path), remoteResourcePrefix) {
				return filepath.SkipDir
			}
			return nil
		}
		if !slices.Contains(manifest.KustomizeBuilderFilenames, filepath.Base(path)) {
			return nil
		}
		return rewriteURLResources(ctx, cache, memFS, path)
	})
}

// rewriteURLResources rewrites URL entries in one kustomization file via
// yaml.Node node-level editing. Node-level editing modifies ONLY the resources
// sequence entries that match HTTP/HTTPS URLs (or git bases); every other byte
// in the file (comments, key ordering, anchors, block-vs-flow style) survives
// the round-trip intact, which a decode-to-map-and-remarshal pass would destroy.
//
// Returns the first fetch failure encountered so the caller can fail the
// Kustomization's reconcile rather than silently dropping the missing resource.
func rewriteURLResources(ctx context.Context, cache *TreeCache, memFS filesys.FileSystem, ksFile string) error {
	body, err := memFS.ReadFile(ksFile)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		// Some kustomization files use unusual shapes (YAML anchors,
		// strict-mode fields) that decode can't handle. Skip silently —
		// kustomize will load them via its own parser, and if any carry URL
		// resources we fall back to kustomize's own fetch path. Better to
		// render imperfectly than fail loud on a doc kustomize itself handles.
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
		// Classify git bases BEFORE the HTTP-file check: a git base is also an
		// https:// URL, but kustomize resolves it by cloning, not by GETting a
		// single file. GETting a git URL returns the host's HTML page, which
		// then fails to parse as YAML (#616) — so it must take the git path.
		if spec, ok := isGitRemoteBase(entry.Value); ok {
			localDir, fetchErr := fetchGitBase(ctx, cache, memFS, dir, spec)
			if fetchErr != nil {
				return fmt.Errorf("remote git base %s: %w", entry.Value, fetchErr)
			}
			setPlainScalar(entry, localDir)
			changed = true
			continue
		}
		if !isHTTPURL(entry.Value) {
			continue
		}
		localFile, fetchErr := fetchRemoteResource(ctx, cache, memFS, dir, entry.Value)
		if fetchErr != nil {
			return fmt.Errorf("remote resource %s: %w", entry.Value, fetchErr)
		}
		setPlainScalar(entry, "./"+localFile)
		changed = true
	}
	if !changed {
		return nil
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	return memFS.WriteFile(ksFile, out)
}

// setPlainScalar rewrites node to a plain (unquoted, untagged) string scalar
// holding value, leaving sibling sequence entries' styles untouched.
func setPlainScalar(node *yaml.Node, value string) {
	node.Value = value
	node.Tag = "!!str"
	node.Style = 0
}

// findMappingValue returns the value node for the first mapping entry with the
// given key inside the document, or nil when the document is not a single-
// mapping document or the key is absent.
func findMappingValue(doc *yaml.Node, key string) *yaml.Node {
	if doc == nil || doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil
	}
	// MappingNode.Content is [key, value, key, value, ...]; iterate as pairs.
	for pair := range slices.Chunk(root.Content, 2) {
		if len(pair) == 2 && pair[0].Value == key {
			return pair[1]
		}
	}
	return nil
}

func isHTTPURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// fetchRemoteResource fetches urlStr into a <prefix><hash>.yaml file beside the
// kustomization that referenced it (in the render's private in-memory fs),
// returning the local filename. The HTTP fetch is deduped via cache.FetchRemote
// so multiple kustomizations referencing the same URL share one network call.
func fetchRemoteResource(ctx context.Context, cache *TreeCache, memFS filesys.FileSystem, dir, urlStr string) (string, error) {
	body, err := cache.FetchRemote(ctx, urlStr)
	if err != nil {
		return "", err
	}
	name := remoteResourcePrefix + urlHash(urlStr) + ".yaml"
	if err := memFS.WriteFile(filepath.Join(dir, name), body); err != nil {
		return "", err
	}
	return name, nil
}

// httpStatusError is a typed sentinel returned by httpGetURL when the server
// responds with a non-2xx status code. The named type lets isHTTPClientError
// classify errors via errors.As so the check is robust against wrapping.
type httpStatusError struct {
	Code int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d", e.Code)
}

// httpGetURL is the actual network call cache.FetchRemote dispatches through a
// per-URL sync.Once + done channel.
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
		return nil, &httpStatusError{Code: resp.StatusCode}
	}
	// Cap with LimitReader +1 so we can detect overflow precisely: read up to
	// remoteFetchMaxBytes+1, and if we actually got MaxBytes+1 bytes the body
	// is larger than the cap and we fail fast instead of OOMing.
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
