package helm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// SecretGetter is the same shape as source.SecretGetter; aliased so
// the helm Client and the source Fetchers consume one canonical type.
// The orchestrator wires the same closure into both.
type SecretGetter = source.SecretGetter

// OCIPuller fetches an OCI artifact into a content-addressed slot
// directory. Helm.Client uses it to route HelmRepository(type=oci)
// and OCIRepository chart resolution through the same machinery as
// source/oci.Fetcher — applying spec.verify / certSecretRef /
// proxySecretRef / insecure / layerSelector / ignore uniformly
// regardless of whether the chart was referenced via OCIRepository
// or HelmRepository(type=oci). When nil, helm.Client falls back to
// its built-in registry-client pull (no TLS/auth/verify surface) —
// matches the pre-unification behavior for EnableOCI=false runs.
//
// source/oci.Fetcher satisfies this interface verbatim (type alias).
type OCIPuller = source.TypedFetcher[*manifest.OCIRepository]

// Client renders HelmReleases. Construct with NewClient.
type Client struct {
	tmpDir   string
	cacheDir string

	mu sync.RWMutex
	// resolver is the canonical (and only) source-lookup surface.
	// Embedders MUST call SetSourceResolver before any Template call;
	// the orchestrator wires NewStoreSourceResolver(store) at
	// construction.
	resolver SourceResolver
	registry *registry.Client
	secrets  SecretGetter
	// ociPuller, when wired, routes both OCIRepository chart
	// resolution and HelmRepository(type=oci) chart resolution
	// through source/oci.Fetcher — so spec.verify / cert /
	// proxy / layerSelector / ignore all apply uniformly. Nil
	// retains the legacy registry-client pull (EnableOCI=false
	// path: no auth/TLS surface, anonymous pulls only).
	ociPuller OCIPuller

	// chartCache memoizes parsed *chart.Chart by on-disk path. Helm's
	// loader.Load reparses the entire tgz on every call — for repos
	// where many HelmReleases share a base chart (e.g. bjw-s
	// app-template referenced by 30+ HRs), the same chart was being
	// re-parsed once per HR. The cache entry includes the file's
	// (mtime, size) at load time so a mutable OCI tag re-pushed under
	// the same name-version pair invalidates correctly — writeAtomic
	// overwrites the same path, but the new mtime trips the check
	// and the next LoadChart re-parses. Without this, every call to
	// LoadChart serves the stale in-memory *chart.Chart.
	//
	// chartLoadLocks serializes first-time loads per-path so N parallel
	// reconciles of the same chart issue exactly one loader.Load
	// (thundering-herd coalesce); the rest hit the populated cache.
	// Distinct paths still parse in parallel.
	chartMu        sync.Mutex
	chartCache     map[string]chartCacheEntry
	chartLoadLocks *keylock.KeyMap[string]
}

// chartCacheEntry pairs the parsed chart with the (mtime, size) of
// the on-disk tgz at load time. A mismatch on a subsequent lookup
// means the file was overwritten (mutable tag re-push, manual
// edit) and the cache entry is stale.
type chartCacheEntry struct {
	chart *chart.Chart
	mtime int64 // unix nanos
	size  int64
}

// NewClient constructs a Client. tmpDir and cacheDir are used for
// scratch chart downloads. Both will be created if absent.
func NewClient(tmpDir, cacheDir string) (*Client, error) {
	tmpDir = cmp.Or(tmpDir, filepath.Join(os.TempDir(), "flate-helm"))
	cacheDir = cmp.Or(cacheDir, filepath.Join(os.TempDir(), "flate-helm-cache"))
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return nil, err
	}
	reg, err := registry.NewClient(registry.ClientOptCredentialsFile(""))
	if err != nil {
		return nil, fmt.Errorf("helm registry: %w", err)
	}
	return &Client{
		tmpDir:         tmpDir,
		cacheDir:       cacheDir,
		registry:       reg,
		chartCache:     map[string]chartCacheEntry{},
		chartLoadLocks: keylock.New[string](),
	}, nil
}

// SetSecretGetter installs a Secret lookup function so HelmRepository
// SecretRef credentials can be resolved at pull time. Safe to call
// before any Add* — typically once at orchestrator construction.
func (c *Client) SetSecretGetter(g SecretGetter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secrets = g
}

// SetOCIPuller installs the OCI fetcher helm.Client routes
// HelmRepository(type=oci) and OCIRepository chart resolution
// through. The orchestrator wires source/oci.Fetcher when
// EnableOCI=true; embedders without one (or EnableOCI=false runs)
// leave it nil and helm.Client falls back to its built-in
// registry-client pull. Safe to call before any Template call.
func (c *Client) SetOCIPuller(p OCIPuller) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ociPuller = p
}

// ociPullerSnapshot returns the configured OCIPuller under a read
// lock. Returns nil when none was wired — callers fall back to the
// registry-client path.
func (c *Client) ociPullerSnapshot() OCIPuller {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ociPuller
}

// secretGetter returns the configured SecretGetter under a read lock —
// the snapshot helper helmRepoAuthOptions / helmRepoTLSOptions use so
// neither has to inline the RLock-copy-RUnlock dance.
func (c *Client) secretGetter() SecretGetter {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.secrets
}

// SetSourceResolver installs the canonical lookup surface for
// HelmRepository / OCIRepository / local-artifact sources. helm.Client
// reads through the resolver on every Template call — there's no
// alternate path. Safe to call before any template call; typically
// once at orchestrator construction.
func (c *Client) SetSourceResolver(r SourceResolver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolver = r
}

// Resolver returns the configured SourceResolver, or nil when none
// has been wired. Exposed so the HelmRelease controller (and embedders
// calling Prepare) can pass resolver.HelmChart into ResolveChartRef
// without holding a separate reference to the resolver.
func (c *Client) Resolver() SourceResolver {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.resolver
}

func (c *Client) resolveHelmRepo(hr *manifest.HelmRelease) *manifest.HelmRepository {
	r := c.Resolver()
	if r == nil {
		return nil
	}
	return r.HelmRepository(hr.Chart.RepoNamespace, hr.Chart.RepoName)
}

func (c *Client) resolveOCIRepo(hr *manifest.HelmRelease) *manifest.OCIRepository {
	r := c.Resolver()
	if r == nil {
		return nil
	}
	return r.OCIRepository(hr.Chart.RepoNamespace, hr.Chart.RepoName)
}

func (c *Client) resolveLocalSource(hr *manifest.HelmRelease) *store.SourceArtifact {
	r := c.Resolver()
	if r == nil {
		return nil
	}
	return r.LocalSourceArtifact(hr.Chart.RepoKind, hr.Chart.RepoNamespace, hr.Chart.RepoName)
}

// LocateChart returns a filesystem path to the chart referenced by hr.
// The caller is responsible for cleanup (chart paths inside the cache
// are reused across calls; paths inside the tmp dir are not).
func (c *Client) LocateChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	if hr == nil {
		return "", errors.New("nil HelmRelease")
	}
	switch hr.Chart.RepoKind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact:
		return c.locateLocalChart(hr)
	case manifest.KindOCIRepository:
		return c.locateOCIChart(ctx, hr)
	case manifest.KindHelmRepository, "":
		return c.locateHelmRepoChart(ctx, hr)
	}
	return "", fmt.Errorf("%w: unsupported chart repo kind %s", manifest.ErrInput, hr.Chart.RepoKind)
}

// LoadChart resolves and loads the chart into helm's in-memory model.
// Parsed *chart.Chart values are cached by path — Helm's loader.Load
// reparses the tgz (and recompiles values.schema.json) on every call,
// which is a significant render-time hot spot when many HelmReleases
// share a base chart (bjw-s app-template, podinfo, common-library, …).
// Path is content-addressed by Helm's own cacher (name-version-digest),
// so this is safe across reconciles.
func (c *Client) LoadChart(ctx context.Context, hr *manifest.HelmRelease) (ChartLoadResult, error) {
	path, err := c.LocateChart(ctx, hr)
	if err != nil {
		return ChartLoadResult{}, err
	}
	// Fast path: already parsed AND the file hasn't been rewritten
	// since (mtime+size match). Mutable OCI tags re-pushed under the
	// same name-version land via writeAtomic at the same path, so the
	// path is a stable string but the underlying bytes may have
	// changed; without the stat check we'd serve the stale chart.
	if ch, ok := c.lookupCachedChart(path); ok {
		return ChartLoadResult{Path: path, Chart: cloneChartForRender(ch)}, nil
	}

	// Coalesce parallel first-loads of the same chart so N concurrent
	// reconciles of the same base chart (bjw-s app-template referenced
	// by 30+ HRs, podinfo across multiple test envs) issue exactly one
	// loader.Load instead of N. Distinct paths still parse in parallel.
	release, err := c.chartLoadLocks.Acquire(ctx, path)
	if err != nil {
		return ChartLoadResult{}, err
	}
	defer release()

	// Re-check under the per-path lock — another goroutine may have
	// populated the cache while we waited.
	if ch, ok := c.lookupCachedChart(path); ok {
		return ChartLoadResult{Path: path, Chart: cloneChartForRender(ch)}, nil
	}

	ch, err := loader.Load(path)
	if err != nil {
		// A truncated/corrupt chart tgz left on disk (process killed
		// mid-download, fs fault, manual delete-then-recreate) would
		// otherwise stay sticky-broken — `os.Stat`-based cache-hit
		// checks in LocateChart see the file, return its path, and we
		// re-error here on every subsequent run. Removing the file
		// lets the next reconcile re-pull cleanly.
		_ = os.Remove(path)
		return ChartLoadResult{}, fmt.Errorf("load chart %s: %w", path, err)
	}
	if mtime, size, ok := chartCacheFingerprint(path); ok {
		c.chartMu.Lock()
		c.chartCache[path] = chartCacheEntry{chart: ch, mtime: mtime, size: size}
		c.chartMu.Unlock()
	}
	// Hand the caller a clone — helm's Install.RunWithContext invokes
	// chartutil.ProcessDependencies which mutates Chart.Values and the
	// per-Dependency Enabled flags. Sharing the cached pointer races
	// across concurrent renders.
	return ChartLoadResult{Path: path, Chart: cloneChartForRender(ch)}, nil
}

// cloneChartForRender returns a shallow copy of src with the fields
// chartutil.ProcessDependencies mutates (Values, Metadata.Dependencies,
// and recursively each subchart's same) cloned per call. Immutable
// fields (Templates, Raw, Files, Schema, ModTime, Lock) are aliased to
// the cached canonical to avoid copying potentially-MBs of template
// bytes per render.
//
// Mutations the helm template path performs that this guards:
//   - `c.Values = util.MergeTables/CoalesceTables(...)` rewrites
//     Values map on the chart and on every subchart
//     (pkg/chart/v2/util/dependencies.go:327,334).
//   - `processDependencyConditions` sets `dep.Enabled` on each entry
//     of `c.Metadata.Dependencies` (line 189).
//   - `processDependencyEnabled` rewrites the dependencies slice
//     itself (lines 223-224).
func cloneChartForRender(src *chart.Chart) *chart.Chart {
	if src == nil {
		return nil
	}
	out := *src
	if src.Metadata != nil {
		md := *src.Metadata
		if len(src.Metadata.Dependencies) > 0 {
			md.Dependencies = make([]*chart.Dependency, len(src.Metadata.Dependencies))
			for i, d := range src.Metadata.Dependencies {
				if d == nil {
					continue
				}
				dc := *d
				md.Dependencies[i] = &dc
			}
		}
		out.Metadata = &md
	}
	out.Values = cloneValuesMap(src.Values)
	if subs := src.Dependencies(); len(subs) > 0 {
		clones := make([]*chart.Chart, 0, len(subs))
		for _, sub := range subs {
			clones = append(clones, cloneChartForRender(sub))
		}
		out.SetDependencies(clones...)
	}
	return &out
}

// cloneValuesMap deep-copies a chart.Values map (`map[string]any`).
// Mirrors pkg/manifest.DeepCopyMap but kept local to avoid widening
// the helm→manifest import seam for one helper.
func cloneValuesMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = cloneValuesNode(v)
	}
	return out
}

func cloneValuesNode(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneValuesMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneValuesNode(e)
		}
		return out
	default:
		return v
	}
}

// lookupCachedChart returns the cached chart only when the on-disk
// file's (mtime, size) still match the values captured at load time.
// A mismatch (mutable OCI tag re-pushed, manual edit) returns false
// so the caller re-parses. A missing or unstattable path also
// returns false — the caller will surface that via loader.Load.
func (c *Client) lookupCachedChart(path string) (*chart.Chart, bool) {
	c.chartMu.Lock()
	entry, ok := c.chartCache[path]
	c.chartMu.Unlock()
	if !ok {
		return nil, false
	}
	mtime, size, ok := chartCacheFingerprint(path)
	if !ok {
		return nil, false
	}
	if mtime != entry.mtime || size != entry.size {
		return nil, false
	}
	return entry.chart, true
}

// chartCacheFingerprint returns the (mtime, size) tuple flate uses
// to detect chart-content changes under a stable path. For a tgz
// chart the file's own stat is the source of truth; for a directory
// chart the directory mtime is filesystem-dependent and doesn't move
// when sub-files change, so stat Chart.yaml — the file loader.Load
// actually parses to derive the chart identity — instead. Returns
// ok=false when neither is reachable.
func chartCacheFingerprint(path string) (mtime, size int64, ok bool) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, false
	}
	if !info.IsDir() {
		return info.ModTime().UnixNano(), info.Size(), true
	}
	cy, err := os.Stat(filepath.Join(path, "Chart.yaml"))
	if err != nil {
		return 0, 0, false
	}
	return cy.ModTime().UnixNano(), cy.Size(), true
}
