package helm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// SecretGetter is the same shape as source.SecretGetter; aliased so
// the helm Client and the source Fetchers consume one canonical type.
// The orchestrator wires the same closure into both.
type SecretGetter = source.SecretGetter

// Client renders HelmReleases. Construct with NewClient.
type Client struct {
	mu sync.RWMutex
	// resolver is the canonical (and only) source-lookup surface.
	// Embedders MUST call SetSourceResolver before any Template call;
	// the orchestrator wires NewStoreSourceResolver(store) at
	// construction.
	resolver SourceResolver

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
	chartMu        sync.RWMutex
	chartCache     map[string]chartCacheEntry
	chartLoadLocks *keylock.KeyMap[string]

	// chartValuesCache memoizes mergeChartValuesFiles output keyed by
	// (chart name + version + joined valuesFiles list). Multiple HRs
	// sharing a base chart and the same spec.chart.spec.valuesFiles
	// stack (common: bjw-s app-template with a fixed set of layered
	// values-*.yaml files) re-yaml.Unmarshal'd the same bytes once per
	// HR. The cache holds the canonical merged map; callers receive
	// a deep clone (defensive-copy convention shared with chartCache)
	// because downstream DeepMerge layering mutates the result.
	//
	// Guarded by chartMu — same lock as chartCache. The two caches
	// share a lifecycle (both live for the duration of the Client)
	// and a relatively low write rate (one entry per unique
	// chart+valuesFiles tuple per orchestrator run); a separate
	// mutex would just add coordination overhead with no contention
	// reduction.
	chartValuesCache map[string]map[string]any

	// valuesCache memoizes parsed-YAML output of ExpandValueReferences
	// across HRs in this Client's lifetime. One HR with 10 valuesFrom
	// refs hits each entry once; M HRs sharing a platform-wide values
	// CM re-yaml.Unmarshal'd the same bytes M times without this.
	// Lives for the Client lifetime — re-creating per Template call
	// would defeat the cross-HR sharing that delivers the win.
	valuesCache *values.Cache

	// templateCache memoizes Template's rendered manifest output keyed
	// by computeTemplateKey (chart fingerprint + resolved values +
	// render options + HR action.Install fields). action.Install.RunWithContext
	// is the single largest CPU + allocation consumer in the codebase
	// (template.go cites ~300 MB on a 200-HR run); repeat HRs with the
	// same effective inputs hit this cache and skip the call entirely.
	//
	// nil when disabled (NewClientWithOptions called with
	// TemplateCacheBytes<=0). Both Get and Put handle nil receivers
	// cleanly so the render path doesn't need extra wiring guards.
	templateCache *templateCache
}

// chartCacheEntry pairs the parsed chart with the (mtime, size) of
// the on-disk tgz at load time. A mismatch on a subsequent lookup
// means the file was overwritten (mutable tag re-push, manual
// edit) and the cache entry is stale.
//
// fingerprint, when non-empty, is the content-addressed digest of
// the chart's loader.Load inputs — computed once at cache-fill
// time and reused on every subsequent LoadChart hit. The template-
// output cache mixes it into its own key so a stale chart never
// serves a different chart's render.
type chartCacheEntry struct {
	chart       *chart.Chart
	mtime       int64 // unix nanos
	size        int64
	fingerprint string
}

// ClientOptions tunes NewClientWithOptions construction. Callers
// that want historical defaults pass DefaultClientOptions();
// embedders that want to disable specific caches (memory-constrained
// CI, embedders that prefer their own caching layer) build the
// struct field-by-field.
type ClientOptions struct {
	// TemplateCacheBytes caps the in-memory helm-template-output
	// cache. <= 0 disables the cache. Use DefaultTemplateCacheBytes
	// for the project-wide default.
	TemplateCacheBytes int64

	// RenderCacheBytes caps the persistent on-disk helm template-
	// output cache. <= 0 disables disk caching. Disk
	// caching is also disabled when RenderCacheRoot is empty even
	// if RenderCacheBytes > 0 — the wiring requires both.
	RenderCacheBytes int64

	// RenderCacheRoot is the on-disk directory the persistent
	// template-output cache writes into (typically
	// layout.RenderHelmCache()). Empty disables disk caching even
	// when RenderCacheBytes > 0.
	RenderCacheRoot string
}

// DefaultTemplateCacheBytes is the historical default size of the
// helm template-output cache: 256 MiB. The CLI surfaces this
// through the --helm-template-cache-mb flag (256 by default; 0
// disables).
const DefaultTemplateCacheBytes int64 = 256 << 20

// DefaultRenderCacheBytes is the default size of the persistent
// helm template-output cache: 1 GiB. The CLI surfaces this through
// the --helm-render-cache-mb flag (1024 by default; 0 disables).
const DefaultRenderCacheBytes int64 = 1024 << 20

// DefaultClientOptions returns the ClientOptions a vanilla NewClient
// call uses — historical project defaults baked in. Disk caching is
// off by default at the constructor level: a vanilla NewClient
// caller hasn't provided a cache root, so we can't safely choose
// one for them. Callers wanting disk caching populate
// RenderCacheRoot explicitly (typically from layout.RenderHelmCache()).
func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		TemplateCacheBytes: DefaultTemplateCacheBytes,
	}
}

// NewClient constructs a Client backed by the supplied Layout with
// project-wide defaults (DefaultClientOptions). helm.Client reads
// already-fetched charts off disk and composes no cache paths of its own —
// chart bytes live in the shared source.Cache (the content-addressed
// <root>/blobs/sha256/ store, so identical chart bytes dedup across
// HelmRepositories and embedders), populated by the source controller.
//
// Disk-backed template-output cache defaults to enabled at
// DefaultRenderCacheBytes, rooted at layout.RenderHelmCache(). When
// layout.Root is empty (an embedder explicitly opted out of any
// persistent cache) the disk layer auto-disables so we don't write
// into the OS tempdir under a synthesised "flate-cache" subdir.
//
// Embedders that need to tune cache sizing (TemplateCacheBytes, …)
// should call NewClientWithOptions instead.
func NewClient(layout cacheroot.Layout) (*Client, error) {
	opts := DefaultClientOptions()
	if layout.Root != "" {
		opts.RenderCacheBytes = DefaultRenderCacheBytes
		opts.RenderCacheRoot = layout.RenderHelmCache()
	}
	return NewClientWithOptions(layout, opts)
}

// NewClientWithOptions is NewClient with explicit ClientOptions.
// Pass DefaultClientOptions() to get historical defaults; build the
// struct manually to disable individual caches (e.g.
// TemplateCacheBytes=0 for the template-output cache).
func NewClientWithOptions(layout cacheroot.Layout, opts ClientOptions) (*Client, error) {
	if layout.Root == "" {
		// Embedders that don't wire a cache root still need a working
		// client. Anchor under the OS tempdir so the legacy default
		// keeps working.
		layout.Root = filepath.Join(os.TempDir(), "flate-cache")
	}
	return &Client{
		chartCache:       map[string]chartCacheEntry{},
		chartValuesCache: map[string]map[string]any{},
		chartLoadLocks:   keylock.New[string](),
		valuesCache:      values.NewCache(),
		templateCache: newTemplateCache(
			opts.TemplateCacheBytes,
			newDiskRenderCache(opts.RenderCacheRoot, opts.RenderCacheBytes),
		),
	}, nil
}

// ValuesCache returns the Client's per-orchestrator valuesFrom parse
// cache so callers (helm.Prepare, the HR controller) can pass it
// through to values.ExpandValueReferences. Always non-nil for
// Clients constructed via NewClient.
func (c *Client) ValuesCache() *values.Cache { return c.valuesCache }

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
func (c *Client) LocateChart(hr *manifest.HelmRelease) (string, error) {
	if hr == nil {
		return "", errors.New("nil HelmRelease")
	}
	switch hr.Chart.RepoKind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact:
		return c.locateLocalChart(hr)
	case manifest.KindOCIRepository:
		return c.locateOCIChart(hr)
	case manifest.KindHelmChart:
		return c.locateHelmChart(hr)
	case manifest.KindHelmRepository, "":
		// HelmRepository charts are materialized into a synthetic HelmChart
		// by the orchestrator's HR controller before render (see
		// materializeHelmChartSource), reaching LocateChart as KindHelmChart
		// above. Hitting this case means LocateChart ran on an
		// un-materialized HelmRepository chart — unsupported; embedders must
		// render through the orchestrator (or pre-synthesize a HelmChart).
		return "", fmt.Errorf("%w: HelmRepository chart %s reached LocateChart unmaterialized — render via the orchestrator",
			manifest.ErrInput, hr.Chart.RepoFullName())
	}
	return "", fmt.Errorf("%w: unsupported chart repo kind %s", manifest.ErrInput, hr.Chart.RepoKind)
}

// LoadChart resolves and loads the chart into helm's in-memory model.
// Parsed *chart.Chart values are cached by path — Helm's loader.Load
// reparses the tgz (and recompiles values.schema.json) on every call,
// which is a significant render-time hot spot when many HelmReleases
// share a base chart (bjw-s app-template, podinfo, common-library, …).
// Path comes from the source cache (a content-addressed blob for HTTP
// tarballs, a ref-keyed slot for OCI) and is stable across reconciles; the
// per-path (mtime,size) re-check below guards a mutable OCI re-push.
func (c *Client) LoadChart(ctx context.Context, hr *manifest.HelmRelease) (ChartLoadResult, error) {
	path, err := c.LocateChart(hr)
	if err != nil {
		return ChartLoadResult{}, err
	}
	// Fast path: already parsed AND the file hasn't been rewritten
	// since (mtime+size match). Mutable OCI tags re-pushed under the
	// same name-version land via writeAtomic at the same path, so the
	// path is a stable string but the underlying bytes may have
	// changed; without the stat check we'd serve the stale chart.
	if ch, fp, ok := c.lookupCachedChart(path); ok {
		return ChartLoadResult{Path: path, Chart: cloneChartForRender(ch), Fingerprint: fp}, nil
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
	if ch, fp, ok := c.lookupCachedChart(path); ok {
		return ChartLoadResult{Path: path, Chart: cloneChartForRender(ch), Fingerprint: fp}, nil
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
	// Compute the chart fingerprint once per cache-fill so every
	// subsequent Template call against this path participates in the
	// template-output cache without re-walking the chart. Skipped
	// when the template cache is disabled to avoid the (cheap but
	// nonzero) digest cost for embedders that opted out.
	var fingerprint string
	if c.templateCache != nil {
		fingerprint = chartFingerprint(ch)
	}
	if mtime, size, ok := chartCacheFingerprint(path); ok {
		c.chartMu.Lock()
		c.chartCache[path] = chartCacheEntry{
			chart:       ch,
			mtime:       mtime,
			size:        size,
			fingerprint: fingerprint,
		}
		c.chartMu.Unlock()
	}
	// Hand the caller a clone — helm's Install.RunWithContext invokes
	// chartutil.ProcessDependencies which mutates Chart.Values and the
	// per-Dependency Enabled flags. Sharing the cached pointer races
	// across concurrent renders.
	return ChartLoadResult{Path: path, Chart: cloneChartForRender(ch), Fingerprint: fingerprint}, nil
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
	out.Values = manifest.DeepCopyMap(src.Values)
	if subs := src.Dependencies(); len(subs) > 0 {
		clones := make([]*chart.Chart, 0, len(subs))
		for _, sub := range subs {
			clones = append(clones, cloneChartForRender(sub))
		}
		out.SetDependencies(clones...)
	}
	return &out
}

// lookupCachedChart returns the cached chart only when the on-disk
// file's (mtime, size) still match the values captured at load time.
// A mismatch (mutable OCI tag re-pushed, manual edit) returns false
// so the caller re-parses. A missing or unstattable path also
// returns false — the caller will surface that via loader.Load.
//
// The second return is the chart's content-addressed fingerprint
// (computed once at cache fill); the template-output cache mixes
// it into its own key so a stale chart never serves a different
// chart's render. Empty when the template cache is disabled.
func (c *Client) lookupCachedChart(path string) (*chart.Chart, string, bool) {
	c.chartMu.RLock()
	entry, ok := c.chartCache[path]
	c.chartMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	mtime, size, ok := chartCacheFingerprint(path)
	if !ok {
		return nil, "", false
	}
	if mtime != entry.mtime || size != entry.size {
		return nil, "", false
	}
	return entry.chart, entry.fingerprint, true
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
	cy, err := os.Stat(filepath.Join(path, chartYamlFilename))
	if err != nil {
		return 0, 0, false
	}
	return cy.ModTime().UnixNano(), cy.Size(), true
}
