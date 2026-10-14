package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/util"

	"github.com/home-operations/flate/internal/diskcache"
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

	// Charts are shared by immutable content pin or mutable origin coordinates.
	// Existing per-key locks coalesce loads; each caller receives a private clone.
	chartMu        sync.RWMutex
	chartCache     map[string]chartCacheEntry
	chartLoadLocks *keylock.KeyMap[string]

	// chartValuesCache memoizes mergeChartValuesFiles output keyed by
	// (loaded fingerprint, ordered valuesFiles, missing-file policy). Multiple HRs
	// sharing a base chart and the same spec.chart.spec.valuesFiles
	// stack (common: bjw-s app-template with a fixed set of layered
	// values-*.yaml files) re-yaml.Unmarshal'd the same bytes once per
	// HR. The cache holds the canonical merged map; callers receive
	// a deep clone (defensive-copy convention shared with chartCache)
	// so callers cannot mutate the canonical merged map.
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

// Mutable chart entries retain the archive or Chart.yaml (mtime, size) validity
// contract. Immutable entries reuse their pin; fingerprints key rendered output.
type chartCacheEntry struct {
	immutable   bool
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
// already-fetched charts through FS and composes no source cache paths of its own:
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
			diskcache.NewStore(opts.RenderCacheRoot, opts.RenderCacheBytes),
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

func (c *Client) resolveLocalSource(hr *manifest.HelmRelease) *store.SourceArtifact {
	r := c.Resolver()
	if r == nil {
		return nil
	}
	return r.LocalSourceArtifact(hr.Chart.RepoKind, hr.Chart.RepoNamespace, hr.Chart.RepoName)
}

// LoadChart caches immutable charts by content pin and mutable charts by origin.
func (c *Client) LoadChart(ctx context.Context, hr *manifest.HelmRelease) (ChartLoadResult, error) {
	if hr == nil {
		return ChartLoadResult{}, errors.New("nil HelmRelease")
	}
	switch hr.Chart.RepoKind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact,
		manifest.KindOCIRepository, manifest.KindHelmChart:
	case manifest.KindHelmRepository, "":
		return ChartLoadResult{}, fmt.Errorf("%w: HelmRepository chart %s reached LoadChart unmaterialized: render via the orchestrator",
			manifest.ErrInput, hr.Chart.RepoFullName())
	default:
		return ChartLoadResult{}, fmt.Errorf("%w: unsupported chart repo kind %s", manifest.ErrInput, hr.Chart.RepoKind)
	}
	art := c.resolveLocalSource(hr)
	if art == nil || art.FS == nil {
		return ChartLoadResult{}, fmt.Errorf("%w: %s %s artifact content unavailable for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoKind, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}
	input, _, err := source.ReadView(ctx, art.FS, false)
	if err != nil {
		return ChartLoadResult{}, err
	}
	var location string
	switch hr.Chart.RepoKind {
	case manifest.KindBucket, manifest.KindGitRepository, manifest.KindExternalArtifact:
		location = path.Clean(hr.Chart.Name)
	default:
		location, err = chartPathInFS(input)
	}
	if err != nil {
		return ChartLoadResult{}, err
	}
	cacheKey := ""
	if art.Key != "" {
		cacheKey = "pin\x00" + art.Key + "\x00" + location
	} else if art.LocalPath != "" {
		cacheKey = "local\x00" + art.LocalRoot + "\x00" + art.LocalPath + "\x00" + location
	}
	label := location
	if art.LocalPath != "" {
		label = filepath.Join(art.LocalPath, filepath.FromSlash(location))
	}
	needsFingerprint := c.templateCache != nil || len(hr.ChartValuesFiles) > 0
	if ch, fp, ok := c.lookupCachedChart(cacheKey, input, location); ok && (!needsFingerprint || fp != "") {
		return ChartLoadResult{Path: label, Chart: cloneChartForRender(ch), Fingerprint: fp}, nil
	}
	if cacheKey != "" {
		release, err := c.chartLoadLocks.Acquire(ctx, cacheKey)
		if err != nil {
			return ChartLoadResult{}, err
		}
		defer release()
	}
	// Re-check validity under the existing load lock, including fingerprint fill.
	ch, fp, cached := c.lookupCachedChart(cacheKey, input, location)
	if cached && (!needsFingerprint || fp != "") {
		return ChartLoadResult{Path: label, Chart: cloneChartForRender(ch), Fingerprint: fp}, nil
	}
	if !cached {
		var missing bool
		ch, missing, err = loadFSChart(input, location)
		if missing {
			return ChartLoadResult{}, err
		}
	}
	if err != nil {
		if _, ok := errors.AsType[chart.ValidationError](err); ok {
			err = fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		return ChartLoadResult{}, fmt.Errorf("load chart %s: %w", label, err)
	}
	var fingerprint string
	if needsFingerprint {
		fingerprint = chartFingerprint(ch)
	}
	mtime, size, valid := int64(0), int64(0), art.Key != ""
	if !valid {
		mtime, size, valid = chartCacheFingerprint(input, location)
	}
	if cacheKey != "" && valid {
		c.chartMu.Lock()
		c.chartCache[cacheKey] = chartCacheEntry{
			immutable: art.Key != "", chart: ch, mtime: mtime, size: size,
			fingerprint: fingerprint,
		}
		c.chartMu.Unlock()
	}
	return ChartLoadResult{Path: label, Chart: cloneChartForRender(ch), Fingerprint: fingerprint}, nil
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

// Mutable lookups validate the current FS at the chart's relative location.
func (c *Client) lookupCachedChart(key string, input fs.FS, location string) (*chart.Chart, string, bool) {
	c.chartMu.RLock()
	entry, ok := c.chartCache[key]
	c.chartMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	if entry.immutable {
		return entry.chart, entry.fingerprint, true
	}
	mtime, size, ok := chartCacheFingerprint(input, location)
	if !ok {
		return nil, "", false
	}
	if mtime != entry.mtime || size != entry.size {
		return nil, "", false
	}
	return entry.chart, entry.fingerprint, true
}

// Directory charts retain Chart.yaml validity; template-only changes are outside
// this metadata contract. Archives use their own metadata.
func chartCacheFingerprint(input fs.FS, location string) (mtime, size int64, ok bool) {
	info, err := fs.Stat(input, location)
	if err != nil {
		return 0, 0, false
	}
	if !info.IsDir() {
		return info.ModTime().UnixNano(), info.Size(), true
	}
	cy, err := fs.Stat(input, path.Join(location, util.ChartfileName))
	if err != nil {
		return 0, 0, false
	}
	return cy.ModTime().UnixNano(), cy.Size(), true
}
