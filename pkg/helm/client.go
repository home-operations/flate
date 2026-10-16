package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"sync"

	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
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

	// Charts are shared by acquisition pin or captured directory content.
	// Existing per-key locks coalesce parsing; renders clone at the mutation boundary.
	chartMu        sync.RWMutex
	chartCache     map[string]chartCacheEntry
	chartLoadLocks *keylock.KeyMap[string]

	// chartValuesCache memoizes mergeChartValuesFiles output keyed by
	// (loaded fingerprint, ordered valuesFiles, missing-file policy). Multiple HRs
	// sharing a base chart and the same spec.chart.spec.valuesFiles
	// stack (common: bjw-s app-template with a fixed set of layered
	// values-*.yaml files) re-yaml.Unmarshal'd the same bytes once per
	// HR. The cache holds the canonical merged map; callers receive
	// a deep clone so callers cannot mutate the canonical merged map.
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

	cache *cache.Cache
}

// Only mutable archive entries need stat validation; directories use captured content.
type chartCacheEntry struct {
	immutable   bool
	chart       *chart.Chart
	mtime       int64 // unix nanos
	size        int64
	fingerprint string
}

// NewClient constructs a Client borrowing the supplied nonnil cache owner.
// The caller MUST keep the owner open until all Client operations finish.
func NewClient(owner *cache.Cache) (*Client, error) {
	return &Client{
		chartCache:       map[string]chartCacheEntry{},
		chartValuesCache: map[string]map[string]any{},
		chartLoadLocks:   keylock.New[string](),
		valuesCache:      values.NewCache(),
		cache:            owner,
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
	return r.LocalSourceArtifact(hr.ChartSource())
}

type chartLoadResult struct {
	chart       *chart.Chart
	fingerprint string
	artifact    store.SourceArtifact
}

// loadChart returns the canonical immutable chart after releasing its load lock.
func (c *Client) loadChart(ctx context.Context, hr *manifest.HelmRelease) (chartLoadResult, error) {
	if hr == nil {
		return chartLoadResult{}, errors.New("nil HelmRelease")
	}
	switch hr.Chart.RepoKind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact,
		manifest.KindOCIRepository, manifest.KindHelmChart:
	case manifest.KindHelmRepository, "":
		return chartLoadResult{}, fmt.Errorf("%w: HelmRepository chart %s reached chart loading unmaterialized: render via the orchestrator",
			manifest.ErrInput, hr.Chart.RepoFullName())
	default:
		return chartLoadResult{}, fmt.Errorf("%w: unsupported chart repo kind %s", manifest.ErrInput, hr.Chart.RepoKind)
	}
	art := c.resolveLocalSource(hr)
	if art == nil || art.FS == nil {
		return chartLoadResult{}, fmt.Errorf("%w: %s %s artifact content unavailable for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoKind, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}
	input, pin, err := source.ReadView(ctx, art.FS, false)
	if err != nil {
		return chartLoadResult{}, err
	}
	var location string
	switch hr.Chart.RepoKind {
	case manifest.KindBucket, manifest.KindGitRepository, manifest.KindExternalArtifact:
		location = path.Clean(hr.Chart.Name)
	default:
		location, err = chartPathInFS(input)
	}
	if err != nil {
		return chartLoadResult{}, err
	}
	admitted := *art
	admitted.FS, admitted.Key = input, pin
	label := location
	if art.LocalPath != "" {
		label = filepath.Join(art.LocalPath, filepath.FromSlash(location))
	}
	var files []*archive.BufferedFile
	var fingerprint string
	cacheKey := ""
	pinned := pin != ""
	if !pinned {
		var missing bool
		files, fingerprint, missing, err = captureChartDir(admitted.FS, location)
		if missing {
			return chartLoadResult{}, err
		}
		if err != nil {
			return chartLoadResult{}, fmt.Errorf("load chart %s: %w", label, err)
		}
		if files != nil {
			cacheKey = fingerprint
		} else if art.LocalPath != "" {
			cacheKey = manifest.Fingerprint([3]string{art.LocalRoot, art.LocalPath, location})
		}
	} else {
		cacheKey = manifest.Fingerprint([2]string{admitted.Key, location})
	}
	if ch, fp, ok := c.lookupCachedChart(cacheKey, admitted.FS, location); ok {
		return chartLoadResult{chart: ch, fingerprint: fp, artifact: *art}, nil
	}
	if cacheKey != "" {
		release, err := c.chartLoadLocks.Acquire(ctx, cacheKey)
		if err != nil {
			return chartLoadResult{}, err
		}
		defer release()
	}
	if ch, fp, ok := c.lookupCachedChart(cacheKey, admitted.FS, location); ok {
		return chartLoadResult{chart: ch, fingerprint: fp, artifact: *art}, nil
	}
	var ch *chart.Chart
	if files != nil {
		ch, err = loader.LoadFiles(files)
	} else {
		var missing bool
		ch, fingerprint, missing, err = loadFSChart(admitted.FS, location)
		if missing {
			return chartLoadResult{}, err
		}
	}
	if err != nil {
		if _, ok := errors.AsType[chart.ValidationError](err); ok {
			err = fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		return chartLoadResult{}, fmt.Errorf("load chart %s: %w", label, err)
	}
	immutable := pinned || files != nil
	mtime, size, valid := int64(0), int64(0), immutable
	if !valid {
		mtime, size, valid = archiveCacheFingerprint(admitted.FS, location)
	}
	if cacheKey != "" && valid {
		c.chartMu.Lock()
		c.chartCache[cacheKey] = chartCacheEntry{immutable: immutable, chart: ch, mtime: mtime, size: size, fingerprint: fingerprint}
		c.chartMu.Unlock()
	}
	return chartLoadResult{chart: ch, fingerprint: fingerprint, artifact: *art}, nil
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
	mtime, size, ok := archiveCacheFingerprint(input, location)
	if !ok {
		return nil, "", false
	}
	if mtime != entry.mtime || size != entry.size {
		return nil, "", false
	}
	return entry.chart, entry.fingerprint, true
}

func archiveCacheFingerprint(input fs.FS, location string) (mtime, size int64, ok bool) {
	info, err := fs.Stat(input, location)
	if err != nil || info.IsDir() {
		return 0, 0, false
	}
	return info.ModTime().UnixNano(), info.Size(), true
}
