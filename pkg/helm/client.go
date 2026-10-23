package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"sync"

	chart "helm.sh/helm/v4/pkg/chart/v2"

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

	// Canonical charts MUST remain immutable; renders clone at the mutation boundary.
	chartMu        sync.RWMutex
	chartCache     map[string]chartDescription
	chartLoadLocks *keylock.KeyMap[string]

	// Cached values MUST remain immutable; chartMu guards lookup and publication.
	chartValuesCache map[string]*chartValuesEntry

	// valuesCache memoizes parsed-YAML output of ExpandValueReferences
	// across HRs in this Client's lifetime. One HR with 10 valuesFrom
	// refs hits each entry once; M HRs sharing a platform-wide values
	// CM re-yaml.Unmarshal'd the same bytes M times without this.
	// Lives for the Client lifetime — re-creating per Template call
	// would defeat the cross-HR sharing that delivers the win.
	valuesCache *values.Cache

	cache *cache.Cache
}

// NewClient constructs a Client borrowing the supplied nonnil cache owner.
// The caller MUST keep the owner open until all Client operations finish.
func NewClient(owner *cache.Cache) (*Client, error) {
	return &Client{
		chartCache:       map[string]chartDescription{},
		chartValuesCache: map[string]*chartValuesEntry{},
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

type chartDescription struct {
	captured      capturedChart
	key, location string
	analysis      *chartAnalysis
	record        *renderRecord
	values        chartValuesEntry
	valuesErr     error
	chart         *chart.Chart
	fingerprint   string
	oci           *ociChartContent
	artifact      store.SourceArtifact
	valuesFS      fs.FS
	valuesPin     string
}

func (c *Client) describeChart(ctx context.Context, hr *manifest.HelmRelease) (chartDescription, error) {
	if hr == nil {
		return chartDescription{}, errors.New("nil HelmRelease")
	}
	switch hr.Chart.Source.Kind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact,
		manifest.KindOCIRepository, manifest.KindHelmChart:
	case manifest.KindHelmRepository, "":
		return chartDescription{}, fmt.Errorf("%w: HelmRepository chart %s reached chart loading unmaterialized: render via the orchestrator",
			manifest.ErrInput, hr.Chart.RepoFullName())
	default:
		return chartDescription{}, fmt.Errorf("%w: unsupported chart repo kind %s", manifest.ErrInput, hr.Chart.Source.Kind)
	}
	var art *store.SourceArtifact
	if r := c.Resolver(); r != nil {
		art = r.LocalSourceArtifact(hr.ChartSource())
	}
	if art == nil || art.FS == nil {
		return chartDescription{}, fmt.Errorf("%w: %s %s artifact content unavailable for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.Source.Kind, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}
	input, pin, err := source.OperationView(ctx, art.FS)
	if err != nil {
		return chartDescription{}, err
	}
	selector := struct {
		Pin, Path string
		Auto      bool
	}{Pin: pin, Auto: true}
	switch hr.Chart.Source.Kind {
	case manifest.KindBucket, manifest.KindGitRepository, manifest.KindExternalArtifact:
		selector.Path, selector.Auto = path.Clean(hr.Chart.Name), false
	}
	var captured capturedChart
	var location string
	capture := func() error {
		location = selector.Path
		if selector.Auto {
			location, err = chartPathInFS(input)
			if err != nil {
				return err
			}
		}
		label := location
		if art.LocalPath != "" {
			label = filepath.Join(art.LocalPath, filepath.FromSlash(location))
		}
		var missing bool
		captured, missing, err = captureChart(input, location)
		if missing {
			return err
		}
		if err != nil {
			return fmt.Errorf("load chart %s: %w", label, err)
		}
		return nil
	}
	var cacheKey string
	if pin != "" {
		cacheKey = manifest.Fingerprint(selector)
	} else {
		if err := capture(); err != nil {
			return chartDescription{}, err
		}
		cacheKey = captured.fingerprint()
	}
	c.chartMu.RLock()
	loaded, cached := c.chartCache[cacheKey]
	c.chartMu.RUnlock()
	if !cached {
		release, err := c.chartLoadLocks.Acquire(ctx, cacheKey)
		if err != nil {
			return chartDescription{}, err
		}
		defer release()
		c.chartMu.RLock()
		loaded, cached = c.chartCache[cacheKey]
		c.chartMu.RUnlock()
		if !cached {
			if pin != "" {
				if err := capture(); err != nil {
					return chartDescription{}, err
				}
			}
			fingerprint := cacheKey
			if pin != "" {
				fingerprint = captured.fingerprint()
			}
			loaded = chartDescription{captured: captured, fingerprint: fingerprint, key: cacheKey, location: location}
			loaded = c.rememberDescription(loaded, nil)
		}
	}
	if pin == "" {
		loaded.location = location
	}
	loaded.artifact = *art
	if hr.Chart.UsesSourceValuesFiles() {
		loaded.valuesFS, loaded.valuesPin = input, pin
	}
	return loaded, nil
}

func (c *Client) rememberDescription(d chartDescription, fresh *renderRecord) chartDescription {
	c.chartMu.Lock()
	if previous := c.chartCache[d.key]; previous.chart != nil {
		d.chart, d.analysis = previous.chart, previous.analysis
	} else if previous.analysis != nil {
		d.analysis = previous.analysis
	}
	if fresh != nil {
		if d.analysis == nil {
			d.analysis = fresh.Analysis
		}
		fresh.Analysis = d.analysis
		d.record = fresh
	} else {
		d.record = c.chartCache[d.key].record
	}
	if d.chart != nil {
		d.captured = capturedChart{}
	}
	c.chartCache[d.key] = chartDescription{captured: d.captured, fingerprint: d.fingerprint, key: d.key, location: d.location, chart: d.chart, analysis: d.analysis, record: d.record}
	c.chartMu.Unlock()
	return d
}

func (c *Client) decodeChart(ctx context.Context, d *chartDescription) error {
	if d.chart != nil {
		return nil
	}
	release, err := c.chartLoadLocks.Acquire(ctx, d.key)
	if err != nil {
		return err
	}
	defer release()
	c.chartMu.RLock()
	previous := c.chartCache[d.key]
	c.chartMu.RUnlock()
	if previous.chart != nil {
		d.chart, d.analysis = previous.chart, previous.analysis
		d.captured = capturedChart{}
		return nil
	}
	d.chart, err = d.captured.load()
	if err != nil {
		if _, ok := errors.AsType[chart.ValidationError](err); ok {
			err = fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		label := d.location
		if d.artifact.LocalPath != "" {
			label = filepath.Join(d.artifact.LocalPath, filepath.FromSlash(d.location))
		}
		return fmt.Errorf("load chart %s: %w", label, err)
	}
	*d = c.rememberDescription(*d, nil)
	return nil
}

func (d chartDescription) analyze() *chartAnalysis {
	if d.analysis != nil {
		return d.analysis
	}
	return &chartAnalysis{Templates: chartTemplateText(d.chart), Allowed: knownTopLevelKeys(d.chart), RequiresLoader: requiresChartLoader(d.chart)}
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
