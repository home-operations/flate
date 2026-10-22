package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
	// Canonical charts MUST remain immutable; renders clone at the mutation boundary.
	chartMu        sync.RWMutex
	chartCache     map[string]chartDescription
	chartLoadLocks *keylock.KeyMap[string]

	// Cached values MUST remain immutable; chartMu guards lookup and publication.
	chartValuesCache map[string]*chartValuesEntry

	valuesCache values.Cache

	cache *cache.Cache
}

// NewClient constructs a Client borrowing the supplied nonnil cache owner.
// The caller MUST keep the owner open until all Client operations finish.
func NewClient(owner *cache.Cache) *Client {
	return &Client{
		chartCache:       map[string]chartDescription{},
		chartValuesCache: map[string]*chartValuesEntry{},
		chartLoadLocks:   keylock.New[string](),
		cache:            owner,
	}
}

// ValuesCache returns the Client's shared valuesFrom parse cache.
func (c *Client) ValuesCache() *values.Cache { return &c.valuesCache }

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

func (c *Client) describeChart(ctx context.Context, hr *manifest.HelmRelease, sources Sources) (chartDescription, error) {
	if hr == nil {
		return chartDescription{}, errors.New("nil HelmRelease")
	}
	location, auto, err := hr.Chart.ChartLocation()
	if err != nil {
		return chartDescription{}, err
	}
	art := sources.LocalSourceArtifact(hr.ChartSource())
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
	}{Pin: pin, Path: location, Auto: auto}
	var captured capturedChart
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
