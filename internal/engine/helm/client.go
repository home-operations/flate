package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/core/keylock"
	"github.com/home-operations/flate/internal/engine/helm/chartinput"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/helmchart"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

// Client renders HelmReleases. Construct with NewClient.
type Client struct {
	// Canonical charts MUST remain immutable; renders clone at the mutation boundary.
	chartMu        sync.RWMutex
	chartCache     map[string]chartDescription
	chartLoadLocks *keylock.KeyMap[string]

	// Cached values MUST remain immutable; chartMu guards lookup and publication.
	chartValuesCache map[string]*chartValuesEntry

	valuesCache values.Cache

	cache   *cache.Cache
	fetcher *helmchart.Fetcher
}

// NewClient callers MUST keep the cache and fetcher open until all client operations finish.
func NewClient(owner *cache.Cache, fetcher *helmchart.Fetcher) *Client {
	return &Client{
		chartCache:       map[string]chartDescription{},
		chartValuesCache: map[string]*chartValuesEntry{},
		chartLoadLocks:   keylock.New[string](),
		cache:            owner,
		fetcher:          fetcher,
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
	remote        bool
	needsBuild    bool
}

func (c *Client) describeChart(
	ctx context.Context,
	hr *manifest.HelmRelease,
	art *store.SourceArtifact,
) (chartDescription, error) {
	if hr == nil {
		return chartDescription{}, errors.New("nil HelmRelease")
	}
	location, auto, err := hr.Chart.ChartLocation()
	if err != nil {
		return chartDescription{}, err
	}
	if art == nil || art.FS == nil {
		return chartDescription{}, manifest.NewError(
			manifest.ErrObjectNotFound,
			nil,
			fmt.Sprintf("%s %s artifact content unavailable", hr.Chart.Source.Kind, hr.Chart.RepoFullName()),
		)
	}
	input, pin, err := source.OperationView(ctx, art.FS)
	if err != nil {
		return chartDescription{}, err
	}
	selector := chartSelector{Pin: pin, Path: location, Auto: auto}
	loaded, err := c.describeInput(ctx, input, selector, art.OriginPath(art.LocalPath))
	if err != nil {
		return chartDescription{}, err
	}
	loaded.artifact = *art
	if hr.Chart.UsesSourceValuesFiles() {
		loaded.valuesFS, loaded.valuesPin = input, pin
	}
	return loaded, nil
}

func (c *Client) describeInput(
	ctx context.Context,
	input fs.FS,
	selector chartSelector,
	root string,
) (chartDescription, error) {
	location := selector.Path
	var err error
	var captured capturedChart
	var fingerprint string
	var freshBuild bool
	capture := func() error {
		captured, location, err = selector.capture(ctx, input, root)
		if err == nil {
			fingerprint = captured.fingerprint()
		}
		return err
	}
	var cacheKey string
	if selector.Pin != "" {
		cacheKey = manifest.Fingerprint(selector)
	} else {
		if captureErr := capture(); captureErr != nil {
			return chartDescription{}, captureErr
		}
		cacheKey = fingerprint
	}
	c.chartMu.RLock()
	loaded, cached := c.chartCache[cacheKey]
	c.chartMu.RUnlock()
	if !cached {
		release, acquireErr := c.chartLoadLocks.Acquire(ctx, cacheKey)
		if acquireErr != nil {
			return chartDescription{}, acquireErr
		}
		defer release()
		c.chartMu.RLock()
		loaded, cached = c.chartCache[cacheKey]
		c.chartMu.RUnlock()
		if !cached && selector.Pin != "" {
			err = capture()
		}
		if err != nil {
			return chartDescription{}, err
		}
		if !cached {
			loaded = chartDescription{captured: captured, fingerprint: fingerprint, key: cacheKey, location: location}
			loaded, err = c.describeCaptured(ctx, input, &loaded, selector.Auto)
			freshBuild = true
		}
	}
	if selector.Pin == "" {
		loaded.location = location
	}
	if loaded.needsBuild {
		loaded, err = c.completeDescription(ctx, input, &loaded, freshBuild)
	}
	if err != nil {
		return chartDescription{}, chartLoadError(location, root, err)
	}
	return loaded, nil
}

func (c *Client) describeCaptured(
	ctx context.Context,
	input fs.FS,
	loaded *chartDescription,
	auto bool,
) (chartDescription, error) {
	if !auto && loaded.captured.files != nil {
		options := chartinput.Options{
			Name:     loaded.location,
			Parent:   loaded.captured.files,
			Validate: validateDependencyPath,
		}
		description, _, err := chartinput.Describe(ctx, input, options, nil)
		if err != nil {
			return chartDescription{}, manifest.NewError(manifest.ErrInput, err, "")
		}
		loaded.captured.parent = description.Chart
		if len(description.Dependencies) != 0 {
			built := description
			loaded.captured.dependencies = &built
			loaded.needsBuild = true
		}
	}
	return c.rememberDescription(loaded, nil), nil
}

func (c *Client) rememberDescription(description *chartDescription, fresh *renderRecord) chartDescription {
	d := *description
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
	c.chartCache[d.key] = chartDescription{
		captured:    d.captured,
		fingerprint: d.fingerprint,
		key:         d.key,
		location:    d.location,
		remote:      d.remote,
		needsBuild:  d.needsBuild,
		chart:       d.chart,
		analysis:    d.analysis,
		record:      d.record,
	}
	c.chartMu.Unlock()
	return d
}

func (c *Client) decodeChart(ctx context.Context, d *chartDescription, input *RenderOptions) error {
	if d.chart != nil {
		return nil
	}
	if input.PreparationTasks != nil {
		decoded := *d
		accounting := input.Work
		decode := input.PreparationTasks.Fork()
		var decodeErr error
		var entered bool
		decode.Go(ctx, "helm history chart decode", func(ctx context.Context) {
			entered = true
			decodeErr = c.decodeChart(ctx, &decoded, &RenderOptions{Work: accounting})
		})
		decode.BlockTillDone()
		if decode.Failures() != 0 {
			panic("helm chart decoder panicked")
		}
		if !entered {
			return fmt.Errorf("%w: chart decode: %w", manifest.ErrFlux, ctx.Err())
		}
		*d = decoded
		return decodeErr
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
	work := input.Work.Decode(d.key)
	scope := input.Work.Attribution()
	work.Add(&manifest.WorkCounts{Attempts: 1, PeakConcurrent: scope.StartConcurrent()})
	d.chart, err = d.captured.load()
	scope.FinishConcurrent()
	work.Finish(err)
	if err != nil {
		if _, ok := errors.AsType[chart.ValidationError](err); ok {
			err = manifest.NewError(manifest.ErrInput, err, "")
		}
		label := d.location
		if d.artifact.LocalPath != "" {
			label = d.artifact.OriginPath(filepath.Join(d.artifact.LocalPath, filepath.FromSlash(d.location)))
		}
		return fmt.Errorf("load chart %s: %w", label, err)
	}
	*d = c.rememberDescription(d, nil)
	return nil
}

func (d *chartDescription) analyze() *chartAnalysis {
	if d.analysis != nil {
		return d.analysis
	}
	analysis := &chartAnalysis{
		Templates:      chartTemplateText(d.chart),
		Allowed:        knownTopLevelKeys(d.chart),
		RequiresLoader: requiresChartLoader(d.chart),
	}
	if d.chart.Metadata != nil {
		analysis.Version = d.chart.Metadata.Version
	}
	return analysis
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

type chartSelector struct {
	Pin  string `json:"Pin"`
	Path string `json:"Path"`
	Auto bool   `json:"Auto"`
}

func (s chartSelector) capture(ctx context.Context, input fs.FS, root string) (capturedChart, string, error) {
	location := s.Path
	archiveShape := false
	if s.Auto {
		location = cache.FileName(input)
		archiveShape = location == ""
		if archiveShape {
			location = "."
		}
	}
	captured, missing, err := captureInput(ctx, input, location, archiveShape)
	if missing {
		return captured, location, err
	}
	if err != nil {
		return captured, location, chartLoadError(location, root, err)
	}
	return captured, location, nil
}

func chartLoadError(location, root string, err error) error {
	if root != "" {
		location = filepath.Join(root, filepath.FromSlash(location))
	}
	return fmt.Errorf("load chart %s: %w", location, err)
}

func validateDependencyPath(name string) error {
	_, err := safepath.ArchiveName(name)
	return err
}
