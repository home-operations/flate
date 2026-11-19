package helm

import (
	"context"
	"fmt"
	"slices"

	"helm.sh/helm/v4/pkg/chart/common"

	"github.com/home-operations/flate/internal/engine/helm/chartinput"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Preparation MUST stay with one logical renderer owner until transfer or retirement.
// Retained input graphs MUST stay immutable for the full ownership lifetime.
type Preparation struct {
	client        *Client
	declaration   *manifest.HelmRelease
	source        *store.SourceArtifact
	complete      bool
	chartPrepared bool
	loaded        chartDescription
	caps          *common.Capabilities
	capabilityErr error
	key           string
	binding       cache.RenderBinding[templatePolicy]
	values        map[string]any
}

// Matches MUST NOT grant publication authority or validate current source bytes.
func (p *Preparation) Matches(client *Client, declaration *manifest.HelmRelease, request *RenderOptions) bool {
	if p == nil || !p.complete && !p.chartPrepared {
		return false
	}
	if p.client != client || declaration == nil || !manifest.Equal(p.declaration, declaration) {
		return false
	}
	if !p.complete {
		return true
	}
	binding := p.binding
	binding.Policy.Options = request.Options
	binding.Policy.Release = declaration.HistoryPolicy()
	return p.binding.Equal(&binding, equalTemplatePolicy)
}

func (p *Preparation) matchesValues(current map[string]any) bool {
	return len(p.values) == 0 && len(current) == 0 || manifest.ValueEqual(p.values, current)
}

type templateRequest struct {
	Preparation

	input   RenderOptions
	release *manifest.HelmRelease
	content any
}

func (c *Client) prepareTemplate(
	ctx context.Context,
	hr *manifest.HelmRelease,
	hrValues map[string]any,
	input *RenderOptions,
) (templateRequest, error) {
	work := input.Work.Preparation()
	opts := input.Options
	if err := ctx.Err(); err != nil {
		work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
		work.Finish(err)
		return templateRequest{}, err
	}
	artifact := input.Sources.chartInput(hr)
	previous := input.Preparation
	reusable := previous.Matches(c, hr, input) && previous.complete
	if reusable {
		read := store.ArtifactContent
		if hr.TracksDigest() {
			read |= store.ArtifactRevision
		}
		reusable = artifact != nil && previous.source.SameMaterial(artifact, read) && previous.matchesValues(hrValues)
	}
	if reusable && artifact.Content != "" && !previous.loaded.remote {
		input.Work.Fingerprint(previous.key)
		previous.bindSource(hr, artifact, input)
		return previous.request(hr, input), nil
	}
	loaded, err := c.preparedChart(ctx, hr, artifact, previous)
	if err == nil {
		err = loaded.requireDependencies(ctx, c, hr, input)
	}
	if err != nil {
		work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
		work.Finish(err)
		return templateRequest{}, err
	}
	if hr.TracksDigest() {
		loaded.oci = &ociChartContent{
			Digest:          loaded.artifact.Digest,
			Revision:        loaded.artifact.Revision,
			DisableTracking: opts.DisableChartDigestTracking,
		}
	}
	if loaded.valuesFS != nil {
		loaded.values, loaded.valuesErr = c.observeValuesFiles(
			ctx, &loaded, hr.ChartValuesFiles,
			hr.IgnoreMissingValuesFiles,
		)
	}
	if reusable && loaded.fingerprint == previous.loaded.fingerprint &&
		loaded.values.key == previous.loaded.values.key &&
		loaded.valuesErr == nil &&
		previous.loaded.valuesErr == nil {
		input.Work.Fingerprint(previous.key)
		previous.bindSource(hr, artifact, input)
		return previous.request(hr, input), nil
	}
	if previous != nil {
		*previous = Preparation{}
	}
	work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	caps, capabilityErr := opts.capabilities()
	if hrValues == nil {
		hrValues = map[string]any{}
	}
	key := computeTemplateKey(&loaded, hrValues, opts, hr)
	input.Work.Fingerprint(key)
	work.Finish(nil)
	prepared := Preparation{
		client: c, declaration: hr,
		chartPrepared: true,
		loaded:        loaded,
		caps:          caps,
		capabilityErr: capabilityErr,
		key:           key,
		values:        hrValues,
	}
	prepared.bindSource(hr, artifact, input)
	if input.Preparation != nil {
		*input.Preparation = prepared
	}
	return prepared.request(hr, input), nil
}

func (c *Client) preparedChart(
	ctx context.Context,
	hr *manifest.HelmRelease,
	artifact *store.SourceArtifact,
	previous *Preparation,
) (chartDescription, error) {
	pending := previous != nil && previous.chartPrepared && !previous.complete
	if pending && previous.client == c && previous.source == artifact {
		return previous.loaded, nil
	}
	return c.describeChart(ctx, hr, artifact)
}

func (d *chartDescription) requireDependencies(
	ctx context.Context,
	client *Client,
	hr *manifest.HelmRelease,
	input *RenderOptions,
) error {
	dependencies := d.captured.dependencyInputs()
	for i := range dependencies {
		dep := &dependencies[i]
		if dep.Capture.Files != nil || dep.Capture.Archive != nil {
			continue
		}
		reads, captured := input.Sources.reader.(*store.ReadSet)
		if captured && client.fetcher != nil {
			_, err := client.fetcher.FetchDependency(
				ctx,
				hr.ChartSource().Namespace,
				&dep.Declaration,
				input.Sources.index,
				reads,
				&cache.Policy{},
				true,
			)
			if err != nil {
				return err
			}
		}
		return manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("unresolved chart dependency %s (%s)", dep.Name, dep.Declaration.Repository),
		)
	}
	return nil
}

func (p *Preparation) bindSource(hr *manifest.HelmRelease, artifact *store.SourceArtifact, input *RenderOptions) {
	binding := cache.RenderBinding[templatePolicy]{
		Policy: templatePolicy{Release: hr.HistoryPolicy(), Options: input.Options},
	}
	binding.Refresh(p.key, hr.ChartSource(), artifact)

	switch {
	case p.complete:
		binding.Policy.Options.SkipKinds = p.binding.Policy.Options.SkipKinds
	case p.loaded.record != nil && p.loaded.record.key == p.key && p.loaded.record.binding.Equal(&binding, equalTemplatePolicy):
		binding = p.loaded.record.binding
	default:
		binding.Policy.Options.SkipKinds = nil
		if len(input.SkipKinds) > 0 {
			binding.Policy.Options.SkipKinds = slices.Clone(input.SkipKinds)
		}
	}
	p.source = artifact
	p.loaded.artifact = *artifact
	p.binding = binding
	p.complete = true
}

func (p *Preparation) request(hr *manifest.HelmRelease, input *RenderOptions) templateRequest {
	omitted := slices.Clone(input.Omitted)
	slices.SortFunc(omitted, manifest.NamedResource.Compare)
	omitted = slices.Compact(omitted)
	current := *input
	current.Omitted = omitted
	return templateRequest{
		Preparation: *p, input: current, release: hr, content: hr.Content(p.key, omitted),
	}
}

// PrepareChart MUST leave transport and cleanup ownership with the acquisition continuation.
//
//nolint:nilnil,nonamedreturns // A nil callback marks completion; the deferred work receipt needs the returned error.
func (c *Client) PrepareChart(
	ctx context.Context,
	hr *manifest.HelmRelease,
	input *RenderOptions,
) (pending func(context.Context) (source.PreparedFetch, error), resultErr error) {
	defer func() {
		if resultErr != nil {
			work := input.Work.Preparation()
			work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
			work.Finish(resultErr)
		}
	}()
	p := input.Preparation
	artifact := input.Sources.chartInput(hr)
	sameInput := p.client == c && p.declaration != nil && manifest.Equal(p.declaration, hr) && p.source == artifact
	prepared := p.chartPrepared && (!p.complete || !p.loaded.remote)
	if !sameInput || !prepared {
		loaded, err := c.describeChart(ctx, hr, artifact)
		if err != nil {
			return nil, err
		}
		*p = Preparation{client: c, declaration: hr, source: artifact, loaded: loaded, chartPrepared: true}
	}
	dependencies := p.loaded.captured.dependencyInputs()
	for i := range dependencies {
		dep := &dependencies[i]
		if dep.Capture.Files != nil || dep.Capture.Archive != nil {
			continue
		}
		reads, ok := input.Sources.reader.(*store.ReadSet)
		if !ok || c.fetcher == nil {
			return nil, manifest.NewError(manifest.ErrInput, nil, "chart dependency acquisition is unavailable")
		}
		return func(ctx context.Context) (source.PreparedFetch, error) {
			return c.fetcher.FetchDependency(
				ctx,
				hr.ChartSource().Namespace,
				&dep.Declaration,
				input.Sources.index,
				reads,
				&cache.Policy{},
				false,
			)
		}, nil
	}
	return nil, nil
}

// AcceptChartDependency consumes the artifact before the acquisition owner closes its handle.
func (p *Preparation) AcceptChartDependency(ctx context.Context, artifact *store.SourceArtifact) error {
	if artifact == nil || artifact.FS == nil {
		return manifest.NewError(manifest.ErrInput, nil, "chart dependency content unavailable")
	}
	input, _, err := source.OperationView(ctx, artifact.FS)
	if err != nil {
		return err
	}
	captured, _, err := (chartSelector{Auto: true}).capture(ctx, input, artifact.LocalPath)
	if err != nil {
		return err
	}
	if _, err := captured.load(); err != nil {
		return manifest.NewError(manifest.ErrInput, err, "")
	}
	built := *p.loaded.captured.dependencies
	built.Dependencies = slices.Clone(built.Dependencies)
	p.loaded.captured.dependencies = &built
	for i := range built.Dependencies {
		dep := &built.Dependencies[i]
		if dep.Capture.Files == nil && dep.Capture.Archive == nil {
			dep.Capture = chartinput.Capture{Files: captured.files, Archive: captured.archive}
			p.loaded.remote = true
			p.loaded.fingerprint = p.loaded.captured.fingerprint()
			p.loaded.key = p.loaded.fingerprint
			p.loaded.chart, p.loaded.analysis, p.loaded.record = nil, nil, nil
			return nil
		}
	}
	panic("chart dependency completion without a pending dependency")
}
