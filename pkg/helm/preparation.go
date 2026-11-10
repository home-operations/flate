package helm

import (
	"context"
	"slices"

	"helm.sh/helm/v4/pkg/chart/common"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Preparation MUST stay with one logical renderer owner until transfer or retirement.
// Retained input graphs MUST stay immutable for the full ownership lifetime.
// Take transfers that lifetime to its destination; clearing the source does not end it.
type Preparation struct {
	client        *Client
	declaration   *manifest.HelmRelease
	source        *store.SourceArtifact
	complete      bool
	loaded        chartDescription
	caps          *common.Capabilities
	capabilityErr error
	key           string
	binding       templateBinding
	values        map[string]any
}

// Retire MUST follow the final consumer and clears retained input references.
func (p *Preparation) Retire() {
	if p != nil {
		*p = Preparation{}
	}
}

// Take MUST NOT move a live capture or grant publication authority.
func (p *Preparation) Take(
	previous *Preparation,
	client *Client,
	declaration *manifest.HelmRelease,
	request *RenderOptions,
) bool {
	if p == nil || p.complete || previous == nil || !previous.complete {
		return false
	}
	if !previous.matchesOwner(client, declaration, request) {
		return false
	}
	*p, *previous = *previous, Preparation{}
	return true
}

func (p *Preparation) matchesOwner(client *Client, declaration *manifest.HelmRelease, request *RenderOptions) bool {
	if p.client != client || declaration == nil || !manifest.Equal(p.declaration, declaration) {
		return false
	}
	binding := p.binding
	binding.Options, binding.Dependencies = request.Options, request.DependencySubstitutions
	binding.Policy = declaration.HistoryPolicy()
	return p.binding.same(&binding)
}

func (p *Preparation) matchesInputs(source *store.SourceArtifact, current map[string]any) bool {
	valuesEqual := len(p.values) == 0 && len(current) == 0 || manifest.ValueEqual(p.values, current)
	if !valuesEqual || source == nil || p.source == nil {
		return false
	}
	read := store.ArtifactContent
	if p.declaration.TracksDigest() {
		read |= store.ArtifactRevision
	}
	return p.source == source || source.Content != "" && p.source.Input(read) == source.Input(read)
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
		//nolint:wrapcheck // Cancellation MUST preserve the caller-visible sentinel identity.
		return templateRequest{}, err
	}
	source := input.Sources.chartInput(hr)
	previous := input.Preparation
	reusable := previous != nil && previous.complete && previous.matchesOwner(c, hr, input) &&
		previous.matchesInputs(source, hrValues)
	if reusable && source.Content != "" {
		input.Work.Fingerprint(previous.key)
		return previous.request(hr, input), nil
	}
	loaded, err := c.describeChart(ctx, hr, source)
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
		return previous.request(hr, input), nil
	}
	if previous != nil {
		previous.Retire()
	}
	work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	caps, capabilityErr := opts.capabilities()
	if hrValues == nil {
		hrValues = map[string]any{}
	}
	key := computeTemplateKey(&loaded, hrValues, opts, hr)
	input.Work.Fingerprint(key)
	binding := templateBinding{
		Policy:       hr.HistoryPolicy(),
		Source:       hr.ChartSource(),
		Kind:         loaded.artifact.Kind,
		URL:          loaded.artifact.URL,
		Path:         loaded.artifact.LocalPath,
		Root:         loaded.artifact.LocalRoot,
		Key:          loaded.artifact.Key,
		Revision:     loaded.artifact.Revision,
		Digest:       loaded.artifact.Digest,
		Alias:        loaded.artifact.InputAlias,
		Options:      input.Options,
		Dependencies: input.DependencySubstitutions,
	}
	if loaded.record != nil && loaded.record.key == key && loaded.record.binding.same(&binding) {
		binding = loaded.record.binding
	} else {
		binding.Options.SkipKinds = nil
		if len(input.SkipKinds) > 0 {
			binding.Options.SkipKinds = slices.Clone(input.SkipKinds)
		}
	}
	work.Finish(nil)
	prepared := Preparation{
		client: c, declaration: hr, source: source, complete: true,
		loaded:        loaded,
		caps:          caps,
		capabilityErr: capabilityErr,
		key:           key,
		values:        hrValues,
		binding:       binding,
	}
	if input.Preparation != nil {
		*input.Preparation = prepared
	}
	return prepared.request(hr, input), nil
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
