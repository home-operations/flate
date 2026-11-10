package helm

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"slices"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/values"
)

type chartFileContent struct {
	Name string `json:"Name"`
	Data []byte `json:"Data"`
}

type chartAnalysis struct {
	Version        string              `json:"Version"`
	Templates      string              `json:"Templates"`
	Allowed        map[string]struct{} `json:"Allowed"`
	RequiresLoader bool                `json:"RequiresLoader"`
}

type renderRecord struct {
	key      string
	identity string
	Output   []byte         `json:"Output"`
	Analysis *chartAnalysis `json:"Analysis"`
	frame    renderFrame
	receipt  *manifest.RenderReceipt
	binding  templateBinding
}

type ociChartContent struct {
	Digest          string `json:"Digest"`
	Revision        string `json:"Revision"`
	DisableTracking bool   `json:"DisableTracking"`
}

// CommonMetadata is stamped after the cached pre-stamp render. Unsupported
// content returns an empty key, which MUST bypass lookup and publication.
func computeTemplateKey(
	loaded *chartDescription,
	releaseValues map[string]any,
	opts Options,
	hr *manifest.HelmRelease,
) string {
	if loaded.fingerprint == "" {
		return ""
	}
	type keyHR struct {
		ReleaseName              string                `json:"ReleaseName"`
		ReleaseNamespace         string                `json:"ReleaseNamespace"`
		CRDsPolicy               helmv2.CRDsPolicy     `json:"CRDsPolicy"`
		DisableSchemaValidation  bool                  `json:"DisableSchemaValidation"`
		DisableOpenAPIValidation bool                  `json:"DisableOpenAPIValidation"`
		InstallDisableHooks      bool                  `json:"InstallDisableHooks"`
		InstallReplace           bool                  `json:"InstallReplace"`
		TestEnable               bool                  `json:"TestEnable"`
		ChartVersion             string                `json:"ChartVersion"`
		ChartValuesFiles         []string              `json:"ChartValuesFiles"`
		IgnoreMissingValuesFiles bool                  `json:"IgnoreMissingValuesFiles"`
		PostRenderers            []helmv2.PostRenderer `json:"PostRenderers"`
	}
	policy := (&helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}).GetInstall()
	khr := keyHR{
		ReleaseName:              hr.ReleaseName(),
		ReleaseNamespace:         hr.ReleaseNamespace(),
		CRDsPolicy:               cmp.Or(policy.CRDs, helmv2.Create),
		DisableSchemaValidation:  hr.DisableSchemaValidation,
		DisableOpenAPIValidation: policy.DisableOpenAPIValidation,
		ChartVersion:             hr.Chart.Version,
		ChartValuesFiles:         hr.ChartValuesFiles,
		IgnoreMissingValuesFiles: hr.IgnoreMissingValuesFiles,
		PostRenderers:            hr.PostRenderers,
	}
	if hr.Install != nil {
		khr.InstallDisableHooks = hr.Install.DisableHooks
		khr.InstallReplace = hr.Install.Replace
	}
	if hr.Test != nil {
		khr.TestEnable = hr.Test.Enable
	}

	inputs := loaded.values.fingerprint
	type keyOptions struct {
		SkipCRDs    bool   `json:"SkipCRDs"`
		SkipTests   bool   `json:"SkipTests"`
		KubeVersion string `json:"KubeVersion"`
		APIVersions string `json:"APIVersions"`
		EnableDNS   bool   `json:"EnableDNS"`
	}
	return manifest.Fingerprint(struct {
		Chart   string           `json:"Chart"`
		OCI     *ociChartContent `json:"OCI"`
		Inputs  *string          `json:"Inputs"`
		Values  map[string]any   `json:"Values"`
		Options keyOptions       `json:"Options"`
		Release keyHR            `json:"Release"`
	}{
		Chart: loaded.fingerprint, OCI: loaded.oci, Inputs: inputs, Values: releaseValues,
		Options: keyOptions{opts.SkipCRDs, opts.SkipTests, opts.KubeVersion, opts.APIVersions, opts.EnableDNS},
		Release: khr,
	})
}

const frameDomain = "helm-output"

type renderHeader struct {
	Key      string         `json:"Key"`
	Binding  string         `json:"Binding"`
	Analysis *chartAnalysis `json:"Analysis"`
}

type templateBinding struct {
	Policy       manifest.HelmReleaseHistoryPolicy `json:"Policy"`
	Source       manifest.NamedResource            `json:"Source"`
	Kind         string                            `json:"Kind"`
	URL          string                            `json:"URL"`
	Path         string                            `json:"Path"`
	Root         string                            `json:"Root"`
	Key          string                            `json:"Key"`
	Revision     string                            `json:"Revision"`
	Digest       string                            `json:"Digest"`
	Alias        bool                              `json:"Alias"`
	Options      Options                           `json:"Options"`
	Dependencies values.DependencySubstitutions    `json:"Dependencies"`
}

func (a *templateBinding) same(b *templateBinding) bool {
	if !a.Policy.Equal(b.Policy) || a.Source != b.Source || a.Kind != b.Kind || a.URL != b.URL || a.Path != b.Path ||
		a.Root != b.Root ||
		a.Key != b.Key ||
		a.Revision != b.Revision ||
		a.Digest != b.Digest ||
		a.Alias != b.Alias ||
		!a.Dependencies.Equal(b.Dependencies) {
		return false
	}
	x, y := a.Options, b.Options
	return x.DisableChartDigestTracking == y.DisableChartDigestTracking && x.SkipCRDs == y.SkipCRDs &&
		x.SkipTests == y.SkipTests &&
		x.KubeVersion == y.KubeVersion &&
		x.APIVersions == y.APIVersions &&
		x.EnableDNS == y.EnableDNS &&
		slices.Equal(x.SkipKinds, y.SkipKinds)
}

type renderFrame struct {
	header  renderHeader
	binding templateBinding
	chart   *chart.Chart
	lookup  string
	output  []byte
	content any
	stored  *cache.Frame
}

func equalRenderFrame(a, b any) bool {
	left, ok := a.(renderFrame)
	right, other := b.(renderFrame)
	if !ok || !other || left.header.Key != right.header.Key || !bytes.Equal(left.output, right.output) {
		return false
	}
	return left.binding.same(&right.binding) && manifest.ValueEqual(left.content, right.content)
}

// RenderAcceptance binds an accepted inventory to its source paths.
type RenderAcceptance struct {
	Work    *manifest.RenderWork
	Owner   manifest.NamedResource                                      `json:"Owner"`
	Receipt *manifest.RenderReceipt                                     `json:"Receipt"`
	Records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] `json:"Records"`
	Sources []manifest.SourceEvidence                                   `json:"Sources"`
}

// RecordRender persists a receipt only with a complete accepted inventory.
//
//nolint:gocritic // Preserve the public value API and its accepted-inventory snapshot.
func (c *Client) RecordRender(ctx context.Context, acceptance RenderAcceptance) error {
	owner, receipt, records := acceptance.Owner, acceptance.Receipt, acceptance.Records
	if receipt == nil || records == nil || acceptance.Sources == nil {
		return nil
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok || frame.header.Key == "" || frame.header.Analysis == nil && frame.chart == nil {
		return manifest.NewError(manifest.ErrFlux, nil, "invalid helm receipt")
	}
	if !c.cache.CanPersistRenders() {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return ctx.Err()
	}
	if frame.header.Binding == "" {
		frame.header.Binding = manifest.Fingerprint(frame.binding)
	}
	if frame.header.Analysis == nil {
		frame.header.Analysis = (&chartDescription{chart: frame.chart}).analyze()
	}
	if frame.stored == nil {
		var err error
		frame.stored, err = cache.NewFrame(frameDomain, frame.header, frame.output)
		if err != nil || frame.stored == nil {
			return err
		}
	}
	publication, err := frame.stored.PreparePublication(ctx, cache.FramePublicationInput{
		Owner: owner, Content: frame.content, Records: records, Complete: true,
	})
	if err != nil || publication == nil {
		return err
	}
	return cache.RecordInventory(
		ctx,
		c.cache,
		cache.AcceptedInventory[*cache.FramePublication]{
			Key:        frame.header.Key,
			Lookup:     frame.lookup,
			Acceptance: publication.Identity(),
			Work:       acceptance.Work.History(),
			History:    cache.RenderHistory{Owner: owner, Sources: acceptance.Sources},
			Content:    publication,
		},
	)
}

func (c *Client) lookupRender(
	ctx context.Context,
	loaded *chartDescription,
	key string,
	input *RenderOptions,
) (*renderRecord, error) {
	record := loaded.record
	if record != nil && record.key == key && record.frame.lookup != "" {
		renderMetadataResult1, renderMetadataResult1Err := c.cache.RenderMetadata(
			ctx,
			key,
			record.frame.lookup,
			input.Work.History(),
		)
		identity, enabled, err := renderMetadataResult1.Identity, renderMetadataResult1.Enabled, renderMetadataResult1Err
		if errors.Is(err, errdef.ErrNotFound) {
			record, err = nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("lookup helm render cache: %w", err)
		}
		if !enabled || identity == "" || record == nil || identity != record.identity {
			record = nil
		}
	} else {
		record = nil
	}
	if record == nil {
		var err error
		record, err = c.readRender(ctx, key, input)
		if err != nil || record == nil {
			return nil, err
		}
	}
	if record.Analysis.RequiresLoader {
		if err := c.decodeChart(ctx, loaded, input); err != nil {
			return nil, err
		}
	}
	return record, nil
}

// PreviousRender returns complete accepted history without running an action or acquiring a chart.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func (c *Client) PreviousRender(
	ctx context.Context,
	hr *manifest.HelmRelease,
	hrValues map[string]any,
	input RenderOptions,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if ctx.Err() != nil || !c.cache.CanPersistRenders() {
		return nil, false
	}
	prepared, err := c.prepareTemplate(ctx, hr, hrValues, &input)
	if err != nil || prepared.key == "" || prepared.loaded.valuesErr != nil || prepared.capabilityErr != nil {
		return nil, false
	}
	_, record := prepared.contentRender(ctx, c)
	if record == nil {
		return nil, false
	}
	if record.frame.stored == nil {
		return nil, false
	}
	records, complete := record.frame.stored.History(hr.Named(), prepared.content)
	if complete && record != prepared.loaded.record {
		c.rememberDescription(&prepared.loaded, record)
	}
	return records, complete
}

func (r *templateRequest) contentRender(ctx context.Context, c *Client) (RenderAdmission, *renderRecord) {
	admission := RenderAdmission{Key: r.key}
	if ctx.Err() != nil || r.key == "" || r.loaded.valuesErr != nil || r.capabilityErr != nil {
		return admission, nil
	}
	record, err := c.lookupRender(ctx, &r.loaded, r.key, &r.input)
	if err != nil || record == nil {
		return admission, nil
	}
	record, documents, err := r.validateRecord(ctx, c, record)
	if err != nil || record == nil {
		return admission, nil
	}
	admission.Receipt = record.receipt
	if record.frame.stored != nil {
		if _, complete := record.frame.stored.History(r.release.Named(), r.content); complete {
			admission.Documents, admission.Accepted = normalizeLists(documents), true
		}
	}
	return admission, record
}

func (c *Client) readRender(ctx context.Context, key string, input *RenderOptions) (*renderRecord, error) {
	reader, _, _, err := c.cache.Blob(
		ctx,
		cache.BlobKey{
			Content: key,
			Render:  cache.RenderPending,
		},
		&cache.Policy{MaxAge: cache.Forever, Work: input.Work.History()},
		nil,
	)
	if errors.Is(err, errdef.ErrNotFound) {
		//nolint:nilnil // An unavailable receipt is a cache miss, not a render failure.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup helm render cache: %w", err)
	}
	identity := cache.AcquisitionIdentity(reader)
	var frame renderFrame
	stored, err := cache.OpenFrame(ctx, frameDomain, reader, &frame.header)
	if _, storage := errors.AsType[*cache.CacheError](err); storage {
		return nil, err
	}
	if err != nil || frame.header.Key != key || frame.header.Analysis == nil {
		return nil, nil //nolint:nilerr,nilnil // Invalid frame metadata is an ordinary cache miss.
	}
	frame.output, err = stored.Payload(ctx)
	if _, storage := errors.AsType[*cache.CacheError](err); storage {
		return nil, err
	}
	if err != nil {
		return nil, nil //nolint:nilerr,nilnil // Invalid reusable payload is an ordinary cache miss.
	}
	frame.stored = stored
	frame.lookup = ""
	record := &renderRecord{
		key:      key,
		identity: identity,
		Output:   frame.output,
		Analysis: frame.header.Analysis,
		frame:    frame,
	}
	return record, nil
}

type templateResult struct {
	admissionErr error
	record       *renderRecord
	observed     bool
	handled      bool
	docs         []manifest.DocOccurrence
	produced     *renderRecord
	receipt      *manifest.RenderReceipt
}

func (r *templateResult) admit(ctx context.Context, c *Client, request *templateRequest) (bool, error) {
	r.observed = true
	admission := RenderAdmission{Key: request.key, Receipt: r.receipt}
	if r.record != nil && r.record.frame.stored != nil {
		if _, complete := r.record.frame.stored.History(request.release.Named(), request.content); complete {
			admission.Documents, admission.Accepted = normalizeLists(r.docs), true
		}
	}
	r.handled, r.admissionErr = request.admit(ctx, c, admission)
	return r.handled, r.admissionErr
}

func (r *templateRequest) template(
	ctx context.Context,
	c *Client,
) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
	render := func(ctx context.Context, cacheErr error) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
		return r.render(ctx, c, cacheErr)
	}
	if r.key == "" || r.loaded.valuesErr != nil || r.capabilityErr != nil {
		if r.input.Admit != nil {
			if handled, admitErr := r.admit(
				ctx,
				c,
				RenderAdmission{Key: r.key},
			); handled ||
				admitErr != nil {
				return nil, nil, admitErr
			}
		}
		return render(ctx, nil)
	}
	// Callback state MUST share one allocation to preserve cache-hit allocation counts.
	var result templateResult
	blobKey := cache.BlobKey{
		Content: r.key,
		Render:  cache.RenderPending,
		Validate: func(ctx context.Context, generation fs.FS) (bool, error) {
			return r.validate(ctx, c, &result, generation)
		},
	}
	if r.input.Admit != nil {
		blobKey.Observe = func(ctx context.Context, _ fs.FS) (bool, error) {
			return result.admit(ctx, c, r)
		}
	}
	_, _, _, err := c.cache.Blob(
		ctx,
		blobKey,
		&cache.Policy{MaxAge: cache.Forever, Work: r.input.Work.Pending(), HistoryWork: r.input.Work.History()},
		func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			return r.produce(ctx, c, &result, w)
		},
	)
	if err == nil {
		if result.handled {
			return nil, nil, nil
		}
		return normalizeLists(result.docs), result.receipt, nil
	}
	if result.admissionErr != nil {
		return nil, nil, result.admissionErr
	}
	if _, storage := errors.AsType[*cache.CacheError](err); !storage {
		return nil, nil, err
	}
	if r.input.Admit != nil && !result.observed {
		if handled, admitErr := result.admit(ctx, c, r); handled || admitErr != nil {
			return nil, nil, admitErr
		}
	}
	return render(ctx, err)
}

func (r *templateRequest) admit(ctx context.Context, c *Client, admission RenderAdmission) (bool, error) {
	admission.Possible = admission.Accepted
	if !admission.Accepted {
		var err error
		admission.Possible, err = r.entryPrecondition(ctx, c)
		if err != nil {
			return false, err
		}
	}
	return r.input.Admit(admission)
}
