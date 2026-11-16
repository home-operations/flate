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
	binding  cache.RenderBinding[templatePolicy]
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
	Analysis *chartAnalysis `json:"Analysis"`
}

type templatePolicy struct {
	Release manifest.HelmReleaseHistoryPolicy `json:"Release"`
	Options Options                           `json:"Options"`
}

func equalTemplatePolicy(a, b *templatePolicy) bool {
	if !a.Release.Equal(b.Release) {
		return false
	}
	x, y := a.Options, b.Options
	if x.DisableChartDigestTracking != y.DisableChartDigestTracking || x.SkipCRDs != y.SkipCRDs ||
		x.SkipTests != y.SkipTests {
		return false
	}
	if x.KubeVersion != y.KubeVersion || x.APIVersions != y.APIVersions || x.EnableDNS != y.EnableDNS {
		return false
	}
	return slices.Equal(x.SkipKinds, y.SkipKinds)
}

type renderFrame struct {
	cache.RenderFrame

	header  renderHeader
	binding cache.RenderBinding[templatePolicy]
	chart   *chart.Chart
	content any
	persist bool
}

func equalRenderFrame(a, b any) bool {
	left, ok := a.(renderFrame)
	right, other := b.(renderFrame)
	if !ok || !other || left.Key != right.Key || !bytes.Equal(left.Output, right.Output) {
		return false
	}
	return left.binding.Equal(&right.binding, equalTemplatePolicy) && manifest.ValueEqual(left.content, right.content)
}

//nolint:gocritic // Receipts retain concrete frame values, so publication MUST use a value receiver.
func (f renderFrame) RenderPublication(
	_ context.Context,
	acceptance *cache.RenderAcceptance,
) (cache.RenderPublication, error) {
	if f.Key == "" || f.header.Analysis == nil && f.chart == nil {
		return cache.RenderPublication{}, manifest.NewError(manifest.ErrFlux, nil, "invalid helm receipt")
	}
	if acceptance == nil || !f.persist {
		return cache.RenderPublication{}, nil
	}
	if f.header.Analysis == nil {
		f.header.Analysis = (&chartDescription{chart: f.chart}).analyze()
	}
	if f.Binding == "" {
		binding := f.binding
		f.Rebind(binding.Fingerprint())
	}
	return cache.RenderPublication{
		Frame: f.RenderFrame, Domain: frameDomain, Metadata: f.header,
		Content: f.content, Sources: acceptance.Sources,
	}, nil
}

func (c *Client) lookupRender(
	ctx context.Context,
	loaded *chartDescription,
	key string,
	input *RenderOptions,
) (*renderRecord, error) {
	record := loaded.record
	if record != nil && record.key == key && record.frame.Lookup != "" {
		renderMetadataResult1, renderMetadataResult1Err := c.cache.RenderMetadata(
			ctx,
			key,
			record.frame.Lookup,
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
	if record.frame.Stored == nil {
		return nil, false
	}
	records, complete := record.frame.Stored.History(hr.Named(), prepared.content)
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
	if record.frame.Stored != nil {
		if _, complete := record.frame.Stored.History(r.release.Named(), r.content); complete {
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
	frame.RenderFrame, err = cache.OpenRenderFrame(ctx, frameDomain, reader, &frame.header)
	if _, storage := errors.AsType[*cache.Error](err); storage {
		return nil, err
	}
	if err != nil || frame.Key != key || frame.header.Analysis == nil {
		return nil, nil //nolint:nilerr,nilnil // Invalid frame metadata is an ordinary cache miss.
	}
	frame.Lookup = ""
	record := &renderRecord{
		key:      key,
		identity: identity,
		Output:   frame.Output,
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
	if r.record != nil && r.record.frame.Stored != nil {
		if _, complete := r.record.frame.Stored.History(request.release.Named(), request.content); complete {
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
	if _, storage := errors.AsType[*cache.Error](err); !storage {
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
