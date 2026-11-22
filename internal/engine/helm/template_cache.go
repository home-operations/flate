package helm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"slices"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
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
	identity string
	Output   []byte         `json:"Output"`
	Analysis *chartAnalysis `json:"Analysis"`
	frame    renderFrame
	receipt  *manifest.RenderReceipt
	accepted *cache.RenderFrame
}

type ociChartContent struct {
	Digest          string `json:"Digest"`
	Revision        string `json:"Revision"`
	DisableTracking bool   `json:"DisableTracking"`
}

type templateReleaseScalars struct {
	ReleaseName              string `json:"ReleaseName"`
	ReleaseNamespace         string `json:"ReleaseNamespace"`
	SkipChartCRDs            bool   `json:"SkipChartCRDs"`
	DisableSchemaValidation  bool   `json:"DisableSchemaValidation"`
	DisableOpenAPIValidation bool   `json:"DisableOpenAPIValidation"`
	InstallDisableHooks      bool   `json:"InstallDisableHooks"`
	InstallReplace           bool   `json:"InstallReplace"`
	TestEnable               bool   `json:"TestEnable"`
	ChartVersion             string `json:"ChartVersion"`
}

type templateReleaseKey struct {
	templateReleaseScalars

	ChartValuesFiles         []string              `json:"ChartValuesFiles"`
	IgnoreMissingValuesFiles bool                  `json:"IgnoreMissingValuesFiles"`
	PostRenderers            []helmv2.PostRenderer `json:"PostRenderers"`
}

func templateReleaseFor(hr *manifest.HelmRelease) templateReleaseKey {
	policy := (&helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}).GetInstall()
	key := templateReleaseKey{
		ReleaseName:              hr.ReleaseName(),
		ReleaseNamespace:         hr.ReleaseNamespace(),
		SkipChartCRDs:            policy.CRDs == helmv2.Skip,
		DisableSchemaValidation:  hr.DisableSchemaValidation,
		DisableOpenAPIValidation: policy.DisableOpenAPIValidation,
		ChartVersion:             hr.Chart.Version,
		ChartValuesFiles:         hr.ChartValuesFiles,
		IgnoreMissingValuesFiles: hr.IgnoreMissingValuesFiles,
		PostRenderers:            hr.PostRenderers,
	}
	if hr.Install != nil {
		key.InstallDisableHooks = hr.Install.DisableHooks
		key.InstallReplace = hr.Install.Replace
	}
	if hr.Test != nil {
		key.TestEnable = hr.Test.Enable
	}

	return key
}

func (k *templateReleaseKey) equal(other *templateReleaseKey) bool {
	sameValuesFiles := k.IgnoreMissingValuesFiles == other.IgnoreMissingValuesFiles &&
		(k.ChartValuesFiles == nil) == (other.ChartValuesFiles == nil) &&
		slices.Equal(k.ChartValuesFiles, other.ChartValuesFiles)
	return k.templateReleaseScalars == other.templateReleaseScalars &&
		sameValuesFiles && manifest.ValueEqual(k.PostRenderers, other.PostRenderers)
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

	inputs := loaded.values.fingerprint
	type keyOptions struct {
		SkipCRDs    bool   `json:"SkipCRDs"`
		SkipTests   bool   `json:"SkipTests"`
		KubeVersion string `json:"KubeVersion"`
		APIVersions string `json:"APIVersions"`
		EnableDNS   bool   `json:"EnableDNS"`
	}
	return manifest.Fingerprint(struct {
		Chart   string             `json:"Chart"`
		OCI     *ociChartContent   `json:"OCI"`
		Inputs  *string            `json:"Inputs"`
		Values  map[string]any     `json:"Values"`
		Options keyOptions         `json:"Options"`
		Release templateReleaseKey `json:"Release"`
	}{
		Chart: loaded.fingerprint, OCI: loaded.oci, Inputs: inputs, Values: releaseValues,
		Options: keyOptions{opts.SkipCRDs, opts.SkipTests, opts.KubeVersion, opts.APIVersions, opts.EnableDNS},
		Release: templateReleaseFor(hr),
	})
}

const frameDomain = "helm-output"

type renderHeader struct {
	Analysis *chartAnalysis `json:"Analysis"`

	chart   *chart.Chart
	content any
	persist bool
}

type templatePolicy struct {
	Release manifest.HelmReleaseHistoryPolicy `json:"Release"`
	Options Options                           `json:"Options"`
}

//nolint:gocritic // Value parameters keep policy snapshots on the caller's stack.
func equalTemplatePolicy(a, b templatePolicy) bool {
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

type renderFrame = cache.RenderReceipt[templatePolicy, renderHeader]

//nolint:gochecknoglobals // Receipts share this immutable domain definition.
var renderDomain = cache.NewRenderDomain(cache.RenderDomain[templatePolicy, renderHeader]{
	Name: frameDomain, EqualPolicy: equalTemplatePolicy,
	ValidHeader: func(h renderHeader) bool { return h.Analysis != nil || h.chart != nil },
	EqualHeader: func(a, b renderHeader) bool { return manifest.ValueEqual(a.content, b.content) },
	Prepare: func(h renderHeader) renderHeader {
		if h.Analysis == nil {
			h.Analysis = (&chartDescription{chart: h.chart}).analyze()
		}
		return h
	},
	Accept: func(h renderHeader, acceptance *cache.RenderAcceptance) (any, []manifest.SourceEvidence) {
		if !h.persist {
			return nil, nil
		}
		return h.content, acceptance.Sources
	},
})

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
	if artifact := input.Sources.chartInput(hr); artifact == nil || artifact.FS == nil {
		return nil, false
	}
	prepared, err := c.prepareTemplate(ctx, hr, hrValues, &input)
	if err != nil || prepared.key == "" || prepared.loaded.valuesErr != nil || prepared.capabilityErr != nil {
		return nil, false
	}
	record, documents, err := prepared.lookupRender(ctx, true)
	if err != nil || record == nil {
		return nil, false
	}
	records, complete := record.frame.History(hr.Named(), prepared.content)
	if complete && record != prepared.loaded.record {
		prepared.loaded = c.rememberDescription(&prepared.loaded, record)
		if input.Preparation != nil {
			input.Preparation.loaded = prepared.loaded
		}
	}
	if complete && input.Previous != nil {
		*input.Previous = RenderAdmission{
			Key: prepared.key, Receipt: record.receipt, Documents: normalizeLists(documents),
			Accepted: true, Possible: true,
		}
	}
	return records, complete
}

func (r *templateRequest) lookupRender(
	ctx context.Context,
	history bool,
) (*renderRecord, []manifest.DocOccurrence, error) {
	reader, _, _, err := r.client.cache.Blob(
		ctx,
		cache.BlobKey{Content: r.key, Render: cache.RenderPending},
		&cache.Policy{MaxAge: cache.Forever, Work: r.input.Work.History()},
		nil,
	)
	if errors.Is(err, errdef.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("lookup helm render cache: %w", err)
	}
	record, err := r.cachedRecord(ctx, reader, cache.AcquisitionIdentity(reader))
	if err != nil || record == nil {
		return nil, nil, err
	}
	var binding *cache.RenderBinding[templatePolicy]
	if r.input.Preparation != nil {
		binding = &r.input.Preparation.binding
	} else {
		// Standalone history owns only a binding snapshot, with no preparation to borrow.
		binding = new(r.binding)
	}
	pendingHistory := history && r.input.Previous == nil && record.accepted == nil
	storedReceipt := record.frame.Stored != nil && record.receipt != nil
	if pendingHistory && storedReceipt && samePreparedRender(&record.frame, binding, r.content) {
		return record, nil, ctx.Err()
	}
	return r.validateRecord(ctx, record, binding, history)
}

func samePreparedRender(frame *renderFrame, binding *cache.RenderBinding[templatePolicy], content any) bool {
	return frame.Inputs.Equal(binding, equalTemplatePolicy) && manifest.ValueEqual(frame.Header.content, content)
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
	r.handled, r.admissionErr = request.admit(ctx, c, r.admission(request))
	return r.handled, r.admissionErr
}

func (r *templateResult) admission(request *templateRequest) RenderAdmission {
	admission := RenderAdmission{Key: request.key, Receipt: r.receipt}
	if r.record != nil {
		if _, complete := r.record.frame.History(request.release.Named(), request.content); complete {
			admission.Documents, admission.Accepted = normalizeLists(r.docs), true
		}
	}
	return admission
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
