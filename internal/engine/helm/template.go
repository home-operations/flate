package helm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	yamlv4 "go.yaml.in/yaml/v4"
	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/cli"
	release "helm.sh/helm/v4/pkg/release/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/cli-runtime/pkg/resource"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/engine/helm/deterministic"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

var envSettings = sync.OnceValue(cli.New)

// RenderError MUST wrap only engine failures after input preparation.
type RenderError struct {
	Err error
}

func (e *RenderError) Error() string { return "helm template: " + e.Err.Error() }
func (e *RenderError) Unwrap() error { return e.Err }

const sourceHeader = "# Source: "

// Template renders a HelmRelease and returns independently owned documents.
// The immutable receipt may be persisted after complete accepted publication.
// When input.Preparation is non-nil, hrValues and all reachable maps and slices
// MUST remain immutable until that preparation and every transferred owner retire.
// Callers MUST replace values or retire their preparation before mutation.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func (c *Client) Template(
	ctx context.Context,
	hr *manifest.HelmRelease,
	hrValues map[string]any,
	input RenderOptions,
) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
	prepared, err := c.prepareTemplate(ctx, hr, hrValues, &input)
	if err != nil {
		return nil, nil, err
	}

	return prepared.template(ctx, c)
}

func normalizeLists(docs []manifest.DocOccurrence) []manifest.DocOccurrence {
	var out []manifest.DocOccurrence
	for i, doc := range docs {
		if _, selected := doc.Document["items"]; !selected || doc.Err != nil {
			if out != nil {
				out = append(out, doc)
			}
			continue
		}
		if out == nil {
			out = make([]manifest.DocOccurrence, 0, len(docs))
			out = append(out, docs[:i]...)
		}
		data, err := yamlv4.Marshal(doc.Document)
		if err != nil {
			doc.Err = manifest.NewError(manifest.ErrInput, fmt.Errorf("encode helm list: %w", err), "")
			out = append(out, doc)
			continue
		}
		infos, err := resource.NewLocalBuilder().
			ContinueOnError().
			Unstructured().
			Flatten().
			Stream(bytes.NewReader(data), "").
			Do().
			Infos()
		if err != nil {
			doc.Err = manifest.NewError(manifest.ErrInput, fmt.Errorf("normalize helm list: %w", err), "")
			out = append(out, doc)
			continue
		}
		for _, info := range infos {
			out = append(
				out,
				//nolint:errcheck // This cache stores only the exact concrete type asserted at this boundary.
				manifest.DocOccurrence{Document: info.Object.(*unstructured.Unstructured).Object, Ordinal: doc.Ordinal},
			)
		}
	}
	if out == nil {
		return docs
	}
	return out
}

func validChartTemplates(ch *chart.Chart) bool {
	for _, file := range ch.Templates {
		if file != nil && !utf8.Valid(file.Data) {
			return false
		}
	}
	for _, sub := range ch.Dependencies() {
		if !validChartTemplates(sub) {
			return false
		}
	}
	return true
}

func validateCRDs(ch *chart.Chart) error {
	crds := ch.CRDObjects()
	for _, crd := range crds {
		docs, err := manifest.DecodeDocs(crdReader(crd), manifest.RequiredDocuments)
		for _, doc := range docs {
			manifest.ReleaseDoc(doc.Document)
		}
		if err == nil {
			err = manifest.DocumentFailure(docs)
		}
		if err == nil {
			continue
		}
		// Full-stream diagnostics preserve Helm's line numbers across CRD files.
		readers := make([]io.Reader, len(crds))
		for i, crd := range crds {
			readers[i] = crdReader(crd)
		}
		docs, fullErr := manifest.DecodeDocs(io.MultiReader(readers...), manifest.RequiredDocuments)
		for _, doc := range docs {
			manifest.ReleaseDoc(doc.Document)
		}
		if fullErr == nil {
			fullErr = manifest.DocumentFailure(docs)
		}
		if fullErr != nil {
			return fullErr
		}
		return err
	}
	return nil
}

func sourcePrefix(name string) string {
	return "---\n" + sourceHeader + name + "\n"
}

func crdReader(crd chart.CRD) io.Reader {
	return io.MultiReader(strings.NewReader(sourcePrefix(crd.Filename)),
		bytes.NewReader(crd.File.Data), strings.NewReader("\n"))
}

// Placeholder values cannot satisfy constraints intended for resolved secrets.
func schemaValidationSkipped(hr *manifest.HelmRelease) bool {
	return hr.DisableSchemaValidation
}

func newInstallAction(
	cfg *action.Configuration,
	hr *manifest.HelmRelease,
	opts Options,
	caps *common.Capabilities,
) (*action.Install, bool) {
	inst := action.NewInstall(cfg)
	inst.DryRunStrategy = action.DryRunClient
	inst.ReleaseName = hr.ReleaseName()
	inst.Namespace = hr.ReleaseNamespace()
	// Match helm-controller: Devel=true so chart references pinned to a
	// prerelease semver (e.g. `1.0.0-beta`) resolve. Without this, the
	// HR renders cleanly in flate (which doesn't consult Devel for local
	// chart resolution) but fails in cluster, or vice versa.
	inst.Devel = true
	policy := (&helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}).GetInstall()
	inst.IncludeCRDs = !opts.SkipCRDs && policy.CRDs != helmv2.Skip
	inst.DisableHooks = policy.DisableHooks
	inst.EnableDNS = opts.EnableDNS
	inst.Replace = policy.Replace
	inst.DisableOpenAPIValidation = policy.DisableOpenAPIValidation
	if renderer := newPostRenderer(hr.PostRenderers); renderer != nil {
		inst.PostRenderer = renderer
	}
	// action.Install consults its own KubeVersion field for chart
	// compatibility checks and ignores cfg.Capabilities for that purpose.
	inst.KubeVersion = &caps.KubeVersion
	// Same for APIVersions — action.Install under DryRunClient
	// replaces cfg.Capabilities with a fresh default copy and then
	// only re-applies inst.APIVersions onto that copy. Setting
	// cfg.Capabilities alone leaves APIVersions empty at render
	// time, silently dropping the user's --api-versions flag.
	if len(caps.APIVersions) > 0 {
		inst.APIVersions = caps.APIVersions
	}
	if hr.Chart.Version != "" {
		inst.Version = hr.Chart.Version
	}
	return inst, inst.DisableHooks
}

type chartValuesFile struct {
	Data     []byte         `json:"Data"`
	Defaults map[string]any `json:"Defaults"`
	Missing  bool           `json:"Missing"`
}

func (l *chartDescription) readValuesFile(ctx context.Context, name, clean string) (chartValuesFile, error) {
	if err := ctx.Err(); err != nil {
		return chartValuesFile{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("read values file %q: %w", name, err),
			"",
		)
	}
	var data []byte
	var err error
	var missing bool
	switch {
	case l.valuesFS != nil:
		var f fs.File
		f, err = l.valuesFS.Open(clean)
		missing = errors.Is(err, fs.ErrNotExist)
		if err == nil {
			info, statErr := f.Stat()
			if statErr == nil && !info.Mode().IsRegular() {
				statErr = fs.ErrInvalid
			}
			if statErr == nil {
				data, err = io.ReadAll(f)
			} else {
				err = statErr
			}
			err = errors.Join(err, f.Close())
		}
	case clean == "values.yaml":
		return chartValuesFile{Defaults: l.chart.Values}, nil
	default:
		err, missing = fs.ErrNotExist, true
		for _, file := range l.chart.Files {
			if file != nil && file.Name == clean {
				data, err, missing = file.Data, nil, false
				break
			}
		}
	}
	if err != nil {
		return chartValuesFile{
			Missing: missing,
		}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("read values file %q: %w", name, err),
			"",
		)
	}
	return chartValuesFile{Data: data}, nil
}

type chartValuesEntry struct {
	key         string
	fingerprint *string
	names       []string
	inputs      []chartValuesFile
	merged      map[string]any
}

func (c *Client) observeValuesFiles(
	ctx context.Context,
	loaded *chartDescription,
	names []string,
	ignoreMissing bool,
) (chartValuesEntry, error) {
	if len(names) == 0 {
		return chartValuesEntry{}, nil
	}
	normalizeValuesFilesResult1, normalizeValuesFilesResult1Err := normalizeValuesFiles(names)
	cleaned, normalized, normalizeErr := normalizeValuesFilesResult1.names, normalizeValuesFilesResult1.normalized, normalizeValuesFilesResult1Err
	if normalizeErr != nil {
		return chartValuesEntry{}, normalizeErr
	}
	if err := ctx.Err(); err != nil {
		return chartValuesEntry{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("read values file %q: %w", names[0], err),
			"",
		)
	}
	var key string
	if loaded.valuesFS == nil || loaded.valuesPin != "" {
		key = chartValuesCacheKey(loaded, cleaned, ignoreMissing, "")
	}
	if key != "" {
		c.chartMu.RLock()
		cached := c.chartValuesCache[key]
		c.chartMu.RUnlock()
		if cached != nil {
			return *cached, nil
		}
	}
	entry := chartValuesEntry{key: key, names: cleaned}
	if loaded.valuesFS == nil {
		return entry, nil
	}
	if err := entry.observe(ctx, loaded, names, ignoreMissing); err != nil {
		return chartValuesEntry{}, err
	}
	fingerprint := manifest.Fingerprint(entry.inputs)
	entry.fingerprint = &fingerprint
	entry.key = chartValuesCacheKey(loaded, cleaned, ignoreMissing, fingerprint)
	if entry.key != "" {
		if !normalized {
			entry.names = slices.Clone(names)
		}
		published := entry
		c.chartMu.Lock()
		if existing := c.chartValuesCache[entry.key]; existing != nil {
			entry = *existing
		} else {
			c.chartValuesCache[entry.key] = &published
		}
		c.chartMu.Unlock()
	}
	return entry, nil
}

func (c *Client) mergeChartValuesFiles(
	ctx context.Context,
	loaded *chartDescription,
	names []string,
	ignoreMissing bool,
) (map[string]any, error) {
	if loaded.valuesErr != nil {
		return nil, loaded.valuesErr
	}
	entry := loaded.values
	if entry.names == nil {
		var err error
		entry, err = c.observeValuesFiles(ctx, loaded, names, ignoreMissing)
		if err != nil {
			return nil, err
		}
	}
	if entry.names == nil {
		return map[string]any{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("read values file %q: %w", names[0], err), "")
	}
	if entry.merged != nil {
		return manifest.DeepCopyMap(entry.merged), nil
	}
	merged, mergeErr := entry.merge(ctx, loaded, names, ignoreMissing)
	if mergeErr != nil {
		return nil, mergeErr
	}
	if entry.key == "" {
		return merged, nil
	}
	published := entry
	published.merged = merged
	if entry.inputs == nil {
		published.names = slices.Clone(entry.names)
	}
	c.chartMu.Lock()
	if existing := c.chartValuesCache[entry.key]; existing != nil && existing.merged != nil {
		merged = existing.merged
	} else {
		c.chartValuesCache[entry.key] = &published
	}
	c.chartMu.Unlock()
	return manifest.DeepCopyMap(merged), nil
}

func chartValuesCacheKey(loaded *chartDescription, names []string, ignoreMissing bool, inputs string) string {
	if loaded.fingerprint == "" {
		return ""
	}
	if loaded.valuesPin != "" {
		inputs = ""
	}
	return manifest.Fingerprint(struct {
		Chart         string           `json:"Chart"`
		OCI           *ociChartContent `json:"OCI"`
		Names         []string         `json:"Names"`
		IgnoreMissing bool             `json:"IgnoreMissing"`
		Source        bool             `json:",omitempty"`
		Pin           string           `json:",omitempty"`
		Inputs        string           `json:",omitempty"`
	}{loaded.fingerprint, loaded.oci, names, ignoreMissing, loaded.valuesFS != nil, loaded.valuesPin, inputs})
}

func releaseManifest(rel *release.Release, disableHooks, skipTests bool) string {
	// Reserve hook framing as well as bodies to avoid repeated growth on large releases.
	size := len(rel.Manifest)
	if !disableHooks {
		for _, h := range rel.Hooks {
			if skipTests && slices.Contains(h.Events, release.HookTest) {
				continue
			}
			size += len("---\n"+sourceHeader+"\n") + len(h.Path) + len(h.Manifest) + 1
		}
	}
	var b strings.Builder
	b.Grow(size)
	b.WriteString(rel.Manifest)
	lastNewline := strings.HasSuffix(rel.Manifest, "\n")
	if !disableHooks {
		for _, h := range rel.Hooks {
			if skipTests && slices.Contains(h.Events, release.HookTest) {
				continue
			}
			if !lastNewline {
				b.WriteByte('\n')
			}
			b.WriteString(sourcePrefix(h.Path))
			b.WriteString(h.Manifest)
			lastNewline = strings.HasSuffix(h.Manifest, "\n")
		}
	}
	return b.String()
}

func (r *templateRequest) render(
	ctx context.Context,
	c *Client,
	cacheErr error,
) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
	decodeChartErr := c.decodeChart(ctx, &r.loaded, &r.input)
	if decodeChartErr != nil {
		return nil, nil, decodeChartErr
	}
	var trackedVersion string
	if r.release.TracksDigest() {
		version, ociChartVersionErr := ociChartVersion(r.loaded.chart.Metadata.Version, r.loaded.artifact.Revision)
		if ociChartVersionErr != nil {
			return nil, nil, fmt.Errorf("track oci chart version: %w", ociChartVersionErr)
		}
		if !r.input.Options.DisableChartDigestTracking {
			trackedVersion = version
		}
	}
	if r.capabilityErr != nil {
		return nil, nil, r.capabilityErr
	}
	finalValues := r.values
	if len(r.release.ChartValuesFiles) > 0 {
		base, mergeChartValuesFilesErr := c.mergeChartValuesFiles(
			ctx, &r.loaded, r.release.ChartValuesFiles,
			r.release.IgnoreMissingValuesFiles,
		)
		if mergeChartValuesFilesErr != nil {
			return nil, nil, fmt.Errorf("chart valuesFiles: %w", mergeChartValuesFilesErr)
		}
		finalValues = values.DeepMergeInto(base, r.values)
	}
	if cacheErr != nil {
		return nil, nil, cacheErr
	}

	ch := cloneChartForRender(r.loaded.chart)
	if trackedVersion != "" {
		ch.Metadata.Version = trackedVersion
	}

	out, installErr := r.install(ctx, ch, finalValues)
	if installErr != nil {
		return nil, nil, installErr
	}
	docs, decodeChartErr := manifest.DecodeDocs(strings.NewReader(out), manifest.RequiredDocuments)
	if decodeChartErr != nil {
		return nil, nil, decodeChartErr
	}
	var receipt *manifest.RenderReceipt
	if r.key != "" && manifest.DocumentFailure(docs) == nil && utf8.ValidString(out) &&
		validChartTemplates(r.loaded.chart) {
		payload, encodeRenderErr := manifest.EncodeRender(docs)
		if encodeRenderErr != nil {
			return nil, nil, encodeRenderErr
		}
		frame := renderFrame{
			Key: r.key, Output: payload, Inputs: r.binding,
			Header: renderHeader{
				Analysis: r.loaded.analysis, chart: r.loaded.chart,
				persist: c.cache.CanPersistRenders(),
				content: r.release.Clone().Content(r.key, r.input.Omitted),
			},
		}
		receipt = renderDomain.Receipt(frame)
	}
	return normalizeLists(docs), receipt, nil
}

func (r *templateRequest) validate(
	ctx context.Context,
	c *Client,
	result *templateResult,
	generation fs.FS,
) (bool, error) {
	identity := cache.AcquisitionIdentity(generation)
	record := result.produced
	if record == nil {
		var err error
		record, err = r.cachedRecord(ctx, generation, identity)
		if err != nil || record == nil {
			return false, err
		}
	} else {
		record.identity = identity
	}
	current, documents, err := r.validateRecord(ctx, record, &r.binding, false)
	if err != nil || current == nil {
		return false, err
	}
	if current != r.loaded.record {
		c.rememberDescription(&r.loaded, current)
	}
	result.docs, result.receipt, result.record = documents, current.receipt, current
	return true, nil
}

func (r *templateRequest) validateRecord(
	ctx context.Context,
	record *renderRecord,
	binding *cache.RenderBinding[templatePolicy],
	history bool,
) (*renderRecord, []manifest.DocOccurrence, error) {
	frame := record.frame
	bindingMatches := frame.Inputs.Equal(binding, equalTemplatePolicy)
	if record.accepted != nil && (history || !bindingMatches) {
		frame.RenderFrame = *record.accepted
		frame.Inputs = cache.RenderBinding[templatePolicy]{}
	}
	raw := frame.RenderFrame
	check := func(header renderHeader) (bool, error) {
		if header.Analysis.RequiresLoader {
			if err := r.client.decodeChart(ctx, &r.loaded, &r.input); err != nil {
				return false, err
			}
		}
		if r.loaded.oci != nil {
			if _, err := ociChartVersion(header.Analysis.Version, r.loaded.artifact.Revision); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	validated, err := renderDomain.Validate(ctx, &frame, binding, cache.RenderCheck[renderHeader]{
		History: history,
		Header:  check,
	})
	if err != nil || !validated.Valid {
		return nil, nil, err
	}
	if !bindingMatches || record.receipt == nil || !manifest.ValueEqual(frame.Header.content, r.content) {
		fresh := *record
		// A rebound receipt MUST NOT displace the durable generation for its original binding.
		if validated.Rebound && raw.Stored != nil && raw.Binding != "" && fresh.accepted == nil {
			fresh.accepted = new(raw)
		}
		frame.Header.content = r.release.Clone().Content(r.key, r.input.Omitted)
		frame.Header.persist = r.client.cache.CanPersistRenders()
		fresh.frame = frame
		fresh.receipt = renderDomain.Receipt(frame)
		record = &fresh
	}
	return record, validated.Documents, nil
}

func (r *templateRequest) produce(
	ctx context.Context,
	c *Client,
	result *templateResult,
	w cache.ProducerInput,
) (cache.ProducerResult, error) {
	var errResult2 error
	result.docs, result.receipt, errResult2 = r.render(ctx, c, nil)
	if errResult2 != nil {
		return cache.ProducerResult{}, errResult2
	}
	if result.receipt == nil {
		// Incomplete renders MUST fail frame validation to preserve each caller's local output.
		return cache.ProducerResult{}, nil
	}
	//nolint:errcheck // This cache stores only the exact concrete type asserted at this boundary.
	frame := result.receipt.Value().(renderFrame)
	errResult2 = renderDomain.WritePending(ctx, w, &frame)
	if errResult2 == nil && frame.Stored != nil {
		frame.Header.chart = nil
		result.produced = &renderRecord{
			Output:   frame.Output,
			Analysis: frame.Header.Analysis,
			frame:    frame,
		}
	}
	return cache.ProducerResult{}, errResult2
}

func (r *templateRequest) install(ctx context.Context, ch *chart.Chart, finalValues map[string]any) (string, error) {
	cfg := new(action.Configuration)
	if initErr := cfg.Init(envSettings().RESTClientGetter(), r.release.ReleaseNamespace(), ""); initErr != nil {
		return "", fmt.Errorf("helm init: %w", initErr)
	}
	cfg.Capabilities = r.caps
	// Helm applies CustomTemplateFuncs last. Each render needs its own stateful
	// random stream, seeded by the release name and namespace in the cache key.
	cfg.CustomTemplateFuncs = deterministic.Funcs(
		deterministic.SeedFor(r.release.ReleaseName(), r.release.ReleaseNamespace()),
	)

	inst, disableHooks := newInstallAction(cfg, r.release, r.input.Options, r.caps)
	if renderer, ok := inst.PostRenderer.(*kustomizePostRenderer); ok {
		renderer.work = r.input.Work.PostRender(r.key)
	}
	inst.SkipSchemaValidation = schemaValidationSkipped(r.release)

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("helm: render", "release", r.release.Named().String())
	}
	work := r.input.Work.Engine(manifest.WorkHelm, r.key)
	work.Add(&manifest.WorkCounts{Attempts: 1})
	rel, decodeChartErr := inst.RunWithContext(ctx, ch, finalValues)
	work.Finish(decodeChartErr)
	if decodeChartErr != nil {
		return "", &RenderError{Err: schemaError(decodeChartErr)}
	}
	relV1, ok := rel.(*release.Release)
	if !ok {
		return "", fmt.Errorf("helm template: unexpected release type %T", rel)
	}

	// Disabled dependencies must be pruned before validating their CRDs.
	policy := (&helmv2.HelmRelease{Spec: r.release.HelmReleaseSpec}).GetInstall()
	if r.input.Options.SkipCRDs && policy.CRDs != helmv2.Skip {
		if validateCRDsErr := validateCRDs(ch); validateCRDsErr != nil {
			return "", validateCRDsErr
		}
	}

	skipTests := r.input.Options.SkipTests || r.release.Test == nil || !r.release.Test.Enable
	return releaseManifest(relV1, disableHooks, skipTests), nil
}

func (r *templateRequest) cachedRecord(ctx context.Context, generation fs.FS, identity string) (*renderRecord, error) {
	record := r.loaded.record
	if record == nil || identity == "" || record.identity != identity {
		frame, decoded, err := renderDomain.Read(ctx, generation)
		if err != nil || !decoded {
			return nil, err
		}
		record = &renderRecord{
			identity: identity, Output: frame.Output, Analysis: frame.Header.Analysis, frame: frame,
		}
	}
	return record, nil
}

func (e *chartValuesEntry) observe(
	ctx context.Context,
	loaded *chartDescription,
	names []string,
	ignoreMissing bool,
) error {
	e.inputs = make([]chartValuesFile, len(names))
	observed := make(map[string]chartValuesFile, len(names))
	for i, name := range names {
		clean := e.names[i]
		file, ok := observed[clean]
		if !ok {
			var err error
			file, err = loaded.readValuesFile(ctx, name, clean)
			if err != nil && (!ignoreMissing || !file.Missing) {
				return err
			}
			observed[clean] = file
		}
		e.inputs[i] = file
	}
	if err := ctx.Err(); err != nil {
		return manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("read values file %q: %w", names[len(names)-1], err),
			"",
		)
	}
	return nil
}

func (e *chartValuesEntry) merge(
	ctx context.Context,
	loaded *chartDescription,
	names []string,
	ignoreMissing bool,
) (map[string]any, error) {
	merged := map[string]any{}
	for i, name := range names {
		var file chartValuesFile
		if e.inputs != nil {
			file = e.inputs[i]
		} else {
			var err error
			file, err = loaded.readValuesFile(ctx, name, e.names[i])
			if file.Missing && ignoreMissing {
				err = nil
			}
			if err != nil {
				return nil, err
			}
		}
		if file.Missing {
			continue
		}
		m := manifest.DeepCopyMap(file.Defaults)
		if file.Defaults == nil {
			if err := yaml.Unmarshal(file.Data, &m); err != nil {
				return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("parse values file %q: %w", name, err), "")
			}
		}
		merged = values.DeepMergeInto(merged, m)
	}
	return merged, nil
}
