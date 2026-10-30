package helm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"path"
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

	"github.com/home-operations/flate/pkg/helm/deterministic"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/values"
)

var envSettings = sync.OnceValue(cli.New)

const sourceHeader = "# Source: "

var errUncacheableRender = errors.New("uncacheable helm render")

// Template renders a HelmRelease and returns independently owned documents.
// The immutable receipt may be persisted after complete accepted publication.
func (c *Client) Template(ctx context.Context, hr *manifest.HelmRelease, hrValues map[string]any, input RenderOptions) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
	prepared, err := c.prepareTemplate(ctx, hr, hrValues, input)
	if err != nil {
		return nil, nil, err
	}
	loaded, caps, capabilityErr, key := prepared.loaded, prepared.caps, prepared.capabilityErr, prepared.key
	hrValues, opts := prepared.values, input.Options
	if input.Admit != nil {
		if handled, err := input.Admit(key); handled || err != nil {
			return nil, nil, err
		}
	}
	render := func(ctx context.Context, cacheErr error) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
		err := c.decodeChart(ctx, &loaded)
		if err != nil {
			return nil, nil, err
		}
		var trackedVersion string
		if hr.TracksDigest() {
			version, err := ociChartVersion(loaded.chart.Metadata.Version, loaded.artifact.Revision)
			if err != nil {
				return nil, nil, fmt.Errorf("track oci chart version: %w", err)
			}
			if !opts.DisableChartDigestTracking {
				trackedVersion = version
			}
		}
		if capabilityErr != nil {
			return nil, nil, capabilityErr
		}
		finalValues := hrValues
		if len(hr.ChartValuesFiles) > 0 {
			base, err := c.mergeChartValuesFiles(ctx, loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
			if err != nil {
				return nil, nil, fmt.Errorf("helm chart valuesFiles %s/%s: %w", hr.Namespace, hr.Name, err)
			}
			finalValues = values.DeepMergeInto(base, hrValues)
		}
		if cacheErr != nil {
			return nil, nil, cacheErr
		}

		ch := cloneChartForRender(loaded.chart)
		if trackedVersion != "" {
			ch.Metadata.Version = trackedVersion
		}

		cfg := new(action.Configuration)
		if err := cfg.Init(envSettings().RESTClientGetter(), hr.ReleaseNamespace(), ""); err != nil {
			return nil, nil, fmt.Errorf("helm init: %w", err)
		}
		cfg.Capabilities = caps
		// Helm applies CustomTemplateFuncs last. Each render needs its own stateful
		// random stream, seeded by the release name and namespace in the cache key.
		cfg.CustomTemplateFuncs = deterministic.Funcs(
			deterministic.SeedFor(hr.ReleaseName(), hr.ReleaseNamespace()),
		)

		inst, disableHooks := newInstallAction(cfg, hr, opts, caps)
		inst.SkipSchemaValidation = schemaValidationSkipped(hr, hrValues)

		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.Debug("helm: render", "release", hr.Named().String())
		}
		rel, err := inst.RunWithContext(ctx, ch, finalValues)
		if err != nil {
			return nil, nil, fmt.Errorf("helm template %s/%s: %w", hr.Namespace, hr.Name, schemaError(err))
		}
		relV1, ok := rel.(*release.Release)
		if !ok {
			return nil, nil, fmt.Errorf("helm template %s/%s: unexpected release type %T", hr.Namespace, hr.Name, rel)
		}

		// Disabled dependencies must be pruned before validating their CRDs.
		policy := (&helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}).GetInstall()
		if opts.SkipCRDs && policy.CRDs != helmv2.Skip {
			if err := validateCRDs(ch); err != nil {
				return nil, nil, err
			}
		}

		skipTests := opts.SkipTests || hr.Test == nil || !hr.Test.Enable
		out := releaseManifest(relV1, disableHooks, skipTests)
		docs, err := manifest.DecodeDocs(strings.NewReader(out), manifest.RequiredDocuments)
		if err != nil {
			return nil, nil, err
		}
		var receipt *manifest.RenderReceipt
		if key != "" && manifest.DocumentFailure(docs) == nil && utf8.ValidString(out) && validChartTemplates(loaded.chart) {
			payload, err := manifest.EncodeRender(docs)
			if err != nil {
				return nil, nil, err
			}
			frame := renderFrame{header: renderHeader{Key: key, Analysis: loaded.analysis}, binding: prepared.binding, chart: loaded.chart, output: payload}
			receipt = manifest.NewRenderReceipt(frame, equalRenderFrame)
		}
		return normalizeLists(docs), receipt, nil
	}
	if key == "" || loaded.valuesErr != nil || capabilityErr != nil {
		return render(ctx, nil)
	}
	// Callback state MUST share one allocation to preserve cache-hit allocation counts.
	var result struct {
		docs     []manifest.DocOccurrence
		produced *renderRecord
		receipt  *manifest.RenderReceipt
	}
	blobKey := cache.BlobKey{Content: key, Render: cache.RenderPending, Validate: func(ctx context.Context, generation fs.FS) (bool, error) {
		identity := cache.AcquisitionIdentity(generation)
		record := result.produced
		if record == nil {
			record = loaded.record
			if record == nil || record.key != key || record.identity != identity {
				data, err := fs.ReadFile(generation, "content")
				if err != nil {
					return false, fmt.Errorf("%w: read helm render cache: %w", manifest.ErrFlux, err)
				}
				frame, err := decodeFrame(data)
				if err != nil || frame.header.Key != key || frame.header.Analysis == nil {
					return false, nil
				}
				frame.identity = identity
				record = &renderRecord{key: key, identity: identity, Output: frame.output, Analysis: frame.header.Analysis, frame: frame}
			}
			var err error
			result.docs, err = manifest.DecodeRender(record.Output)
			if err != nil {
				return false, nil
			}
		} else {
			record.identity = identity
			record.frame.identity = identity
		}
		if record.Analysis.RequiresLoader {
			if err := c.decodeChart(ctx, &loaded); err != nil {
				return false, err
			}
		}
		if loaded.oci != nil {
			if _, err := ociChartVersion(record.Analysis.Version, loaded.artifact.Revision); err != nil {
				return false, err
			}
		}
		if !record.binding.same(prepared.binding) || record.receipt == nil {
			fresh := *record
			fresh.binding = prepared.binding
			frame := record.frame
			frame.binding = prepared.binding
			binding := manifest.Fingerprint(prepared.binding)
			if frame.header.Binding != binding {
				frame.header.Binding, frame.encoded, frame.raw = binding, nil, ""
				frame.accepted, frame.acceptedKey, frame.identity = nil, "", ""
			}
			fresh.receipt = manifest.NewRenderReceipt(frame, equalRenderFrame)
			record = &fresh
		}
		if record != loaded.record {
			c.rememberDescription(loaded, record)
		}
		result.receipt = record.receipt
		return true, nil
	}}
	_, _, _, err = c.cache.Blob(ctx, blobKey, cache.Policy{MaxAge: cache.Forever}, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		var err error
		result.docs, result.receipt, err = render(ctx, nil)
		if err != nil {
			return cache.ProducerResult{}, err
		}
		if result.receipt == nil {
			return cache.ProducerResult{}, errUncacheableRender
		}
		frame := result.receipt.Value().(renderFrame)
		if frame.header.Analysis == nil {
			frame.header.Analysis = loaded.analyze()
		}
		data, err := encodeFrame(frame.header, frame.output)
		if err == nil {
			_, err = w.Write(data)
		}
		if err == nil {
			frame.chart = nil
			result.produced = &renderRecord{key: key, Output: frame.output, Analysis: frame.header.Analysis, frame: frame}
		}
		return cache.ProducerResult{}, err
	})
	if err != nil {
		if _, storage := errors.AsType[*cache.CacheError](err); !storage {
			if errors.Is(err, errUncacheableRender) {
				return result.docs, nil, nil
			}
			return nil, nil, err
		}
		return render(ctx, err)
	}
	return normalizeLists(result.docs), result.receipt, nil
}

type templateRequest struct {
	loaded        chartDescription
	caps          *common.Capabilities
	capabilityErr error
	key           string
	binding       templateBinding
	values        map[string]any
}

func (c *Client) prepareTemplate(ctx context.Context, hr *manifest.HelmRelease, hrValues map[string]any, input RenderOptions) (templateRequest, error) {
	opts := input.Options
	if err := ctx.Err(); err != nil {
		return templateRequest{}, err
	}
	loaded, err := c.describeChart(ctx, hr, input.Sources)
	if err != nil {
		return templateRequest{}, err
	}
	if hr.TracksDigest() {
		loaded.oci = &ociChartContent{Digest: loaded.artifact.Digest, Revision: loaded.artifact.Revision, DisableTracking: opts.DisableChartDigestTracking}
	}
	caps, capabilityErr := opts.capabilities()
	if loaded.valuesFS != nil {
		loaded.values, loaded.valuesErr = c.observeValuesFiles(ctx, loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
	}
	if hrValues == nil {
		hrValues = map[string]any{}
	}
	key := computeTemplateKey(loaded, hrValues, opts, hr)
	binding := templateBinding{Policy: hr.HistoryPolicy(), Source: hr.ChartSource(), Kind: loaded.artifact.Kind, URL: loaded.artifact.URL, Path: loaded.artifact.LocalPath, Root: loaded.artifact.LocalRoot, Key: loaded.artifact.Key, Revision: loaded.artifact.Revision, Digest: loaded.artifact.Digest, Alias: loaded.artifact.InputAlias, Options: input.Options, Wipe: input.WipeSecrets, Dependencies: input.DependencySubstitutions}
	if loaded.record != nil && loaded.record.key == key && loaded.record.binding.same(binding) {
		binding = loaded.record.binding
	} else {
		binding.Options.SkipKinds = nil
		if len(input.SkipKinds) > 0 {
			binding.Options.SkipKinds = slices.Clone(input.SkipKinds)
		}
	}
	return templateRequest{loaded: loaded, caps: caps, capabilityErr: capabilityErr, key: key, values: hrValues, binding: binding}, nil
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
			doc.Err = fmt.Errorf("%w: encode helm list: %w", manifest.ErrInput, err)
			out = append(out, doc)
			continue
		}
		infos, err := resource.NewLocalBuilder().ContinueOnError().Unstructured().Flatten().Stream(bytes.NewReader(data), "").Do().Infos()
		if err != nil {
			doc.Err = fmt.Errorf("%w: normalize helm list: %w", manifest.ErrInput, err)
			out = append(out, doc)
			continue
		}
		for _, info := range infos {
			out = append(out, manifest.DocOccurrence{Document: info.Object.(*unstructured.Unstructured).Object, Ordinal: doc.Ordinal})
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
func schemaValidationSkipped(hr *manifest.HelmRelease, values map[string]any) bool {
	return hr.DisableSchemaValidation || manifest.ContainsValuePlaceholder(values)
}

func newInstallAction(cfg *action.Configuration, hr *manifest.HelmRelease, opts Options, caps *common.Capabilities) (*action.Install, bool) {
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
	// inst.SkipSchemaValidation is values-dependent, so Template sets it once
	// the effective values are resolved (see schemaValidationSkipped).
	// spec.postRenderers — pipe rendered output through one or more
	// kustomize patch+image transforms. helm-controller does this via
	// the same postrenderer.PostRenderer hook.
	inst.PostRenderer = newPostRenderer(hr.PostRenderers)
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
	Data     []byte
	Defaults map[string]any
	Missing  bool
}

func (l chartDescription) readValuesFile(ctx context.Context, name, clean string) (chartValuesFile, error) {
	if err := ctx.Err(); err != nil {
		return chartValuesFile{}, fmt.Errorf("%w: read values file %q: %w", manifest.ErrInput, name, err)
	}
	var data []byte
	var err error
	var missing bool
	if l.valuesFS != nil {
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
	} else if clean == "values.yaml" {
		return chartValuesFile{Defaults: l.chart.Values}, nil
	} else {
		err, missing = fs.ErrNotExist, true
		for _, file := range l.chart.Files {
			if file != nil && file.Name == clean {
				data, err, missing = file.Data, nil, false
				break
			}
		}
	}
	if err != nil {
		return chartValuesFile{Missing: missing}, fmt.Errorf("%w: read values file %q: %w", manifest.ErrInput, name, err)
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

func (c *Client) observeValuesFiles(ctx context.Context, loaded chartDescription, names []string, ignoreMissing bool) (chartValuesEntry, error) {
	if len(names) == 0 {
		return chartValuesEntry{}, nil
	}
	cleaned := names
	normalized := false
	for i, name := range names {
		clean := path.Clean(name)
		if name == "" || !fs.ValidPath(clean) || clean == "." || strings.IndexByte(clean, 0) >= 0 {
			return chartValuesEntry{}, fmt.Errorf("%w: read values file %q: %w", manifest.ErrInput, name, fs.ErrInvalid)
		}
		if clean != name {
			if !normalized {
				cleaned = slices.Clone(names)
				normalized = true
			}
			cleaned[i] = clean
		}
	}
	if err := ctx.Err(); err != nil {
		return chartValuesEntry{}, fmt.Errorf("%w: read values file %q: %w", manifest.ErrInput, names[0], err)
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
	entry.inputs = make([]chartValuesFile, len(names))
	observed := make(map[string]chartValuesFile, len(names))
	for i, name := range names {
		clean := cleaned[i]
		file, ok := observed[clean]
		if !ok {
			var err error
			file, err = loaded.readValuesFile(ctx, name, clean)
			if err != nil && (!ignoreMissing || !file.Missing) {
				return chartValuesEntry{}, err
			}
			observed[clean] = file
		}
		entry.inputs[i] = file
	}
	if err := ctx.Err(); err != nil {
		return chartValuesEntry{}, fmt.Errorf("%w: read values file %q: %w", manifest.ErrInput, names[len(names)-1], err)
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

func (c *Client) mergeChartValuesFiles(ctx context.Context, loaded chartDescription, names []string, ignoreMissing bool) (map[string]any, error) {
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
		return nil, fmt.Errorf("%w: read values file %q: %w", manifest.ErrInput, names[0], err)
	}
	if entry.merged != nil {
		return manifest.DeepCopyMap(entry.merged), nil
	}
	merged := map[string]any{}
	for i, name := range names {
		var file chartValuesFile
		if entry.inputs != nil {
			file = entry.inputs[i]
		} else {
			var err error
			file, err = loaded.readValuesFile(ctx, name, entry.names[i])
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
				return nil, fmt.Errorf("%w: parse values file %q: %w", manifest.ErrInput, name, err)
			}
		}
		merged = values.DeepMergeInto(merged, m)
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

func chartValuesCacheKey(loaded chartDescription, names []string, ignoreMissing bool, inputs string) string {
	if loaded.fingerprint == "" {
		return ""
	}
	if loaded.valuesPin != "" {
		inputs = ""
	}
	return manifest.Fingerprint(struct {
		Chart         string
		OCI           *ociChartContent
		Names         []string
		IgnoreMissing bool
		Source        bool   `json:",omitempty"`
		Pin           string `json:",omitempty"`
		Inputs        string `json:",omitempty"`
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
