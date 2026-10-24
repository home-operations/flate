package helm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/home-operations/flate/pkg/values"
)

var envSettings = sync.OnceValue(cli.New)

const sourceHeader = "# Source: "

// Template renders a HelmRelease and returns independently owned documents.
// The immutable receipt may be persisted after complete accepted publication.
func (c *Client) Template(ctx context.Context, hr *manifest.HelmRelease, hrValues map[string]any, input RenderOptions) ([]map[string]any, *manifest.RenderReceipt, error) {
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
	var cacheErr error
	if key != "" && loaded.valuesErr == nil && capabilityErr == nil {
		record, lookupErr := c.lookupRender(ctx, &loaded, key)
		cacheErr = lookupErr
		if record != nil {
			docs, decodeErr := manifest.DecodeRender(record.Output)
			if decodeErr == nil {
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
				docs, err = normalizeLists(docs)
				return docs, record.receipt, err
			}
		}
	}

	err = c.decodeChart(ctx, &loaded)
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
	inst.SkipSchemaValidation = schemaValidationSkipped(opts, hr, hrValues)

	rel, err := inst.RunWithContext(ctx, ch, finalValues)
	if err != nil {
		return nil, nil, fmt.Errorf("helm template %s/%s: %w", hr.Namespace, hr.Name, err)
	}
	relV1, ok := rel.(*release.Release)
	if !ok {
		return nil, nil, fmt.Errorf("helm template %s/%s: unexpected release type %T", hr.Namespace, hr.Name, rel)
	}

	// Disabled dependencies must be pruned before validating their CRDs.
	if opts.SkipCRDs && hr.CRDsPolicy != string(helmv2.Skip) {
		if err := validateCRDs(ch); err != nil {
			return nil, nil, err
		}
	}

	skipTests := opts.SkipTests || hr.Test == nil || !hr.Test.Enable
	out := releaseManifest(relV1, opts, disableHooks, skipTests)
	docs, err := manifest.SplitDocs([]byte(out))
	if err != nil {
		return nil, nil, err
	}
	var receipt *manifest.RenderReceipt
	if key != "" && utf8.ValidString(out) && validChartTemplates(loaded.chart) {
		payload, err := manifest.EncodeRender(docs)
		if err != nil {
			return nil, nil, err
		}
		frame := renderFrame{header: renderHeader{Key: key, Analysis: loaded.analysis}, binding: prepared.binding, chart: loaded.chart, output: payload}
		receipt = manifest.NewRenderReceipt(frame, equalRenderFrame)
	}
	docs, err = normalizeLists(docs)
	if err != nil {
		return nil, nil, err
	}
	return docs, receipt, nil
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
		binding.Options.SkipKinds, binding.Options.ShowOnly = nil, nil
		if len(input.SkipKinds) > 0 {
			binding.Options.SkipKinds = slices.Clone(input.SkipKinds)
		}
		if len(input.ShowOnly) > 0 {
			binding.Options.ShowOnly = slices.Clone(input.ShowOnly)
		}
	}
	return templateRequest{loaded: loaded, caps: caps, capabilityErr: capabilityErr, key: key, values: hrValues, binding: binding}, nil
}

func normalizeLists(docs []map[string]any) ([]map[string]any, error) {
	var out []map[string]any
	for i, doc := range docs {
		if _, selected := doc["items"]; !selected {
			if out != nil {
				out = append(out, doc)
			}
			continue
		}
		if out == nil {
			out = make([]map[string]any, 0, len(docs))
			out = append(out, docs[:i]...)
		}
		data, err := yamlv4.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("%w: encode helm list: %w", manifest.ErrInput, err)
		}
		infos, err := resource.NewLocalBuilder().ContinueOnError().Unstructured().Flatten().Stream(bytes.NewReader(data), "").Do().Infos()
		if err != nil {
			return nil, fmt.Errorf("%w: normalize helm list: %w", manifest.ErrInput, err)
		}
		for _, info := range infos {
			out = append(out, info.Object.(*unstructured.Unstructured).Object)
		}
	}
	if out == nil {
		return docs, nil
	}
	return out, nil
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
		docs, err := manifest.DecodeDocs(crdReader(crd))
		for _, doc := range docs {
			manifest.ReleaseDoc(doc)
		}
		if err == nil {
			continue
		}
		// Full-stream diagnostics preserve Helm's line numbers across CRD files.
		readers := make([]io.Reader, len(crds))
		for i, crd := range crds {
			readers[i] = crdReader(crd)
		}
		docs, fullErr := manifest.DecodeDocs(io.MultiReader(readers...))
		for _, doc := range docs {
			manifest.ReleaseDoc(doc)
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

// schemaValidationSkipped uses the selected action's policy. Placeholder values
// cannot be validated against constraints intended for resolved secrets.
func schemaValidationSkipped(opts Options, hr *manifest.HelmRelease, values map[string]any) bool {
	disabled := hr.DisableSchemaValidation
	if opts.IsUpgrade {
		disabled = hr.Upgrade != nil && hr.Upgrade.DisableSchemaValidation
	}
	return disabled || manifest.ContainsValuePlaceholder(values)
}

// newInstallAction builds the DryRunClient action.Install that Template
// renders through, mirroring helm-controller's install/upgrade field
// wiring. Returns the configured install, the effective disableHooks
// flag (reused by Template's post-render hook filter so hooks don't
// leak into the output when disabled).
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
	inst.IncludeCRDs = !opts.SkipCRDs
	// The CLI skip flag suppresses CRDs regardless of HR policy.
	// spec.install.crds / spec.upgrade.crds set to "Skip" also suppresses
	// CRDs when the CLI requests them.
	if hr.CRDsPolicy == string(helmv2.Skip) {
		inst.IncludeCRDs = false
	}
	disableHooks := hr.Install != nil && hr.Install.DisableHooks
	if opts.IsUpgrade {
		disableHooks = hr.Upgrade != nil && hr.Upgrade.DisableHooks
	}
	disableHooks = opts.NoHooks || disableHooks
	inst.DisableHooks = disableHooks
	inst.IsUpgrade = opts.IsUpgrade
	inst.EnableDNS = opts.EnableDNS
	// Honor spec.install.replace per helm-controller's
	// internal/action/install.go: install.Replace = obj.GetInstall().Replace.
	// Default false (the chart fields' zero value); set true only when
	// the HR explicitly asks. Mostly a no-op under DryRunClient but
	// keeps flate's render path matching upstream so any future
	// validation behavior change in helm.action lands consistently.
	if hr.Install != nil {
		inst.Replace = hr.Install.Replace
	}
	inst.DisableOpenAPIValidation = hr.DisableOpenAPIValidation
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
	return inst, disableHooks
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

func releaseManifest(rel *release.Release, opts Options, disableHooks, skipTests bool) string {
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
	out := b.String()
	if len(opts.ShowOnly) > 0 {
		out = filterShowOnly(out, opts.ShowOnly)
	}
	return out
}

// filterShowOnly keeps only sections whose "# Source: <path>" header
// matches one of the requested template paths. Paths lists are short
// (typically 1-5 entries from --show-only), so linear scan avoids a
// map allocation on the hot render path.
func filterShowOnly(content string, paths []string) string {
	var out strings.Builder
	var opening string
	for section := range strings.SplitAfterSeq(content, "\n---\n") {
		frame, nextOpening := section, ""
		if strings.HasSuffix(section, "\n---\n") {
			// The boundary newline terminates this frame; the separator opens the next.
			frame = strings.TrimSuffix(section, "---\n")
			nextOpening = "---\n"
		}
		var header string
		for line := range strings.SplitSeq(frame, "\n") {
			if rest, ok := strings.CutPrefix(line, sourceHeader); ok {
				header = rest
				break
			}
		}
		if slices.Contains(paths, header) {
			out.WriteString(opening)
			out.WriteString(frame)
		}
		opening = nextOpening
	}
	return out.String()
}
