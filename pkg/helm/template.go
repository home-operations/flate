package helm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/cli"
	release "helm.sh/helm/v4/pkg/release/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Template renders a HelmRelease and returns the rendered manifest as a
// single YAML string (multiple documents separated by "---" lines).
//
// Output is cached by computeTemplateKey when c.templateCache is wired
// (NewClient's default, sized by ClientOptions.TemplateCacheBytes).
// Cache hits skip action.Install.RunWithContext — the single largest
// CPU + allocation consumer in the codebase (cited at ~300 MB on a
// 200-HR run). Each key folds in every input that affects the
// rendered bytes (chart content fingerprint, fully-resolved values,
// render Options, HR-level action.Install fields like ReleaseName,
// CRDs policy, hooks, post-renderers) so a stale entry never serves
// a different render.
func (c *Client) Template(ctx context.Context, hr *manifest.HelmRelease, hrValues map[string]any, opts Options) (string, error) {
	loaded, err := c.LoadChart(ctx, hr)
	if err != nil {
		return "", err
	}
	caps, err := opts.capabilities()
	if err != nil {
		return "", err
	}

	settings := cli.New()

	cfg := new(action.Configuration)
	if err := cfg.Init(settings.RESTClientGetter(), hr.ReleaseNamespace(), ""); err != nil {
		return "", fmt.Errorf("helm init: %w", err)
	}
	cfg.Capabilities = caps

	inst, disableHooks, err := newInstallAction(cfg, hr, opts, caps)
	if err != nil {
		return "", err
	}
	if hrValues == nil {
		hrValues = map[string]any{}
	}
	inst.SkipSchemaValidation = schemaValidationSkipped(opts, hr, hrValues)

	// Apply chart valuesFiles BEFORE HR.Values so HR overrides win.
	// Mirrors helm-controller's CoalesceValues layering: chart defaults
	// (handled internally by helm) → chart-named valuesFiles → HR.Values.
	finalValues := hrValues
	if len(hr.ChartValuesFiles) > 0 {
		base, err := c.mergeChartValuesFiles(loaded.Chart, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		if err != nil {
			return "", fmt.Errorf("helm chart valuesFiles %s/%s: %w", hr.Namespace, hr.Name, err)
		}
		finalValues = values.DeepMerge(base, hrValues)
	}

	// Output cache: the same (chart fingerprint, finalValues, opts,
	// hr-fields) tuple deterministically produces the same rendered
	// manifest. Repeat HRs with the same effective spec hit the cache
	// and skip inst.RunWithContext — the call cited at ~300 MB of
	// allocations per 200-HR run. The key folds in every render-
	// affecting input, so cache hits are byte-identical to the
	// uncached path (also pinned by an integration test).
	var key string
	if c.templateCache != nil {
		key = computeTemplateKey(loaded.Fingerprint, loaded.Chart, finalValues, opts, hr)
		if cached, ok := c.templateCache.Get(key); ok {
			return cached, nil
		}
	}

	rel, err := inst.RunWithContext(ctx, loaded.Chart, finalValues)
	if err != nil {
		return "", fmt.Errorf("helm template %s/%s: %w", hr.Namespace, hr.Name, err)
	}
	relV1, ok := rel.(*release.Release)
	if !ok {
		return "", fmt.Errorf("helm template %s/%s: unexpected release type %T", hr.Namespace, hr.Name, rel)
	}

	// spec.test.enable defaults to false; tests only land in the
	// rendered output when the HR explicitly enables them or the
	// CLI overrides. CLI --skip-tests always wins.
	skipTests := opts.SkipTests || hr.Test == nil || !hr.Test.Enable
	out := releaseManifest(relV1, opts, disableHooks, skipTests)
	if c.templateCache != nil {
		c.templateCache.Put(key, out)
	}
	return out, nil
}

// schemaValidationSkipped reports whether to bypass helm's values.schema.json
// validation for this render. Beyond the explicit opt-outs (CLI flag, HR
// field), flate skips when a wipe placeholder reaches rendering: secret-derived
// values it couldn't resolve become a ..PLACEHOLDER_KEY.. token that a schema's
// DNS/URL/regex constraint would reject. Real Flux resolves the secret and
// validates for real — flate can't, so it skips rather than emit a bogus
// failure against a value it fabricated.
func schemaValidationSkipped(opts Options, hr *manifest.HelmRelease, values map[string]any) bool {
	return opts.SkipSchemaValidation ||
		hr.DisableSchemaValidation ||
		manifest.ContainsValuePlaceholder(values)
}

// newInstallAction builds the DryRunClient action.Install that Template
// renders through, mirroring helm-controller's install/upgrade field
// wiring. Returns the configured install, the effective disableHooks
// flag (reused by Template's post-render hook filter so hooks don't
// leak into the output when disabled), and any kube-version parse error.
func newInstallAction(cfg *action.Configuration, hr *manifest.HelmRelease, opts Options, caps *common.Capabilities) (*action.Install, bool, error) {
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
	// HR-scoped policy wins: spec.install.crds / spec.upgrade.crds set
	// to "Skip" suppresses CRDs even when the CLI requests them.
	// "Create" / "CreateReplace" force them on. An empty policy lets
	// the CLI flag decide.
	switch hr.CRDsPolicy {
	case "Skip":
		inst.IncludeCRDs = false
	case "Create", "CreateReplace":
		inst.IncludeCRDs = true
	}
	// HR-scoped install/upgrade.disableHooks OR'd with the CLI flag,
	// mirroring helm-controller. Either side forces hooks off — and
	// the same effective flag drives the post-render hook filter at
	// the bottom of Template so they don't leak into the output.
	disableHooks := opts.NoHooks ||
		(hr.Install != nil && hr.Install.DisableHooks) ||
		(hr.Upgrade != nil && hr.Upgrade.DisableHooks)
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
	if opts.KubeVersion != "" {
		kv, err := common.ParseKubeVersion(opts.KubeVersion)
		if err != nil {
			return nil, false, fmt.Errorf("parse kube-version %q: %w", opts.KubeVersion, err)
		}
		inst.KubeVersion = kv
	}
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
	return inst, disableHooks, nil
}

// mergeChartValuesFiles is the cache-aware entry point: it consults
// Client.chartValuesCache before re-parsing and stores the canonical
// merged map on miss. Callers receive a deep clone (defensive-copy
// convention matching indexCache) — downstream layering DeepMerges
// the result, which may mutate intermediate sub-maps.
//
// Cache key = sha256(chart.Name || chart.Version || joined valuesFiles
// list || ignoreMissing bit). Distinct chart identities (different
// name or version, e.g. a chart upgrade landing under the same path)
// produce distinct keys, so a stale entry never serves a different
// chart's values. ignoreMissing is folded into the key because two
// HRs with the same (chart, valuesFiles) but different policies must
// not share — a missing file is an error in one and skipped in the
// other.
func (c *Client) mergeChartValuesFiles(ch *chart.Chart, names []string, ignoreMissing bool) (map[string]any, error) {
	key := chartValuesCacheKey(ch, names, ignoreMissing)
	c.chartMu.RLock()
	cached, ok := c.chartValuesCache[key]
	c.chartMu.RUnlock()
	if ok {
		return manifest.DeepCopyMap(cached), nil
	}
	merged, err := mergeChartValuesFilesUncached(ch, names, ignoreMissing)
	if err != nil {
		return nil, err
	}
	c.chartMu.Lock()
	// Re-check under the write lock: a sibling goroutine may have
	// populated the same key between RUnlock and Lock.
	if existing, ok := c.chartValuesCache[key]; ok {
		c.chartMu.Unlock()
		return manifest.DeepCopyMap(existing), nil
	}
	c.chartValuesCache[key] = merged
	c.chartMu.Unlock()
	return manifest.DeepCopyMap(merged), nil
}

// chartValuesCacheKey builds the cache key for a (chart, valuesFiles,
// ignoreMissing) tuple. The hash input is delimited so a chart named
// "a-b" with version "c" hashes distinctly from a chart named "a"
// with version "b-c". The trailing ignoreMissing byte separates the
// two policy variants.
func chartValuesCacheKey(ch *chart.Chart, names []string, ignoreMissing bool) string {
	// hash.Hash.Write never returns an error per its contract; drain
	// the (int, error) tuple so gosec G104 stays quiet.
	h := sha256.New()
	if ch != nil && ch.Metadata != nil {
		_, _ = h.Write([]byte(ch.Metadata.Name))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(ch.Metadata.Version))
	}
	_, _ = h.Write([]byte{0})
	for _, n := range names {
		_, _ = h.Write([]byte(n))
		_, _ = h.Write([]byte{0})
	}
	if ignoreMissing {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// mergeChartValuesFilesUncached merges the named values files (relative
// paths inside the chart archive) in the supplied order. Missing files
// are skipped when ignoreMissing is true; otherwise the first missing
// file is an error. Mirrors helm-controller's chartutil layering: each
// file is merged on top of the previous one.
//
// Pure function — no caching. Client.mergeChartValuesFiles wraps this
// with the chart-keyed cache layer so production callers never re-parse
// the same bytes twice. Kept exported-to-package so benchmarks can
// measure the uncached parse cost in isolation.
func mergeChartValuesFilesUncached(c *chart.Chart, names []string, ignoreMissing bool) (map[string]any, error) {
	out := map[string]any{}
	for _, name := range names {
		var data []byte
		for _, f := range c.Files {
			if f != nil && f.Name == name {
				data = f.Data
				break
			}
		}
		if data == nil {
			if ignoreMissing {
				continue
			}
			return nil, fmt.Errorf("values file %q not found in chart", name)
		}
		var m map[string]any
		if err := yaml.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("parse values file %q: %w", name, err)
		}
		out = values.DeepMerge(out, m)
	}
	return out, nil
}

// TemplateDocs renders and returns each document parsed as a generic map.
func (c *Client) TemplateDocs(ctx context.Context, hr *manifest.HelmRelease, values map[string]any, opts Options) ([]map[string]any, error) {
	raw, err := c.Template(ctx, hr, values, opts)
	if err != nil {
		return nil, err
	}
	docs, err := manifest.SplitDocs([]byte(raw))
	if err != nil {
		return nil, err
	}
	applyHRCommonMetadata(docs, hr.CommonMetadata)
	applyHROriginLabels(docs, hr)
	return manifest.DropKinds(docs, opts.SkipResourceKinds()), nil
}

// applyHROriginLabels stamps the helm.toolkit.fluxcd.io/{name,namespace}
// ownership labels onto every non-hook rendered doc, mirroring
// helm-controller's OriginLabels post-renderer fed through
// PostRenderStrategyNoHooks. Real Flux uses these to track which HR
// owns each in-cluster resource for pruning + selection
// (`kubectl get -l helm.toolkit.fluxcd.io/name=...`), and they take
// precedence over CommonMetadata when keys collide — so we apply this
// pass AFTER applyHRCommonMetadata, matching the upstream order.
//
// Helm hooks (Job / ConfigMap with helm.sh/hook annotation) are
// intentionally NOT stamped — upstream's BuildPostRenderers chain is
// fed only the non-hook stream via PostRenderStrategyNoHooks. Stamping
// hooks in flate would produce labels real Flux never applies in
// cluster.
func applyHROriginLabels(docs []map[string]any, hr *manifest.HelmRelease) {
	group := helmv2.GroupVersion.Group
	origin := map[string]string{
		group + "/name":      hr.Name,
		group + "/namespace": hr.Namespace,
	}
	for _, doc := range docs {
		if isHookDoc(doc) {
			continue
		}
		manifest.MergeStringMap(manifest.EnsureMetadata(doc), "labels", origin)
	}
}

// applyHRCommonMetadata merges spec.commonMetadata.labels and .annotations
// onto every workload rendered doc's metadata, mirroring helm-controller's
// CommonRenderer pass fed through PostRenderStrategyNoHooks. commonMetadata
// overwrites chart-template defaults but loses to origin labels on
// collision — applyHROriginLabels runs AFTER this and reasserts the
// helm.toolkit.fluxcd.io/{name,namespace} keys, matching helm-controller's
// build.go:46 comment.
//
// Excluded from the stamping pass:
//   - Hooks (helm.sh/hook annotation): upstream's post-render chain
//     runs after hook separation via PostRenderStrategyNoHooks.
//   - CRDs (CustomResourceDefinition kind): upstream's separate
//     applyCRDs path attaches only origin labels via setOriginVisitor,
//     never commonMetadata. Stamping CRDs in flate produced labels
//     real Flux never applies in cluster.
func applyHRCommonMetadata(docs []map[string]any, cm *helmv2.CommonMetadata) {
	if cm == nil || (len(cm.Labels) == 0 && len(cm.Annotations) == 0) {
		return
	}
	for _, doc := range docs {
		if isHookDoc(doc) || manifest.DocKind(doc) == manifest.KindCustomResourceDefinition {
			continue
		}
		md := manifest.EnsureMetadata(doc)
		manifest.MergeStringMap(md, "labels", cm.Labels)
		manifest.MergeStringMap(md, "annotations", cm.Annotations)
	}
}

// isHookDoc reports whether a rendered manifest is a Helm hook
// (carrying the helm.sh/hook annotation). Used to gate the
// commonMetadata + origin-label post-render passes so they only apply
// to "real" workload resources — matching upstream helm-controller's
// PostRenderStrategyNoHooks contract.
func isHookDoc(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		return false
	}
	anns, _ := md["annotations"].(map[string]any)
	if anns == nil {
		return false
	}
	_, has := anns["helm.sh/hook"]
	return has
}

// releaseManifest joins rel.Manifest with hooks (when allowed) and
// returns a single YAML string. ShowOnly filters by template path,
// mirroring `helm template --show-only`.
func releaseManifest(rel *release.Release, opts Options, disableHooks, skipTests bool) string {
	// Pre-size the builder: rendered manifest + every hook body. Saves
	// the geometric bytes.Buffer.grow walk on big releases — measured
	// at ~300 MB of allocations on a 200-HR drag0n141 run.
	size := len(rel.Manifest)
	if !disableHooks {
		for _, h := range rel.Hooks {
			if skipTests && isTestHook(h) {
				continue
			}
			size += len("---\n# Source: \n") + len(h.Path) + len(h.Manifest) + 1
		}
	}
	var b strings.Builder
	b.Grow(size)
	b.WriteString(rel.Manifest)
	lastNewline := strings.HasSuffix(rel.Manifest, "\n")
	if !disableHooks {
		for _, h := range rel.Hooks {
			if skipTests && isTestHook(h) {
				continue
			}
			if !lastNewline {
				b.WriteByte('\n')
			}
			fmt.Fprintf(&b, "---\n# Source: %s\n%s", h.Path, h.Manifest)
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
	for section := range strings.SplitSeq(content, "\n---\n") {
		var header string
		for line := range strings.SplitSeq(section, "\n") {
			if rest, ok := strings.CutPrefix(line, "# Source: "); ok {
				header = rest
				break
			}
		}
		if !slices.Contains(paths, header) {
			continue
		}
		if out.Len() > 0 {
			out.WriteString("---\n")
		}
		out.WriteString(section)
	}
	return out.String()
}

func isTestHook(h *release.Hook) bool {
	return slices.Contains(h.Events, release.HookTest)
}
