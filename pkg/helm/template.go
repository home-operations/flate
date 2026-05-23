package helm

import (
	"context"
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

	inst := action.NewInstall(cfg)
	inst.DryRunStrategy = action.DryRunClient
	inst.ReleaseName = hr.ReleaseName()
	inst.Namespace = hr.ReleaseNamespace()
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
	// the bottom of this function so they don't leak into the output.
	disableHooks := opts.NoHooks ||
		(hr.Install != nil && hr.Install.DisableHooks) ||
		(hr.Upgrade != nil && hr.Upgrade.DisableHooks)
	inst.DisableHooks = disableHooks
	inst.IsUpgrade = opts.IsUpgrade
	inst.EnableDNS = opts.EnableDNS
	inst.Replace = true
	inst.DisableOpenAPIValidation = hr.DisableOpenAPIValidation
	inst.SkipSchemaValidation = hr.DisableSchemaValidation
	// spec.postRenderers — pipe rendered output through one or more
	// kustomize patch+image transforms. helm-controller does this via
	// the same postrenderer.PostRenderer hook.
	inst.PostRenderer = newPostRenderer(hr.PostRenderers)
	// action.Install consults its own KubeVersion field for chart
	// compatibility checks and ignores cfg.Capabilities for that purpose.
	if opts.KubeVersion != "" {
		kv, err := common.ParseKubeVersion(opts.KubeVersion)
		if err != nil {
			return "", fmt.Errorf("parse kube-version %q: %w", opts.KubeVersion, err)
		}
		inst.KubeVersion = kv
	}
	if hrValues == nil {
		hrValues = map[string]any{}
	}

	if hr.Chart.Version != "" {
		inst.Version = hr.Chart.Version
	}

	// Apply chart valuesFiles BEFORE HR.Values so HR overrides win.
	// Mirrors helm-controller's CoalesceValues layering: chart defaults
	// (handled internally by helm) → chart-named valuesFiles → HR.Values.
	finalValues := hrValues
	if len(hr.ChartValuesFiles) > 0 {
		base, err := mergeChartValuesFiles(loaded.Chart, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		if err != nil {
			return "", fmt.Errorf("helm chart valuesFiles %s/%s: %w", hr.Namespace, hr.Name, err)
		}
		finalValues = values.DeepMerge(base, hrValues)
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
	return releaseManifest(relV1, opts, disableHooks, skipTests), nil
}

// mergeChartValuesFiles merges the named values files (relative paths
// inside the chart archive) in the supplied order. Missing files are
// skipped when ignoreMissing is true; otherwise the first missing file
// is an error. Mirrors helm-controller's chartutil layering: each file
// is merged on top of the previous one.
func mergeChartValuesFiles(c *chart.Chart, names []string, ignoreMissing bool) (map[string]any, error) {
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
	skip := opts.SkipResourceKinds()
	if len(skip) == 0 {
		return docs, nil
	}
	return slices.DeleteFunc(docs, func(doc map[string]any) bool {
		return slices.Contains(skip, manifest.DocKind(doc))
	}), nil
}

// applyHROriginLabels stamps the helm.toolkit.fluxcd.io/{name,namespace}
// ownership labels onto every rendered doc, mirroring helm-controller's
// OriginLabels post-renderer. Real Flux uses these to track which HR
// owns each in-cluster resource for pruning + selection
// (`kubectl get -l helm.toolkit.fluxcd.io/name=...`), and they take
// precedence over CommonMetadata when keys collide — so we apply this
// pass AFTER applyHRCommonMetadata, matching the upstream order.
func applyHROriginLabels(docs []map[string]any, hr *manifest.HelmRelease) {
	group := helmv2.GroupVersion.Group
	origin := map[string]string{
		group + "/name":      hr.Name,
		group + "/namespace": hr.Namespace,
	}
	for _, doc := range docs {
		md, _ := doc["metadata"].(map[string]any)
		if md == nil {
			md = map[string]any{}
			doc["metadata"] = md
		}
		mergeStringMap(md, "labels", origin)
	}
}

// applyHRCommonMetadata merges spec.commonMetadata.labels and .annotations
// onto every rendered doc's metadata, mirroring helm-controller's
// CommonRenderer pass. commonMetadata wins on conflict, matching
// controller semantics.
func applyHRCommonMetadata(docs []map[string]any, cm *helmv2.CommonMetadata) {
	if cm == nil || (len(cm.Labels) == 0 && len(cm.Annotations) == 0) {
		return
	}
	for _, doc := range docs {
		md, _ := doc["metadata"].(map[string]any)
		if md == nil {
			md = map[string]any{}
			doc["metadata"] = md
		}
		mergeStringMap(md, "labels", cm.Labels)
		mergeStringMap(md, "annotations", cm.Annotations)
	}
}

func mergeStringMap(md map[string]any, key string, in map[string]string) {
	if len(in) == 0 {
		return
	}
	out, _ := md[key].(map[string]any)
	if out == nil {
		out = make(map[string]any, len(in))
	}
	for k, v := range in {
		out[k] = v
	}
	md[key] = out
}

// releaseManifest joins rel.Manifest with hooks (when allowed) and
// returns a single YAML string. ShowOnly filters by template path,
// mirroring `helm template --show-only`.
func releaseManifest(rel *release.Release, opts Options, disableHooks, skipTests bool) string {
	var b strings.Builder
	b.WriteString(rel.Manifest)
	if !disableHooks {
		for _, h := range rel.Hooks {
			if skipTests && isTestHook(h) {
				continue
			}
			if !strings.HasSuffix(b.String(), "\n") {
				b.WriteByte('\n')
			}
			fmt.Fprintf(&b, "---\n# Source: %s\n%s", h.Path, h.Manifest)
		}
	}
	out := b.String()
	if len(opts.ShowOnly) > 0 {
		out = filterShowOnly(out, opts.ShowOnly)
	}
	return out
}

// filterShowOnly keeps only sections whose "# Source: <path>" header
// matches one of the requested template paths.
func filterShowOnly(content string, paths []string) string {
	want := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		want[p] = struct{}{}
	}
	var out strings.Builder
	for section := range strings.SplitSeq(content, "\n---\n") {
		header := ""
		for line := range strings.SplitSeq(section, "\n") {
			if rest, ok := strings.CutPrefix(line, "# Source: "); ok {
				header = rest
				break
			}
		}
		if _, ok := want[header]; !ok {
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
