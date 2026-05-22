package helm

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/chart/common"
	"helm.sh/helm/v4/pkg/cli"
	release "helm.sh/helm/v4/pkg/release/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// Template renders a HelmRelease and returns the rendered manifest as a
// single YAML string (multiple documents separated by "---" lines).
func (c *Client) Template(ctx context.Context, hr *manifest.HelmRelease, values map[string]any, opts Options) (string, error) {
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
	inst.ReleaseName = hr.Name
	inst.Namespace = hr.ReleaseNamespace()
	inst.IncludeCRDs = !opts.SkipCRDs
	inst.DisableHooks = opts.NoHooks
	inst.IsUpgrade = opts.IsUpgrade
	inst.EnableDNS = opts.EnableDNS
	inst.Replace = true
	inst.DisableOpenAPIValidation = hr.DisableOpenAPIValidation
	// action.Install consults its own KubeVersion field for chart
	// compatibility checks and ignores cfg.Capabilities for that purpose.
	if opts.KubeVersion != "" {
		kv, err := common.ParseKubeVersion(opts.KubeVersion)
		if err != nil {
			return "", fmt.Errorf("parse kube-version %q: %w", opts.KubeVersion, err)
		}
		inst.KubeVersion = kv
	}
	if values == nil {
		values = map[string]any{}
	}

	if hr.Chart.Version != "" {
		inst.Version = hr.Chart.Version
	}

	rel, err := inst.RunWithContext(ctx, loaded.Chart, values)
	if err != nil {
		return "", fmt.Errorf("helm template %s/%s: %w", hr.Namespace, hr.Name, err)
	}
	relV1, ok := rel.(*release.Release)
	if !ok {
		return "", fmt.Errorf("helm template %s/%s: unexpected release type %T", hr.Namespace, hr.Name, rel)
	}

	return releaseManifest(relV1, opts), nil
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
	skip := opts.SkipResourceKinds()
	if len(skip) == 0 {
		return docs, nil
	}
	return slices.DeleteFunc(docs, func(doc map[string]any) bool {
		return slices.Contains(skip, manifest.DocKind(doc))
	}), nil
}

// releaseManifest joins rel.Manifest with hooks (when allowed) and
// returns a single YAML string. ShowOnly filters by template path,
// mirroring `helm template --show-only`.
func releaseManifest(rel *release.Release, opts Options) string {
	var b strings.Builder
	b.WriteString(rel.Manifest)
	if !opts.NoHooks {
		for _, h := range rel.Hooks {
			if opts.SkipTests && isTestHook(h) {
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

