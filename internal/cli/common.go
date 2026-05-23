package cli

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/orchestrator"
)

type commonFlags struct {
	path               string
	pathOrig           string
	namespace          string
	labels             map[string]string
	skipCRDs           bool
	skipSecrets        bool
	allowMissingSecrets bool
	skipKinds          []string
	output             string
	enableOCI          bool
	registryConfig     string
	concurrency        int
}

func bindCommon(fs *pflag.FlagSet, f *commonFlags) {
	fs.StringVar(&f.path, "path", ".", "path to the Flux cluster directory")
	fs.StringVar(&f.pathOrig, "path-orig", "",
		"baseline path; when set, every command runs in changed-only mode")
	fs.StringVarP(&f.namespace, "namespace", "n", "",
		"limit to this namespace (default: every namespace, or just the changed ones when --path-orig is set)")
	fs.BoolVar(&f.skipCRDs, "skip-crds", true, "exclude CRD objects from rendered output")
	fs.BoolVar(&f.skipSecrets, "skip-secrets", true, "exclude Secret objects from rendered output")
	fs.BoolVar(&f.allowMissingSecrets, "allow-missing-secrets", false,
		"soft-skip sources whose auth Secret is missing or has PLACEHOLDER-wiped values "+
			"(typical when the live cluster materializes auth via ExternalSecret); dependent "+
			"Kustomizations/HelmReleases propagate the skip. Verify/cert/proxy secretRefs still fail loud.")
	fs.StringSliceVar(&f.skipKinds, "skip-kinds", nil, "extra kinds to drop from rendered output")
	fs.StringVarP(&f.output, "output", "o", "table", "output format: table, yaml, json, name")
	fs.BoolVar(&f.enableOCI, "enable-oci", true, "reconcile OCIRepository objects")
	fs.StringVar(&f.registryConfig, "registry-config", "", "docker config.json for OCI authentication")
	fs.IntVar(&f.concurrency, "concurrency", runtime.NumCPU()*4,
		"max parallel reconcile bodies (0 = unbounded)")
}

// skipResourceKinds returns the union of canonical kinds (CRDs +
// Secrets when their corresponding boolean flags are set) and any
// user-supplied `--skip-kinds` entries. Build / diff write paths
// apply this against rendered docs from BOTH HelmRelease and
// Kustomization sources — helm.TemplateDocs pre-filters HR output
// inside the controller, but KS-rendered docs go through unfiltered
// (the docs must reach the Store unfiltered so downstream HRs can
// resolve valuesFrom / substituteFrom against them). The CLI applies
// this union at emit time so the user sees consistent filtering
// regardless of which controller produced the resource.
func (c *commonFlags) skipResourceKinds() []string {
	out := append([]string{}, c.skipKinds...)
	if c.skipCRDs {
		out = append(out, "CustomResourceDefinition")
	}
	if c.skipSecrets {
		out = append(out, "Secret")
	}
	return out
}

// bindSelector wires the `-l/--selector` flag. Scoped to commands that
// actually filter by labels — today, only `get`. Binding it on
// commands that ignore it (build/diff/test) would silently accept
// `-l foo=bar` and do nothing.
func bindSelector(fs *pflag.FlagSet, f *commonFlags) {
	fs.StringToStringVarP(&f.labels, "selector", "l", nil, "label selector (key=value, repeatable)")
}

// scopedNamespaces returns the namespace filter the command should
// honor. nil ↦ "no filter" (every namespace).
//
//   - An explicit --namespace value always wins.
//   - In --path-orig mode, the namespaces of the resolved keep-set
//     are used so commands automatically focus on what actually
//     changed without the user having to set -n.
//   - Otherwise every namespace is included.
func (c *commonFlags) scopedNamespaces(filter *change.Filter) map[string]struct{} {
	if c.namespace != "" {
		return map[string]struct{}{c.namespace: {}}
	}
	if filter.Enabled() {
		if ns := filter.KeepNamespaces(); len(ns) > 0 {
			return ns
		}
	}
	return nil
}

// includeNamespace reports whether ns passes the effective filter.
// Empty namespace (cluster-scoped resources) is always included.
func (c *commonFlags) includeNamespace(filter *change.Filter, ns string) bool {
	if ns == "" {
		return true
	}
	scope := c.scopedNamespaces(filter)
	if scope == nil {
		return true
	}
	_, ok := scope[ns]
	return ok
}

// helmFlags collect the helm template options. Mirrors flux-local's
// --kube-version/--api-versions/--no-hooks/etc.
type helmFlags struct {
	kubeVersion          string
	apiVersions          string
	isUpgrade            bool
	noHooks              bool
	showOnly             []string
	enableDNS            bool
	skipSchemaValidation bool
}

// rendersHelm reports whether the supplied kinds slice contains
// KindHelmRelease, used to gate bindHelmFlags off of subcommands
// (`build ks`, `diff ks`, `test ks`) that only render Kustomizations.
// Without this gate the helm-template flags were silently accepted
// on KS-only subcommands and no-op'd — confusing to users who set
// e.g. `--show-only templates/foo.yaml` on `flate build ks` and
// wondered why nothing changed.
func rendersHelm(kinds []string) bool {
	for _, k := range kinds {
		if k == "HelmRelease" {
			return true
		}
	}
	return false
}

func bindHelmFlags(fs *pflag.FlagSet, h *helmFlags) {
	// Default to the Kubernetes minor version bundled with the k8s.io/api
	// dependency. Charts gated on KubeVersion (e.g. >=1.33 for ImageVolume)
	// then render against the latest version flate knows about, which
	// matches what a freshly-`flux install`'d cluster would see.
	fs.StringVar(&h.kubeVersion, "kube-version", helm.BundledKubeVersion(),
		"Kubernetes version for .Capabilities.KubeVersion (default: version bundled with flate)")
	fs.StringVarP(&h.apiVersions, "api-versions", "a", "", "comma-separated API versions for .Capabilities.APIVersions")
	fs.BoolVar(&h.isUpgrade, "is-upgrade", false, "set .Release.IsUpgrade instead of .Release.IsInstall")
	fs.BoolVar(&h.noHooks, "no-hooks", false, "exclude hook-annotated templates")
	fs.StringSliceVarP(&h.showOnly, "show-only", "s", nil, "only show specific template paths (repeatable)")
	fs.BoolVar(&h.enableDNS, "enable-dns", false, "enable DNS lookups during helm template")
	fs.BoolVar(&h.skipSchemaValidation, "skip-schema-validation", false,
		"skip helm values.schema.json validation (dominates allocation churn on big repos)")
}

func (c commonFlags) helmOptions(h helmFlags) helm.Options {
	return helm.Options{
		SkipCRDs:             c.skipCRDs,
		SkipSecrets:          c.skipSecrets,
		SkipKinds:            c.skipKinds,
		KubeVersion:          h.kubeVersion,
		APIVersions:          h.apiVersions,
		IsUpgrade:            h.isUpgrade,
		NoHooks:              h.noHooks,
		ShowOnly:             h.showOnly,
		EnableDNS:            h.enableDNS,
		SkipSchemaValidation: h.skipSchemaValidation,
		SkipTests:            true,
	}
}

func buildOrchCfg(c commonFlags, h helmFlags) orchestrator.Config {
	return orchestrator.Config{
		Path:               c.path,
		PathOrig:           c.pathOrig,
		HelmOptions:        c.helmOptions(h),
		WipeSecrets:        true,
		EnableOCI:          c.enableOCI,
		RegistryConfig:     c.registryConfig,
		Concurrency:        c.concurrency,
		AllowMissingSecrets: c.allowMissingSecrets,
	}
}

func runOrchestrator(ctx context.Context, c commonFlags, h helmFlags) (*orchestrator.Orchestrator, *orchestrator.Result, error) {
	if c.path == "" {
		return nil, nil, errors.New("path is required")
	}
	if _, err := format.ParseOutput(c.output); err != nil {
		return nil, nil, err
	}
	return runOrchestratorCfg(ctx, buildOrchCfg(c, h))
}

// outputOrDefault returns the user's -o choice, or fallback when -o
// is at its CLI default ("table"). Subcommands like build/diff have
// no table representation, so "table" effectively means "the
// subcommand-natural default" (yaml for build, diff for diff).
func (c *commonFlags) outputOrDefault(fallback format.Output) format.Output {
	if c.output == string(format.OutputTable) {
		return fallback
	}
	return format.Output(c.output)
}

// requireOutput rejects an -o value that's outside the subcommand's
// supported set. Use for subcommands that don't honor every global -o
// value (e.g. build has no concept of "name"; diff has no concept of
// "name"). Treats "table" as accepted so the global default doesn't
// trigger this check — callers downstream coerce "table" to their
// own natural default via outputOrDefault.
func (c *commonFlags) requireOutput(allowed ...format.Output) error {
	if c.output == string(format.OutputTable) {
		return nil
	}
	for _, a := range allowed {
		if format.Output(c.output) == a {
			return nil
		}
	}
	names := make([]string, len(allowed))
	for i, a := range allowed {
		names[i] = string(a)
	}
	return fmt.Errorf("--output %q not supported by this subcommand (want one of: table, %s)",
		c.output, strings.Join(names, ", "))
}

// runOrchestratorCfg routes the CLI through the embed-friendly
// Orchestrator.Render entry point. Returns the populated orchestrator
// (for Store lookups the CLI legitimately needs — object listings,
// status queries, filter scope) AND the structured render Result.
// Both stay non-nil when Bootstrap succeeded, even if Run had per-
// resource failures: the partial output is still usable. A nil
// orchestrator indicates a fatal init/bootstrap error and callers
// should bail.
//
// Dogfooding Render here closes a drift hazard the iter-13 review
// flagged: the embed API and the CLI used to read rendered artifacts
// through different code paths (CLI reached straight into the Store
// with a type assertion). Now there's one path.
func runOrchestratorCfg(ctx context.Context, cfg orchestrator.Config) (*orchestrator.Orchestrator, *orchestrator.Result, error) {
	o, err := orchestrator.New(cfg)
	if err != nil {
		return nil, nil, err
	}
	res, err := o.Render(ctx)
	return o, res, err
}

func cmdContext(cmd *cobra.Command) context.Context {
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}
