package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

type commonFlags struct {
	path     string
	pathOrig string
	// pathOrigRoot / pathOrigSelfURLs are resolved by resolveBaseline for
	// the --base flow: the materialized baseline tree carries no .git, so
	// its repo root (the spec.path anchor + change-detect root) and the
	// live tree's remote URLs (for self-referential aliasing on the
	// baseline side) are threaded explicitly rather than re-derived from a
	// .git. Empty for an explicit --path-orig (the CLI defaults the root
	// via repoRootOf and lets the side read its own .git remotes).
	pathOrigRoot        string
	pathOrigSelfURLs    []string
	base                string
	namespace           string
	skipCRDs            bool
	skipSecrets         bool
	allowMissingSecrets bool
	skipKinds           []string
	output              string
	registryConfig      string
	concurrency         int
	// sourceRetry* tune the bounded retry applied uniformly to every source
	// fetch on transient network errors. attempts is the total tries (first +
	// retries); 1 disables. min/max bound the exponential backoff and jitter
	// spreads it. Plumbed into orchestrator.Config.SourceRetry → source.WithRetry.
	sourceRetryAttempts int
	sourceRetryMinWait  time.Duration
	sourceRetryMaxWait  time.Duration
	sourceRetryJitter   float64
	// gitDepth caps the shallow-clone history depth for GitRepository
	// sources. Default 1 (opt-out shallow); 0 = full clone. Refs pinned
	// to an explicit commit always full-clone. Plumbed into
	// orchestrator.Config.GitDepth → git.Fetcher.Depth.
	gitDepth int
	// cacheDir is resolved lazily via resolveCacheRoot and memoized so
	// multiple lookups within one invocation return the same value.
	cacheDir string
	// profileMode selects a runtime profile to capture for the run.
	// Empty disables profiling; valid values are "cpu", "mem", "block",
	// "mutex", and "trace". Wired through startProfile in profile.go.
	profileMode string
	// profileOut is the directory profile files land in. Defaults to
	// the current working directory when --profile is set and no
	// --profile-out is given.
	profileOut string
	// helmTemplateCacheMB caps the in-memory helm template-output
	// cache in megabytes. Default 256; 0 disables. Plumbed through
	// orchestrator.Config.HelmTemplateCacheBytes into the helm.Client
	// constructor.
	helmTemplateCacheMB int
	// helmRenderCacheMB caps the persistent on-disk helm template-
	// output cache in megabytes. Default 1024; 0
	// disables. Plumbed through orchestrator.Config.HelmRenderCacheBytes
	// into the helm.Client constructor. Cross-process: repeat `flate
	// build` / `flate diff` runs against the same checkout reuse
	// previously-rendered manifests instead of re-running
	// action.Install.RunWithContext.
	helmRenderCacheMB int
}

// bindCommon wires the flags every reconcile-running subcommand shares.
// outputs is the subcommand's accepted -o values in help-display order:
// the first is the default the flag takes when -o is omitted, and the flag
// (an outputValue enum) rejects any value outside the set at parse time.
// Each subcommand passes only the formats it honors — e.g. build doesn't
// claim `name`, and diff doesn't claim `table`. Passing no formats (test,
// which emits one fixed report) registers no -o flag at all.
func bindCommon(fs *pflag.FlagSet, f *commonFlags, outputs ...format.Output) {
	fs.StringVarP(&f.path, "path", "p", ".", "path to the Flux cluster directory")
	fs.StringVarP(&f.pathOrig, "path-orig", "P", "",
		"baseline path; when set, every command runs in changed-only mode")
	bindBase(fs, f)
	fs.StringVarP(&f.namespace, "namespace", "n", "",
		"limit to this namespace (default: every namespace, or just the changed ones when --path-orig is set)")
	fs.BoolVar(&f.skipCRDs, "skip-crds", true, "exclude CRD objects from rendered output")
	fs.BoolVar(&f.skipSecrets, "skip-secrets", true, "exclude Secret objects from rendered output")
	fs.BoolVar(&f.allowMissingSecrets, "allow-missing-secrets", false,
		"soft-skip source auth Secrets and omit unavailable HelmRelease valuesFrom Secret/ConfigMap refs "+
			"that only materialize in the live cluster. "+
			"Verify/cert/proxy secretRefs still fail loud.")
	fs.StringSliceVar(&f.skipKinds, "skip-kinds", nil, "extra kinds to drop from rendered output")
	// Commands with no -o variants (test) register no -o flag at all, so
	// `-o anything` is an unknown flag rather than a rendered alternative.
	// Otherwise -o is an enum (outputValue): an unsupported value is
	// rejected at parse time, so cobra reports it before the command runs.
	if len(outputs) > 0 {
		f.output = string(outputs[0]) // default; outputValue.String reports it in --help
		fs.VarP(&outputValue{target: &f.output, allowed: outputs}, "output", "o", outputUsage(outputs))
	}
	fs.StringVar(&f.registryConfig, "registry-config", "", "docker config.json for OCI authentication")
	fs.StringVar(&f.cacheDir, "cache-dir", "",
		"on-disk cache root for source artifacts, helm charts, "+
			"and persistent render output. Defaults to $XDG_CACHE_HOME/flate "+
			"(Linux), ~/Library/Caches/flate (macOS), %LocalAppData%/flate "+
			"(Windows), falling back to $TMPDIR/flate-cache if those error.")
	fs.IntVar(&f.concurrency, "concurrency", runtime.NumCPU()*4,
		"max parallel reconcile bodies (0 = unbounded)")
	// Retry only kicks in for transient network failures (connection
	// reset/refused, timeouts); a bad path / auth / not-found still fails
	// on the first try. --source-retry-attempts=1 disables it entirely.
	fs.IntVar(&f.sourceRetryAttempts, "source-retry-attempts", 3,
		"max attempts per source fetch on transient network errors (1 disables retry)")
	fs.DurationVar(&f.sourceRetryMinWait, "source-retry-min-wait", 200*time.Millisecond,
		"minimum backoff between source-fetch retries")
	fs.DurationVar(&f.sourceRetryMaxWait, "source-retry-max-wait", 3*time.Second,
		"maximum backoff between source-fetch retries")
	fs.Float64Var(&f.sourceRetryJitter, "source-retry-jitter", 0.1,
		"jitter fraction [0,1] applied to source-fetch retry backoff")
	// Shallow clone is the dominant speedup for GitRepository sources that
	// pull a subdir out of a deep-history monorepo: depth=1 fetches only
	// the tip commit, which is all the worktree materialization needs.
	// Commit-pinned refs always full-clone (the pinned commit may be deep).
	fs.IntVar(&f.gitDepth, "git-depth", 1,
		"shallow-clone depth for GitRepository sources; 0 = full clone "+
			"(refs pinned to an explicit commit always full-clone)")
	fs.Var(&profileValue{target: &f.profileMode}, "profile",
		"write a runtime profile: cpu, mem, block, mutex, or trace (off by default)")
	fs.StringVar(&f.profileOut, "profile-out", ".",
		"directory to write profile files into (used with --profile)")
	// 256 MiB default mirrors helm.DefaultTemplateCacheBytes. 0 disables
	// the cache entirely (useful in memory-constrained CI or when
	// debugging the uncached helm render path).
	fs.IntVar(&f.helmTemplateCacheMB, "helm-template-cache-mb", 256,
		"size of the in-memory helm template-output cache in megabytes (0 disables)")
	// 1 GiB default mirrors helm.DefaultRenderCacheBytes. Cross-process:
	// repeat `flate build`/`flate diff` runs against the same checkout
	// hit the persistent cache and short-circuit the helm render. 0
	// disables (useful when debugging the uncached path or in shared
	// CI runners where disk cost outweighs the rerun savings).
	fs.IntVar(&f.helmRenderCacheMB, "helm-render-cache-mb", 1024,
		"size of the persistent on-disk helm template-output cache in megabytes (0 disables)")
}

// skipResourceKinds delegates to helm.Options.SkipResourceKinds so
// the CLI write paths (build/diff emit) and the orchestrator's
// in-controller filtering use one canonical union of canonical
// kinds (CRDs + Secrets when their flags are set) plus any
// user-supplied `--skip-kinds` entries. KS-rendered docs reach the
// Store unfiltered (downstream HRs need them for valuesFrom /
// substituteFrom resolution); HR-rendered docs are pre-filtered
// inside the controller via helm.TemplateDocs. The CLI applies
// this union at emit time so the user sees consistent filtering
// regardless of which controller produced the resource.
func (c *commonFlags) skipResourceKinds() []string {
	return helm.Options{
		SkipCRDs:    c.skipCRDs,
		SkipSecrets: c.skipSecrets,
		SkipKinds:   c.skipKinds,
	}.SkipResourceKinds()
}

// listFlags holds flags that are only meaningful for list/get commands.
// Kept separate from commonFlags so commands that don't filter by labels
// (build/diff/test) don't carry a dead field.
type listFlags struct {
	labels map[string]string
}

// bindSelector wires the `-l/--selector` flag. Scoped to commands that
// actually filter by labels — today, only `get`. Binding it on
// commands that ignore it (build/diff/test) would silently accept
// `-l foo=bar` and do nothing.
func bindSelector(fs *pflag.FlagSet, f *listFlags) {
	fs.StringToStringVarP(&f.labels, "selector", "l", nil, "label selector (key=value, repeatable)")
}

// bindBase wires the `--base` flag. Bound on every command that
// accepts --path-orig (build, get, test, diff). Selects the baseline
// git rev that the auto-baseline flow materializes into a tempdir
// for changed-only mode.
//
// Semantics differ per verb:
//   - diff REQUIRES a baseline; bare command auto-detects via
//     merge-base with @{u} / origin/HEAD / origin/{main,master}.
//   - build/get/test do NOT auto-detect — bare command tests/builds
//     everything (preserves the existing "full tree" default).
//     Setting --base on these verbs opts into changed-only mode
//     against the named rev, sharing the same materialization
//     machinery as diff.
//
// Mutually exclusive with --path-orig (the absolute-path escape
// hatch); the check fires at runtime in resolveBaselineIfRequested.
func bindBase(fs *pflag.FlagSet, f *commonFlags) {
	fs.StringVar(&f.base, "base", "",
		"baseline git rev (e.g. main, origin/main, HEAD~3, SHA) — "+
			"materializes the rev's tree to a tempdir and runs in changed-only mode. "+
			"On `diff`, omitting --base auto-detects via merge-base with @{u} / origin/HEAD. "+
			"On `build`/`get`/`test`, omitting --base keeps the default full-tree behavior. "+
			"Mutually exclusive with --path-orig.")
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

// resolveBaseline runs baseline.AutoResolve when the user opted into
// changed-only mode via --base, mutates c.pathOrig to the synthetic
// materialized path, and returns a tempdir-cleanup func.
//
// autoFallback toggles the "fire even when both flags are empty" case
// — true for `flate diff` (which always needs a baseline; bare
// command auto-detects via the merge-base ladder); false for
// build/get/test (where bare command means "no baseline, full tree",
// and changed-only mode is opt-in via --base or --path-orig).
//
// Mutual exclusion with --path-orig is enforced here so every caller
// gets the same error message.
//
// Returns a cleanup func that callers MUST defer — it removes the
// materialized baseline tempdir. Previously cleanup was bound to
// `context.AfterFunc(ctx, ...)`, but ctx cancels on SIGINT
// concurrently with orchestrator goroutines that may still be
// reading under PathOrig (helm/kustomize/go-git filesystem reads
// aren't all ctx-aware), producing ENOENT noise in the shutdown
// error tail. Caller-defer'd cleanup runs after the orchestrator
// has finished (or panicked through) the read path, eliminating
// the race.
//
// Callers receive a no-op when no materialization happened (no
// --base / no autoFallback / explicit --path-orig).
func resolveBaseline(c *commonFlags, autoFallback bool) (func(), error) {
	noop := func() {}
	if c.pathOrig != "" && c.base != "" {
		return noop, errors.New("--path-orig and --base are mutually exclusive")
	}
	if c.pathOrig != "" {
		// Explicit --path-orig — caller already specified the baseline.
		return noop, nil
	}
	if c.base == "" && !autoFallback {
		// No opt-in and no fallback semantics — leave c.pathOrig empty
		// so the orchestrator runs in full-tree mode (the build/get/test
		// default).
		return noop, nil
	}
	res, err := baseline.AutoResolve(c.path, c.base, cacheroot.New(c.resolveCacheRoot()))
	if err != nil {
		return noop, err
	}
	c.pathOrig = res.PathOrig
	// The materialized baseline has no .git: pass its repo root + the live
	// tree's remotes explicitly so the baseline side anchors spec.path and
	// aliases self-referential sources without a synthetic marker.
	c.pathOrigRoot = res.TempDir
	c.pathOrigSelfURLs = res.SelfURLs
	slog.Debug("baseline", "source", res.Source, "rev", res.Rev, "path_orig", res.PathOrig, "persistent", res.Persistent)
	if res.Persistent {
		return noop, nil
	}
	return func() { _ = os.RemoveAll(res.TempDir) }, nil
}

// baselineRoot is the baseline side's source root: the materialized
// --base tree root when resolveBaseline set it, else the .git default of
// an explicit --path-orig. Empty when there's no baseline (full-tree mode).
func (c *commonFlags) baselineRoot() string {
	if c.pathOrigRoot != "" {
		return c.pathOrigRoot
	}
	return repoRootOf(c.pathOrig)
}

// repoRootOf resolves path to its source root the way discovery defaults
// RepoRoot: the .git ancestor of the resolved path, or the path itself
// when there's no .git. Empty in → empty out. This is the CLI's
// .git-based default for the explicit RepoRoot / baseline PathOrig the
// core now consumes — the changed-only core no longer infers a root or
// "widens" to a .git ancestor itself; the CLI resolves it here and passes
// it in.
func repoRootOf(path string) string {
	if path == "" {
		return ""
	}
	abs, err := discovery.ResolveScanPath(path)
	if err != nil {
		return path
	}
	return discovery.FindRepoRoot(abs)
}

func buildOrchCfg(c commonFlags, h helmFlags) orchestrator.Config {
	return orchestrator.Config{
		Path: c.path,
		// PathOrig carries the baseline's REPO ROOT for change detection
		// (change.Detect diffs root-to-root): the materialized --base tree
		// root, or the .git default of an explicit --path-orig. Replaces
		// the core's old .git "widen" heuristic.
		PathOrig:       c.baselineRoot(),
		HelmOptions:    c.helmOptions(h),
		WipeSecrets:    true,
		RegistryConfig: c.registryConfig,
		Concurrency:    c.concurrency,
		SourceRetry: source.RetryConfig{
			Attempts: c.sourceRetryAttempts,
			MinWait:  c.sourceRetryMinWait,
			MaxWait:  c.sourceRetryMaxWait,
			Jitter:   c.sourceRetryJitter,
		},
		GitDepth:               c.gitDepth,
		AllowMissingSecrets:    c.allowMissingSecrets,
		CacheDir:               c.resolveCacheRoot(),
		HelmTemplateCacheBytes: int64(c.helmTemplateCacheMB) << 20,
		HelmRenderCacheBytes:   int64(c.helmRenderCacheMB) << 20,
	}
}

// resolveCacheRoot returns dir if non-empty, or the platform default.
// The result is memoized back into dir so repeated calls within one
// invocation are cheap and return the same path.
func resolveCacheRoot(dir *string) string {
	if *dir == "" {
		*dir = cacheroot.Default()
	}
	return *dir
}

func (c *commonFlags) resolveCacheRoot() string {
	return resolveCacheRoot(&c.cacheDir)
}

func runOrchestrator(ctx context.Context, c commonFlags, h helmFlags) (*orchestrator.Orchestrator, *orchestrator.Result, error) {
	if c.path == "" {
		return nil, nil, errors.New("path is required")
	}
	if err := validatePathFlag("--path", c.path); err != nil {
		return nil, nil, err
	}
	if c.pathOrig != "" {
		if err := validatePathFlag("--path-orig", c.pathOrig); err != nil {
			return nil, nil, err
		}
	}
	// Cleanup is deferred (not bound to ctx) so the tempdir survives
	// SIGINT until the orchestrator's read paths have actually
	// unwound.
	cleanup, err := resolveBaseline(&c, false)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()
	return runOrchestratorCfg(ctx, buildOrchCfg(c, h))
}

// validatePathFlag rejects a flag value that doesn't point at a real
// directory, surfacing a clean typed error before the orchestrator
// digs deep enough to fail with a generic `flate error: ...`. Both
// the "directory missing" and "exists but is a file" cases are
// distinct user errors worth the early bail.
func validatePathFlag(flag, p string) error {
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s %q does not exist", flag, p)
		}
		return fmt.Errorf("%s %q: %w", flag, p, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %q is not a directory", flag, p)
	}
	return nil
}

// outputUsage renders the -o flag's help text from a subcommand's accepted
// formats, listed in the order bindCommon received them (the first is the
// default).
func outputUsage(outputs []format.Output) string {
	return "output format: " + joinOutputs(outputs)
}

// joinOutputs renders accepted -o formats as a comma-separated list, shared
// by the help text and the parse-time rejection so the two can't drift.
func joinOutputs(outputs []format.Output) string {
	names := make([]string, len(outputs))
	for i, o := range outputs {
		names[i] = string(o)
	}
	return strings.Join(names, ", ")
}

// outputValue is the pflag.Value backing -o: a string constrained to a
// subcommand's accepted formats. Set rejects anything else, so pflag/cobra
// surface the error (with usage) at parse time, before the command runs —
// no per-command validation call needed.
type outputValue struct {
	target  *string
	allowed []format.Output
}

func (o *outputValue) String() string { return *o.target }
func (o *outputValue) Type() string   { return "string" }

func (o *outputValue) Set(v string) error {
	if slices.Contains(o.allowed, format.Output(v)) {
		*o.target = v
		return nil
	}
	return fmt.Errorf("must be one of: %s", joinOutputs(o.allowed))
}

// profileModes are the runtime profiles --profile accepts. The empty
// string (the default) means no profiling; startProfile treats it as a
// no-op.
var profileModes = []string{"cpu", "mem", "block", "mutex", "trace"}

// profileValue is the pflag.Value backing --profile: a string
// constrained to profileModes (or "" for off). Set rejects anything
// else, so cobra surfaces the error (with usage) at parse time, before
// the command runs — instead of deferring to startProfile's runtime
// default case after all other init has happened. Mirrors outputValue.
type profileValue struct{ target *string }

func (p *profileValue) String() string { return *p.target }
func (p *profileValue) Type() string   { return "string" }

func (p *profileValue) Set(v string) error {
	if v == "" || slices.Contains(profileModes, v) {
		*p.target = v
		return nil
	}
	return fmt.Errorf("must be one of: %s", strings.Join(profileModes, ", "))
}

// runOrchestratorCfg routes the CLI through the embed-friendly
// Orchestrator.Render entry point. Returns the populated orchestrator
// (for Store lookups the CLI legitimately needs — object listings,
// status queries, filter scope) AND the structured render Result.
// Both stay non-nil when Bootstrap succeeded, even if Run had per-
// resource failures: the partial output is still usable. A nil
// orchestrator indicates a fatal init/bootstrap error and callers
// should bail.
func runOrchestratorCfg(ctx context.Context, cfg orchestrator.Config) (*orchestrator.Orchestrator, *orchestrator.Result, error) {
	o, err := orchestrator.New(cfg)
	if err != nil {
		return nil, nil, err
	}
	res, err := o.Render(ctx)
	// Render nils the result only when Bootstrap fails (Run-time
	// per-resource failures still produce a non-nil Result). Drop
	// the partial orchestrator in that case: every CLI verb gates
	// on `o == nil` to surface the underlying error, but without
	// this nil-out the verb would proceed to read a Store that's
	// partially populated by the discovery pre-Bootstrap walk and
	// produce confusing output — e.g. `flate test all` reporting
	// every loaded resource as "FAILED (no status reported)"
	// instead of surfacing the actual Bootstrap error (an
	// unimplemented ResourceSet inputStrategy, a YAML schema
	// rejection, etc.). Issue surfaced by tholinka/home-ops where
	// a Permute ResourceSet drowned the real "not yet implemented"
	// message under 247 phantom failures.
	if res == nil {
		return nil, nil, err
	}
	return o, res, err
}

func scopedRunError(o *orchestrator.Orchestrator, res *orchestrator.Result, c *commonFlags, runErr error) error {
	if runErr == nil {
		return nil
	}
	if o == nil || res == nil {
		return runErr
	}
	extras := nonResourceRunErrors(runErr)
	failed := make(map[manifest.NamedResource]store.StatusInfo, len(res.Failed))
	for id, info := range res.Failed {
		if c == nil || c.includeNamespace(o.Filter(), id.Namespace) {
			failed[id] = info
		}
	}
	if len(failed) == 0 {
		return errors.Join(extras...)
	}
	return errors.Join(aggregateScopedFailures(failed), errors.Join(extras...))
}

// resourceAggregatePrefix is the leading text of the error
// aggregateScopedFailures produces. nonResourceRunErrors uses it to tell
// the per-resource failure aggregate apart from incidental run errors, so
// both must reference this one constant or the classification silently
// drifts.
const resourceAggregatePrefix = "reconcile completed with "

func aggregateScopedFailures(failed map[manifest.NamedResource]store.StatusInfo) error {
	msgs := make([]string, 0, len(failed))
	for id, info := range failed {
		msgs = append(msgs, fmt.Sprintf("%s: %s", id, info.Message))
	}
	slices.Sort(msgs)
	return fmt.Errorf("%s%d failure(s):\n  %s",
		resourceAggregatePrefix, len(msgs), strings.Join(msgs, "\n  "))
}

func nonResourceRunErrors(err error) []error {
	var out []error
	for _, leaf := range flattenErrors(err) {
		if isResourceAggregateError(leaf) {
			continue
		}
		out = append(out, leaf)
	}
	return out
}

func flattenErrors(err error) []error {
	if err == nil {
		return nil
	}
	if uw, ok := err.(interface{ Unwrap() []error }); ok {
		var out []error
		for _, child := range uw.Unwrap() {
			out = append(out, flattenErrors(child)...)
		}
		return out
	}
	if uw, ok := err.(interface{ Unwrap() error }); ok {
		return flattenErrors(uw.Unwrap())
	}
	return []error{err}
}

func isResourceAggregateError(err error) bool {
	return strings.HasPrefix(err.Error(), resourceAggregatePrefix)
}

func cmdContext(cmd *cobra.Command) context.Context {
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}
