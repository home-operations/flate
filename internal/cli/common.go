package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
	"github.com/home-operations/flate/pkg/store"
)

const (
	sourceRetryAttempts = 3
	sourceRetryMinWait  = 200 * time.Millisecond
	sourceRetryMaxWait  = 3 * time.Second
	sourceRetryJitter   = 0.1
)

type commonFlags struct {
	input     *store.SourceArtifact
	baseInput *store.SourceArtifact
	changes   *change.Set
	owner     *sourcecache.Cache
	path      string
	pathOrig  string
	// Synthetic baseline labels need explicit source coordinates and identity.
	pathRoot             string
	pathOrigRoot         string
	pathOrigSelfURLs     []string
	selfURLs             []string
	base                 string
	namespace            string
	substituteFrom       []string
	substitute           []string
	strictSubstitutions  bool
	substitutionSources  []orchestrator.SubstitutionSource
	substitutionValues   map[string]string
	allowMissingSecrets  bool
	allowMissingCRDs     bool
	forceGenericProvider bool
	restrictEgress       bool
	skipKinds            []string
	output               string
	registryConfig       string
	concurrency          int
	// gitDepth caps the shallow-clone history depth for GitRepository
	// sources. Default 1 (opt-out shallow); 0 = full clone. Refs pinned
	// to an explicit commit always full-clone. Plumbed into
	// orchestrator.Config.GitDepth → git.Fetcher.Depth.
	gitDepth int
	cacheDir string
}

// bindCommon wires the flags every reconcile-running subcommand shares.
// outputs is the subcommand's accepted -o values in help-display order:
// the first is the default the flag takes when -o is omitted, and the flag
// (an outputValue enum) rejects any value outside the set at parse time.
// Each subcommand passes only the formats it honors — e.g. build doesn't
// claim `name`, and diff doesn't claim `table`. Passing no formats (test,
// which emits one fixed report) registers no -o flag at all.
func bindCommon(fs *pflag.FlagSet, f *commonFlags, outputs ...output.Format) {
	fs.StringVarP(&f.path, "path", "p", ".", "path to the Flux cluster directory")
	fs.StringVarP(&f.pathOrig, "path-orig", "P", "",
		"baseline path; when set, every command runs in changed-only mode")
	bindBase(fs, f)
	fs.StringVarP(&f.namespace, "namespace", "n", "",
		"limit to this namespace (default: every namespace, or just the changed ones when --path-orig is set)")
	fs.BoolVar(&f.allowMissingCRDs, "allow-missing-crds", false,
		"accept CRD dependencies absent from the offline inputs because the cluster installs them at runtime")
	fs.StringArrayVar(&f.substituteFrom, "substitute-from", nil,
		"ConfigMap/Secret YAML file for postBuild references (repeatable; later identities replace earlier ones)")
	fs.StringArrayVar(&f.substitute, "substitute", nil,
		"postBuild KEY=VALUE override (repeatable; applied after references and inline values)")
	fs.BoolVar(&f.strictSubstitutions, "strict-substitutions", false,
		"fail postBuild expansion on undefined variables without defaults")
	fs.BoolVar(&f.allowMissingSecrets, "allow-missing-secrets", false,
		"soft-skip sources with absent or wiped auth/cert/proxy Secrets and HelmRelease valuesFrom Secret/ConfigMap refs "+
			"that only materialize in the live cluster. Usually unnecessary: a missing Secret "+
			"declared by an in-repo ExternalSecret/SealedSecret (with a static target name) is "+
			"auto-skipped without this flag. Malformed real material still fails.")
	fs.BoolVar(&f.forceGenericProvider, "force-generic-provider", false,
		"route non-generic spec.provider sources (GitRepository/OCIRepository/Bucket) "+
			"through the generic credential path instead of failing. With a secretRef, "+
			"static credentials render offline and a missing one is soft-skipped by "+
			"--allow-missing-secrets; with no secretRef, Git/Bucket go anonymous and OCI "+
			"falls back to --registry-config, so a private source fails on auth.")
	fs.BoolVar(&f.restrictEgress, "restrict-egress", false,
		"untrusted-render guard: block source fetches (kustomize remote resources/bases, "+
			"Git/OCI/Helm/Bucket sources) to loopback, RFC1918, link-local, and cloud-metadata "+
			"(169.254.169.254) addresses, AND reject file:// / non-https-ssh GitRepository sources "+
			"(no reading arbitrary in-pod repos). Off by default — only enable when rendering "+
			"UNTRUSTED input; trusted repos legitimately reach private/LAN hosts and use local "+
			"file:// self-sources. A configured proxy and ssh:// git egress are not covered (operator-owned).")
	fs.StringSliceVar(&f.skipKinds, "skip-kinds",
		[]string{manifest.KindSecret, manifest.KindCustomResourceDefinition}, "kinds to exclude from rendered output (empty includes all kinds)")
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
		"opt-in on-disk cache root for source artifacts, helm charts, and render output (default: memory)")
	fs.IntVar(&f.concurrency, "concurrency", runtime.NumCPU()*4,
		"max parallel reconcile bodies (0 = unbounded)")
	// Shallow clone is the dominant speedup for GitRepository sources that
	// pull a subdir out of a deep-history monorepo: depth=1 fetches only
	// the tip commit, which is all the worktree materialization needs.
	// Commit-pinned refs always full-clone (the pinned commit may be deep).
	fs.IntVar(&f.gitDepth, "git-depth", 1,
		"shallow-clone depth for GitRepository sources; 0 = full clone "+
			"(refs pinned to an explicit commit always full-clone)")
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

// bindBase selects a baseline revision for changed-only mode.
//
// Semantics differ per verb:
//   - diff REQUIRES a baseline; bare command auto-detects via
//     merge-base with @{u} / origin/HEAD / origin/{main,master}.
//   - build/get/test do NOT auto-detect — bare command tests/builds
//     everything (preserves the existing "full tree" default).
//     Setting --base on these verbs opts into changed-only mode
//     against the named rev, sharing diff's admitted input pair.
//
// Mutually exclusive with --path-orig (the absolute-path escape
// hatch); the check fires at runtime in resolveBaseline.
func bindBase(fs *pflag.FlagSet, f *commonFlags) {
	fs.StringVar(&f.base, "base", "",
		"baseline git rev (e.g. main, origin/main, HEAD~3, SHA) for changed-only mode. "+
			"A rev the checkout doesn't hold is fetched from its origin remote. "+
			"On `diff`, omitting --base auto-detects via merge-base with @{u} / origin/HEAD. "+
			"On `build`/`get`/`test`, omitting --base keeps the default full-tree behavior. "+
			"Mutually exclusive with --path-orig.")
}

func (c *commonFlags) roots(name string, kinds ...string) []manifest.NamedResource {
	roots := make([]manifest.NamedResource, len(kinds))
	for i, kind := range kinds {
		roots[i] = manifest.NamedResource{Kind: kind, Namespace: c.namespace, Name: name}
	}
	return roots
}

// helmFlags collect the helm template options. Mirrors flux-local's
// --kube-version/--api-versions/--no-hooks/etc.
type helmFlags struct {
	kubeVersion                string
	apiVersions                string
	isUpgrade                  bool
	noHooks                    bool
	showOnly                   []string
	enableDNS                  bool
	disableChartDigestTracking *bool
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
	fs.BoolFunc("disable-chart-digest-tracking",
		"preserve original OCI chart versions (absent: auto-detect; =false: force digest tracking)",
		func(text string) error {
			disabled, err := strconv.ParseBool(text)
			if err != nil {
				return err
			}
			h.disableChartDigestTracking = &disabled
			return nil
		})
}

func (c commonFlags) helmOptions(h helmFlags) helm.Options {
	return helm.Options{
		SkipCRDs: slices.Contains(c.skipKinds, manifest.KindCustomResourceDefinition),
		SkipKinds: slices.DeleteFunc(slices.Clone(c.skipKinds), func(kind string) bool {
			return kind == manifest.KindCustomResourceDefinition
		}),
		KubeVersion: h.kubeVersion,
		APIVersions: h.apiVersions,
		IsUpgrade:   h.isUpgrade,
		NoHooks:     h.noHooks,
		ShowOnly:    h.showOnly,
		EnableDNS:   h.enableDNS,
		SkipTests:   true,
	}
}

// resolveBaseline admits --base inputs through the command's cache owner.
// autoFallback selects automatic baseline resolution for diff commands.
// Callers MUST run cleanup after both orchestrators' readers have joined.
func resolveBaseline(ctx context.Context, c *commonFlags, autoFallback bool) (func() error, error) {
	noop := func() error { return nil }
	c.pathRoot, c.pathOrigRoot, c.pathOrigSelfURLs = "", "", nil
	if c.pathOrig != "" && c.base != "" {
		return noop, errors.New("--path-orig and --base are mutually exclusive")
	}
	if c.pathOrig == "" && c.base == "" && !autoFallback {
		return noop, nil
	}
	current, original := c.path, c.pathOrig
	for _, path := range []*string{&original, &current} {
		if *path == "" {
			continue
		}
		resolved, err := discovery.ResolveScanPath(*path)
		if err != nil {
			return noop, err
		}
		*path = resolved
	}
	if original != "" {
		var err error
		c.pathRoot, c.pathOrigRoot, c.pathOrigSelfURLs, err = baseline.ResolvePaths(ctx, current, original)
		return noop, err
	}
	ssrfguard.Restrict(c.restrictEgress)
	c.path = current
	owner, err := sourcecache.New(ctx, c.cacheDir)
	if err != nil {
		return noop, err
	}
	res, err := baseline.AutoResolve(ctx, c.path, c.base, owner)
	if err != nil {
		return noop, errors.Join(err, owner.Close())
	}
	rel, err := filepath.Rel(res.Source.LocalPath, c.path)
	if err != nil {
		return noop, errors.Join(err, owner.Close())
	}
	c.owner, c.input, c.baseInput, c.changes = owner, res.Source, res.BaseInput, res.Changes
	c.pathOrig = filepath.Join(res.BaseInput.LocalPath, rel)
	c.pathRoot = res.Source.LocalPath
	c.pathOrigRoot = res.BaseInput.LocalPath
	c.pathOrigSelfURLs = res.SelfURLs
	c.selfURLs = res.SelfURLs
	slog.Debug("baseline", "source", res.Source.Metadata["baseline-selection"], "rev", res.Rev, "path_orig", c.pathOrig)
	return owner.Close, nil
}

func buildOrchCfg(c commonFlags, h helmFlags) orchestrator.Config {
	return orchestrator.Config{
		Path: c.path, RepoRoot: c.pathRoot,
		Input: c.input, BaseInput: c.baseInput, ExternalChanges: c.changes, Cache: c.owner,
		SelfURLs:                   c.selfURLs,
		PathOrig:                   c.pathOrigRoot,
		HelmOptions:                c.helmOptions(h),
		DisableChartDigestTracking: h.disableChartDigestTracking,
		WipeSecrets:                true,
		RegistryConfig:             c.registryConfig,
		Concurrency:                c.concurrency,
		SourceRetry: sourcecache.RetryConfig{
			Attempts: sourceRetryAttempts,
			MinWait:  sourceRetryMinWait,
			MaxWait:  sourceRetryMaxWait,
			Jitter:   sourceRetryJitter,
		},
		GitDepth:             c.gitDepth,
		SubstituteFrom:       c.substitutionSources,
		Substitute:           c.substitutionValues,
		StrictSubstitutions:  c.strictSubstitutions,
		AllowMissingSecrets:  c.allowMissingSecrets,
		AllowMissingCRDs:     c.allowMissingCRDs,
		ForceGenericProvider: c.forceGenericProvider,
		RestrictEgress:       c.restrictEgress,
		CacheDir:             c.cacheDir,
	}
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
	if err := c.loadSubstitutions(ctx); err != nil {
		return nil, nil, err
	}
	cleanup, err := resolveBaseline(ctx, &c, false)
	if err != nil {
		return nil, nil, err
	}
	o, res, runErr := runOrchestratorCfg(ctx, buildOrchCfg(c, h))
	return o, res, errors.Join(runErr, cleanup())
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
func outputUsage(outputs []output.Format) string {
	return "output format: " + joinOutputs(outputs)
}

// joinOutputs renders accepted -o formats as a comma-separated list, shared
// by the help text and the parse-time rejection so the two can't drift.
func joinOutputs(outputs []output.Format) string {
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
	allowed []output.Format
}

func (o *outputValue) String() string { return *o.target }
func (o *outputValue) Type() string   { return "string" }

func (o *outputValue) Set(v string) error {
	if slices.Contains(o.allowed, output.Format(v)) {
		*o.target = v
		return nil
	}
	return fmt.Errorf("must be one of: %s", joinOutputs(o.allowed))
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
	o, err := orchestrator.New(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	// Live status bar on stderr (TTY-gated via barSink). A Bubble Tea program
	// runs the inline frame; store status events feed it via Program.Send, so
	// its counters and spinner track the reconcile as it runs. stdout (the
	// rendered output) is untouched. Defers run LIFO: the unsubscribe fires,
	// then finishMsg clears the frame and the program is detached from slog
	// before Render's callers print anything.
	if barSink != nil {
		// With no input attached, Bubble Tea's startup terminal queries would
		// get answered into the shell's input buffer; queryFilterFile strips
		// them (progressBarEnabled guarantees out is a TTY *os.File).
		out := barSink.out
		if f, ok := out.(*os.File); ok {
			out = &queryFilterFile{File: f}
		}
		p := tea.NewProgram(newBarModel(barSink.color),
			tea.WithOutput(out),
			tea.WithInput(nil),         // output-only: never capture the keyboard
			tea.WithoutSignalHandler(), // the CLI context owns Ctrl-C, not the bar
		)
		barSink.setProgram(p)
		go func() { _, _ = p.Run() }()
		// kustomize writes deprecation/sort-order warnings straight to the
		// stderr fd (no injectable writer), which would shred the inline frame.
		// Capture the fd for the bar's lifetime and demote those lines to
		// slog.Debug — flate's own structured warnings still reach the user
		// above the frame via barSink, while dependency fd chatter stays off the
		// clean bar unless --log-level debug asks for it.
		restoreStderr := captureStderr(func(line string) { slog.Debug(line) })
		unsub := o.Store().AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
			p.Send(statusMsg{id: id, info: payload.(store.StatusInfo)})
		}, false)
		defer func() {
			unsub()
			restoreStderr() // restore os.Stderr + flush the drain while the frame is still live
			p.Send(finishMsg{})
			p.Wait()
			barSink.setProgram(nil)
		}()
	}
	res, err := o.Render(ctx)
	// A Bootstrap failure has no Result. Callers must receive a nil
	// orchestrator so they surface the error instead of reading a
	// partially populated Store.
	if res == nil {
		return nil, nil, err
	}
	return o, res, err
}

// logBuffer is the process-wide deferred-log sink installed by setLogLevel; the
// noisy Warn/Info chatter a failing run emits is held here and rendered in the
// report footer instead of interleaving with output. nil only in tests that
// bypass root setup.
var logBuffer *deferSink

// drainLogNotes returns and clears the buffered log lines (nil when unset).
func drainLogNotes() []output.Note {
	if logBuffer == nil {
		return nil
	}
	return logBuffer.drain()
}

// reportedError marks an error whose human-facing report has already been
// rendered to stderr, so run's top-level printer skips the plain reprint.
type reportedError struct{ err error }

func (e reportedError) Error() string { return e.err.Error() }
func (e reportedError) Unwrap() error { return e.err }

type reportErrors struct {
	selected  error
	remainder error
}

func failureModel(result *orchestrator.Result) output.Model {
	var primary []output.Primary
	var missing []output.Missing
	blocked := result.BlockedCount()
	for root, victims := range result.Causes {
		if info, isFailed := result.Failed[root]; isFailed {
			primary = append(primary, output.Primary{ID: root, Msg: info.Message, Blocks: victims})
		} else {
			missing = append(missing, output.Missing{ID: root, RequiredBy: victims})
		}
	}

	slices.SortFunc(primary, func(a, b output.Primary) int { return a.ID.Compare(b.ID) })
	slices.SortFunc(missing, func(a, b output.Missing) int { return a.ID.Compare(b.ID) })
	return output.Model{Primary: primary, Missing: missing, Warnings: result.Unrelated, Blocked: blocked}
}

// reportFailures preserves the command's named error-write policy.
func reportFailures(w io.Writer, m output.Model, named bool, errs reportErrors) error {
	if m.Empty() {
		return errs.selected
	}
	encoder := output.Encoder{Writer: w, Color: output.ColorEnabled(w)}
	if writeErr := encoder.Report(m); writeErr != nil && named {
		return errors.Join(writeErr, errs.selected)
	}
	if writeErr := encoder.Errors(errs.remainder); writeErr != nil {
		return errors.Join(writeErr, errs.selected)
	}
	if errs.selected == nil {
		return nil
	}
	return reportedError{errs.selected}
}

func cmdContext(cmd *cobra.Command) context.Context {
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}
