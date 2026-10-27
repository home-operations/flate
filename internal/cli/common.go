package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"time"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/source"
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
	supply               []string
	substitute           []string
	strictSubstitutions  bool
	suppliedObjects      []store.SuppliedObject
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

	helm helm.Options
}

func bindCommon(fs *pflag.FlagSet, f *commonFlags) {
	fs.StringVarP(&f.path, "path", "p", ".", "path to the Flux cluster directory")
	fs.StringVarP(&f.pathOrig, "path-orig", "P", "",
		"baseline path; when set, every command runs in changed-only mode")
	bindBase(fs, f)
	fs.StringVarP(&f.namespace, "namespace", "n", "",
		"limit to this namespace (default: every namespace, or just the changed ones when --path-orig is set)")
	fs.BoolVar(&f.allowMissingCRDs, "allow-missing-crds", false,
		"accept CRD dependencies absent from the offline inputs because the cluster installs them at runtime")
	fs.StringArrayVar(&f.supply, "supply", nil,
		"Secret/ConfigMap YAML file for supported data and credential references (repeatable; later identities replace earlier ones)")
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
	fs.StringVar(&f.registryConfig, "registry-config", "", "docker config.json for OCI authentication")
	bindCacheDir(fs, &f.cacheDir,
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
	fs.StringVar(&f.helm.KubeVersion, "kube-version", helm.BundledKubeVersion(),
		"Kubernetes version for .Capabilities.KubeVersion (default: version bundled with flate)")
	fs.StringVarP(&f.helm.APIVersions, "api-versions", "a", "", "comma-separated API versions for .Capabilities.APIVersions")
	fs.BoolVar(&f.helm.IsUpgrade, "is-upgrade", false, "set .Release.IsUpgrade instead of .Release.IsInstall")
	fs.BoolVar(&f.helm.NoHooks, "no-hooks", false, "exclude hook-annotated templates")
	fs.StringSliceVarP(&f.helm.ShowOnly, "show-only", "s", nil, "only show specific template paths (repeatable)")
	fs.BoolVar(&f.helm.EnableDNS, "enable-dns", false, "enable DNS lookups during helm template")
	fs.BoolVar(&f.helm.DisableChartDigestTracking, "disable-chart-digest-tracking", false,
		"set when the cluster runs helm-controller with --feature-gates=DisableChartDigestTracking=true; "+
			"=true preserves original OCI chart versions, =false enables tracking (default); CLI overrides the environment")
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
		for info := range manifest.Kinds() {
			if info.Kind == kind {
				roots[i].Group = info.Group
				break
			}
		}
	}
	return roots
}

func resolveBaseline(ctx context.Context, c *commonFlags, autoFallback bool) error {
	c.pathRoot, c.pathOrigRoot, c.pathOrigSelfURLs = "", "", nil
	if c.pathOrig != "" && c.base != "" {
		return errors.New("--path-orig and --base are mutually exclusive")
	}
	if c.pathOrig == "" && c.base == "" && !autoFallback {
		return nil
	}
	if c.pathOrig != "" {
		res, err := baseline.ResolvePaths(ctx, c.path, c.pathOrig)
		if err != nil {
			return err
		}
		c.input, c.baseInput = res.Source, res.BaseInput
		c.path, c.pathOrig = res.ScanPath, res.BaseScanPath
		c.pathRoot, c.pathOrigRoot = res.Source.LocalPath, res.BaseInput.LocalPath
		c.pathOrigSelfURLs = res.SelfURLs
		return nil
	}
	res, err := baseline.AutoResolve(ctx, c.path, c.base, c.owner)
	if res != nil {
		c.input, c.baseInput = res.Source, res.BaseInput
	}
	if err != nil {
		return err
	}
	c.input, c.baseInput, c.changes = res.Source, res.BaseInput, res.Changes
	c.path, c.pathOrig = res.ScanPath, res.BaseScanPath
	c.pathRoot = res.Source.LocalPath
	c.pathOrigRoot = res.BaseInput.LocalPath
	c.pathOrigSelfURLs = res.SelfURLs
	c.selfURLs = res.SelfURLs
	rev := res.BaseInput.Revision
	slog.Debug("baseline", "source", res.Source.Metadata["baseline-selection"], "rev", rev[:min(len(rev), 7)], "path_orig", c.pathOrig)
	return nil
}

func inputTrees(c commonFlags) []orchestrator.Tree {
	trees := []orchestrator.Tree{{
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		RepoRoot: c.pathRoot, Path: c.path, SelfURLs: c.selfURLs,
		Input: c.input, BaseInput: c.baseInput, ExternalChanges: c.changes,
	}}
	if c.pathOrig != "" {
		trees = append([]orchestrator.Tree{{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
			RepoRoot: c.pathOrigRoot, Path: c.pathOrig, SelfURLs: c.pathOrigSelfURLs,
			Input: c.baseInput, BaseInput: c.input, ExternalChanges: c.changes,
		}}, trees...)
	}
	return trees
}

func buildOrchCfg(c commonFlags) orchestrator.Config {
	c.helm.SkipTests = true
	c.helm.SkipCRDs = slices.Contains(c.skipKinds, manifest.KindCustomResourceDefinition)
	c.helm.SkipKinds = slices.DeleteFunc(slices.Clone(c.skipKinds), func(kind string) bool {
		return kind == manifest.KindCustomResourceDefinition
	})
	return orchestrator.Config{
		Cache:          c.owner,
		HelmOptions:    c.helm,
		WipeSecrets:    true,
		RegistryConfig: c.registryConfig,
		Concurrency:    c.concurrency,
		SourceRetry: sourcecache.RetryConfig{
			Attempts: sourceRetryAttempts,
			MinWait:  sourceRetryMinWait,
			MaxWait:  sourceRetryMaxWait,
			Jitter:   sourceRetryJitter,
		},
		GitDepth:             c.gitDepth,
		Supply:               c.suppliedObjects,
		Substitute:           c.substitutionValues,
		StrictSubstitutions:  c.strictSubstitutions,
		AllowMissingSecrets:  c.allowMissingSecrets,
		AllowMissingCRDs:     c.allowMissingCRDs,
		ForceGenericProvider: c.forceGenericProvider,
		RestrictEgress:       c.restrictEgress,
		CacheDir:             c.cacheDir,
	}
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

// reportedError retains a fatal cause already presented to the user.
type reportedError struct{ err error }

func (e reportedError) Error() string { return e.err.Error() }
func (e reportedError) Unwrap() error { return e.err }

func failureModel(result *selection.Selection) output.Model {
	var primary []output.Primary
	var missing []output.Missing
	blocked := result.BlockedCount()
	for root, victims := range result.Causes {
		if info, isFailed := result.Failed[root]; isFailed {
			primary = append(primary, output.Primary{ID: root, Msg: info.String(), Blocks: victims})
		} else {
			missing = append(missing, output.Missing{ID: root, RequiredBy: victims})
		}
	}

	slices.SortFunc(primary, func(a, b output.Primary) int { return a.ID.Compare(b.ID) })
	slices.SortFunc(missing, func(a, b output.Missing) int { return a.ID.Compare(b.ID) })
	return output.Model{Primary: primary, Missing: missing, Blocked: blocked}
}

var renderKinds, runnableKinds = func() ([]string, []string) {
	var render, runnable []string
	for info := range manifest.Kinds() {
		if info.Render {
			render = append(render, info.Kind)
		}
		if info.Runnable {
			runnable = append(runnable, info.Kind)
		}
	}
	return render, runnable
}()

type selectedRequest struct {
	notice         manifest.Warning
	flags          commonFlags
	roots          []manifest.NamedResource
	labels         map[string]string
	pair           bool
	stdout, stderr io.Writer
	test           bool
}

type selectedInvocation struct {
	groups         []selectedGroup
	err, remainder error
	elapsed        time.Duration
}

type selectedGroup struct {
	cluster        string
	sides          []*selection.Selection
	absent         [2]bool
	err, remainder error
	complete       bool
}

type selectedPayload struct {
	err      error
	warnings [][]manifest.Warning
	rosters  []output.TestReport
	subjects [][]manifest.NamedResource
	prepare  func([]output.Names)
	write    func() error
}

func runSelected(ctx context.Context, req selectedRequest, payload func(*selectedInvocation) selectedPayload) error {
	start := time.Now()
	req.notice, _ = ctx.Value(renderNoticeKey{}).(manifest.Warning)
	early := func(err error) error {
		return finishSelected(req, &selectedInvocation{err: err, remainder: err}, selectedPayload{})
	}
	c := &req.flags
	if !req.pair {
		if c.path == "" {
			return early(errors.New("path is required"))
		}
		if err := validatePathFlag("--path", c.path); err != nil {
			return early(err)
		}
		if c.pathOrig != "" {
			if err := validatePathFlag("--path-orig", c.pathOrig); err != nil {
				return early(err)
			}
		}
	}
	if err := c.loadInputs(ctx); err != nil {
		return early(err)
	}
	ssrfguard.Restrict(c.restrictEgress)
	owner, err := sourcecache.New(ctx, c.cacheDir)
	if err != nil {
		return early(err)
	}
	c.owner = owner
	cfg := buildOrchCfg(*c)
	cfg.HelmClient, cfg.Network = helm.NewClient(owner), new(source.Network)
	defer cfg.Network.Close()
	inv := selectedInvocation{}
	if err := resolveBaseline(ctx, c, req.pair); err != nil {
		inv.err, inv.remainder = err, err
		group := selectedGroup{cluster: "."}
		for _, input := range []*store.SourceArtifact{c.baseInput, c.input} {
			if req.pair || len(group.sides) == 0 {
				group.sides = append(group.sides, &selection.Selection{})
			}
			if input != nil {
				side := group.sides[len(group.sides)-1]
				side.Warnings = append(side.Warnings, input.Warnings...)
			}
		}
		inv.groups = append(inv.groups, group)
	} else {
		cfg.HeadOnly = !req.pair && c.baseInput != nil
		var stop func()
		if barSink != nil {
			cfg.StatusListener, stop = barSink.progress(ctx)
		}
		runs, runErr := orchestrator.RenderTrees(ctx, inputTrees(*c), cfg)
		if stop != nil {
			stop()
		}
		if len(runs) == 0 {
			inv.err, inv.remainder = runErr, runErr
		}
		for i := 0; i < len(runs); {
			j := i + 1
			for j < len(runs) && runs[j].ID.Cluster == runs[i].ID.Cluster {
				j++
			}
			group := selectedGroup{cluster: string(runs[i].ID.Cluster), complete: true}
			selected := runs[i:j]
			i = j
			var groupErr error
			for _, run := range selected {
				groupErr = errors.Join(groupErr, run.Err)
			}
			if !req.pair {
				base := selected[0]
				selected = selected[len(selected)-1:]
				if selected[0].ID.Side != manifest.TreeHead || selected[0].Absent {
					inv.err, inv.remainder = errors.Join(inv.err, groupErr), errors.Join(inv.remainder, groupErr)
					continue
				}
				if selected[0].Orchestrator == nil && base.ID.Side == manifest.TreeBase && base.Result != nil {
					result := orchestrator.Result{}
					if selected[0].Result != nil {
						result = *selected[0].Result
					}
					result.Warnings = append(slices.Clone(base.Result.Warnings), result.Warnings...)
					selected[0].Result = &result
				}
			}
			for _, run := range selected {
				group.absent[run.ID.Side] = run.Absent
				group.complete = group.complete && (run.Absent || run.Orchestrator != nil && run.Result != nil)
			}
			group.sides, group.err, group.remainder = orchestrator.Select(selected, groupErr, req.roots, req.labels)
			inv.groups = append(inv.groups, group)
		}
	}
	inv.elapsed = time.Since(start)
	root := req.roots[0]
	count, complete, incomplete := 0, false, false
	for _, group := range inv.groups {
		complete = complete || group.complete
		incomplete = incomplete || !group.complete
		for _, side := range group.sides {
			count += side.Counts[root.Kind]
		}
	}
	var result selectedPayload
	if root.Name != "" && count == 0 && complete && !incomplete {
		path := "--path"
		if req.pair {
			path = "--path or --path-orig"
		}
		missing := noNamedError(root.Kind, root.Name, path)
		inv.err = errors.Join(missing, inv.err)
		inv.remainder = errors.Join(missing, inv.remainder)
	} else if complete {
		if root.Name != "" {
			inv.groups = slices.DeleteFunc(inv.groups, func(group selectedGroup) bool {
				return group.complete && !slices.ContainsFunc(group.sides, func(side *selection.Selection) bool { return side.Counts[root.Kind] != 0 })
			})
		}
		result = payload(&inv)
	}
	closeErr := owner.Close()
	inv.err, inv.remainder = errors.Join(inv.err, closeErr), errors.Join(inv.remainder, closeErr)
	return finishSelected(req, &inv, result)
}

func finishSelected(req selectedRequest, inv *selectedInvocation, payload selectedPayload) error {
	final := errors.Join(payload.err, inv.err)
	diagnostic := errors.Join(payload.err, inv.remainder)
	report := output.Report{Groups: make([]output.ReportGroup, len(inv.groups)), Test: payload.rosters != nil, Elapsed: inv.elapsed}
	if req.notice.Category != "" {
		report.Warnings = append(report.Warnings, output.Warning{Warning: req.notice})
	}
	named := len(req.roots) > 0 && req.roots[0].Name != ""
	for i, group := range inv.groups {
		g := &report.Groups[i]
		g.Cluster = group.cluster
		if i < len(payload.rosters) {
			g.Test = &payload.rosters[i]
		}
		groupErr, remainder := group.err, group.remainder
		if len(inv.groups) > 1 {
			if groupErr != nil {
				groupErr = fmt.Errorf("cluster %s: %w", group.cluster, groupErr)
			}
			if remainder != nil {
				remainder = fmt.Errorf("cluster %s: %w", group.cluster, remainder)
			}
		}
		final, diagnostic = errors.Join(final, groupErr), errors.Join(diagnostic, remainder)
		for j, side := range group.sides {
			var model output.Model
			if !req.test || named && group.err != nil {
				model = failureModel(side)
			}
			if g.Test != nil {
				model.Primary = slices.DeleteFunc(model.Primary, func(p output.Primary) bool {
					return slices.ContainsFunc(g.Test.Cases, func(c output.Case) bool { return c.ID == p.ID && c.Outcome == output.OutcomeFailed })
				})
			}
			context := ""
			if req.pair {
				context = []string{"orig snapshot", "current snapshot"}[j]
			}
			model.Context = context
			g.Models = append(g.Models, model)
			for _, warning := range selectedWarnings(side) {
				report.Warnings = append(report.Warnings, output.Warning{Warning: warning, Group: i, Context: context})
			}
		}
		if i < len(payload.warnings) {
			for _, warning := range payload.warnings[i] {
				report.Warnings = append(report.Warnings, output.Warning{Warning: warning, Group: i})
			}
		}
	}
	names := report.Prepare(payload.subjects)
	if payload.prepare != nil {
		payload.prepare(names)
	}
	if payload.write != nil {
		err := payload.write()
		final, diagnostic = errors.Join(final, err), errors.Join(diagnostic, err)
	}
	if report.Test && slices.ContainsFunc(payload.rosters, output.TestReport.AnyFailed) &&
		(!named || inv.err == nil && !slices.ContainsFunc(inv.groups, func(g selectedGroup) bool { return g.err != nil })) {
		final = errors.Join(errors.New("test failures detected"), final)
	}
	encoder := output.Encoder{Writer: req.stderr, Color: output.ColorEnabled(req.stderr)}
	out := output.Encoder{Writer: req.stdout, Color: output.ColorEnabled(req.stdout)}
	report.Error = diagnostic
	writeErr := encoder.Report(report, out)
	final = errors.Join(final, writeErr)
	if err := encoder.Errors(writeErr); err != nil {
		return errors.Join(final, err)
	}
	if final != nil {
		presented := diagnostic != nil || writeErr != nil || report.Test && slices.ContainsFunc(payload.rosters, output.TestReport.AnyFailed)
		for _, group := range report.Groups {
			presented = presented || slices.ContainsFunc(group.Models, func(m output.Model) bool { return len(m.Primary)+len(m.Missing) > 0 || m.Blocked > 0 })
		}
		if presented {
			return reportedError{final}
		}
		return final
	}
	return nil
}

func selectedWarnings(side *selection.Selection) []manifest.Warning {
	total := len(side.Warnings) + len(side.Unrelated) + len(side.External)
	if total == 0 {
		return nil
	}
	type identity struct {
		resource          manifest.NamedResource
		category, message string
		count             int
	}
	seen := make(map[identity][]int, total)
	warnings := make([]manifest.Warning, 0, total)
	for _, group := range [][]manifest.Warning{side.Warnings, side.Unrelated, side.External} {
		for _, warning := range group {
			key := identity{warning.Resource, warning.Category, warning.Message, warning.Count}
			if slices.ContainsFunc(seen[key], func(i int) bool {
				return slices.Equal(warnings[i].Detail, warning.Detail)
			}) {
				continue
			}
			seen[key] = append(seen[key], len(warnings))
			warnings = append(warnings, warning)
		}
	}
	return warnings
}
