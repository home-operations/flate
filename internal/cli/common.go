package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
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
	sourceRetryAttempts      = 3
	defaultConcurrencyPerCPU = 4
	shortRevisionLength      = 7
	sourceRetryMinWait       = 200 * time.Millisecond
	sourceRetryMaxWait       = 3 * time.Second
	sourceRetryJitter        = 0.1
)

type commonFlags struct {
	input     *store.SourceArtifact
	baseInput *store.SourceArtifact
	changes   *change.Set
	owner     *sourcecache.Cache
	path      string
	pathOrig  string
	// Synthetic baseline labels need explicit source coordinates and identity.
	pathRoot            string
	pathOrigRoot        string
	pathOrigSelfURLs    []string
	selfURLs            []string
	base                string
	namespace           string
	supply              []string
	strictSubstitutions bool
	suppliedObjects     []store.Declaration
	allowMissingKinds   []string
	restrictEgress      bool
	skipKinds           []string
	output              string
	registryConfig      string
	concurrency         int
	// gitDepth caps the shallow-clone history depth for GitRepository
	// sources. Default 1 (opt-out shallow); 0 = full clone. Refs pinned
	// to an explicit commit always full-clone. Plumbed into
	// orchestrator.Config.GitDepth → git.Fetcher.Depth.
	gitDepth int
	cacheDir string

	helm helm.Options
}

func bindCommon(fs *pflag.FlagSet, f *commonFlags) {
	fs.StringVarP(&f.path, pathKey, "p", ".", "Read Flux manifests from this directory")
	fs.StringVarP(&f.pathOrig, "path-orig", "P", "", "Compare with a baseline directory (exclusive with --base)")
	bindBase(fs, f)
	fs.StringVarP(&f.namespace, namespaceKey, "n", "", "Select resources in this namespace")
	fs.StringArrayVar(
		&f.supply,
		"supply",
		nil,
		"Read reference-only Kubernetes objects from YAML; - reads stdin (repeatable)",
	)
	fs.BoolVar(
		&f.strictSubstitutions,
		"strict-substitutions",
		false,
		"Match kustomize-controller StrictPostBuildSubstitutions",
	)
	fs.StringSliceVar(
		&f.allowMissingKinds,
		"allow-missing-kinds",
		nil,
		"Tolerate missing cluster inputs: Secret,ConfigMap,CustomResourceDefinition. Missing source auth without a producer first tries native registry helpers, SSH agent, Helm login or AWS environment/shared-profile/IAM credentials",
	)
	fs.BoolVar(&f.restrictEgress, "restrict-egress", false, "Block private-address fetches and local Git transports")
	fs.StringSliceVar(
		&f.skipKinds,
		"skip-kinds",
		[]string{
			manifest.KindSecret,
			manifest.KindCustomResourceDefinition,
		},
		"Exclude these output kinds (empty includes all)",
	)
	fs.StringVar(
		&f.registryConfig,
		"registry-config",
		"",
		"Read OCI credentials through this native Docker/Helm store, including credential helpers",
	)
	bindCacheDir(fs, &f.cacheDir, "Store fetched sources and renders in this directory")
	fs.IntVar(
		&f.concurrency,
		"concurrency",
		runtime.NumCPU()*defaultConcurrencyPerCPU,
		"Limit parallel reconciles (0 means unbounded)",
	)
	// Shallow clone is the dominant speedup for GitRepository sources that
	// pull a subdir out of a deep-history monorepo: depth=1 fetches only
	// the tip commit, which is all the worktree materialization needs.
	// Commit-pinned refs always full-clone (the pinned commit may be deep).
	fs.IntVar(&f.gitDepth, "git-depth", 1, "Set Git fetch depth (0 means full history)")
	fs.StringVar(
		&f.helm.KubeVersion,
		"kube-version",
		helm.BundledKubeVersion(),
		"Set the cluster Kubernetes version for Helm",
	)
	fs.StringVarP(&f.helm.APIVersions, "api-versions", "a", "", "Set cluster APIs available to Helm (comma-separated)")
	fs.BoolVar(&f.helm.EnableDNS, "enable-dns", false, "Match helm-controller AllowDNSLookups")
	fs.BoolVar(
		&f.helm.DisableChartDigestTracking,
		"disable-chart-digest-tracking",
		false,
		"Match helm-controller DisableChartDigestTracking",
	)
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
	fs.StringToStringVarP(&f.labels, "selector", "l", nil, "Select labels by key=value (comma-separated)")
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
	fs.StringVar(&f.base, "base", "", "Compare with a Git revision (exclusive with --path-orig)")
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
	slog.Debug(
		"cli: baseline",
		sourceKey,
		res.Source.Metadata["baseline-selection"],
		"rev",
		rev[:min(len(rev), shortRevisionLength)],
		"path_orig",
		c.pathOrig,
	)
	return nil
}

func inputTrees(c *commonFlags) []orchestrator.Tree {
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

func buildOrchCfg(c *commonFlags) orchestrator.Config {
	helmOptions := c.helm
	helmOptions.SkipTests = true
	helmOptions.SkipCRDs = slices.Contains(c.skipKinds, manifest.KindCustomResourceDefinition)
	helmOptions.SkipKinds = slices.DeleteFunc(slices.Clone(c.skipKinds), func(kind string) bool {
		return kind == manifest.KindCustomResourceDefinition
	})
	return orchestrator.Config{
		Cache:          c.owner,
		HelmOptions:    helmOptions,
		WipeSecrets:    true,
		RegistryConfig: c.registryConfig,
		Concurrency:    c.concurrency,
		SourceRetry: sourcecache.RetryConfig{
			Attempts: sourceRetryAttempts,
			MinWait:  sourceRetryMinWait,
			MaxWait:  sourceRetryMaxWait,
			Jitter:   sourceRetryJitter,
		},
		GitDepth:            c.gitDepth,
		Supply:              c.suppliedObjects,
		StrictSubstitutions: c.strictSubstitutions,
		AllowMissingKinds:   c.allowMissingKinds,
		RestrictEgress:      c.restrictEgress,
		CacheDir:            c.cacheDir,
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

//nolint:gochecknoglobals // Derive manifest kind inventories once before parallel rendering.
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
	stdin          io.Reader
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

//nolint:gocritic // Each invocation owns mutable flag state without mutating the caller's request.
func runSelected(ctx context.Context, req selectedRequest, payload func(*selectedInvocation) selectedPayload) error {
	start := time.Now()
	req.notice, _ = ctx.Value(renderNoticeKey{}).(manifest.Warning)
	early := func(err error) error {
		return finishSelected(&req, &selectedInvocation{err: err, remainder: err}, &selectedPayload{})
	}
	c := &req.flags
	if err := c.loadInputs(ctx, req.stdin); err != nil {
		return early(err)
	}
	if err := validateSelectedPaths(&req); err != nil {
		return early(err)
	}

	ssrfguard.Restrict(c.restrictEgress)
	owner, err := sourcecache.New(ctx, c.cacheDir)
	if err != nil {
		return early(err)
	}
	c.owner = owner
	cfg := buildOrchCfg(c)
	cfg.HelmClient, cfg.Network = helm.NewClient(owner), new(source.Network)
	defer cfg.Network.Close()
	inv := selectedInvocation{}
	if err := resolveBaseline(ctx, c, req.pair); err != nil {
		inv.failBaseline(c, req.pair, err)
	} else {
		inv.renderTrees(ctx, &req, &cfg)
	}
	inv.elapsed = time.Since(start)
	result := inv.preparePayload(&req, payload)
	closeErr := owner.Close()
	inv.err, inv.remainder = errors.Join(inv.err, closeErr), errors.Join(inv.remainder, closeErr)
	return finishSelected(&req, &inv, &result)
}

func finishSelected(req *selectedRequest, inv *selectedInvocation, payload *selectedPayload) error {
	final := errors.Join(payload.err, inv.err)
	diagnostic := errors.Join(payload.err, inv.remainder)
	report := output.Report{
		Groups:  make([]output.ReportGroup, len(inv.groups)),
		Test:    payload.rosters != nil,
		Elapsed: inv.elapsed,
	}
	if req.notice.Category != "" {
		report.Warnings = append(report.Warnings, output.Warning{Warning: req.notice, Group: -1})
	}
	named := len(req.roots) > 0 && req.roots[0].Name != ""
	groupErrors, remainders := appendSelectedGroups(&report, req, inv, payload)
	final, diagnostic = errors.Join(final, groupErrors), errors.Join(diagnostic, remainders)

	names := report.Prepare(payload.subjects)
	if payload.prepare != nil {
		payload.prepare(names)
	}
	if payload.write != nil {
		err := payload.write()
		final, diagnostic = errors.Join(final, err), errors.Join(diagnostic, err)
	}
	if report.Test && testReportsFailed(payload.rosters) &&
		(!named || inv.err == nil && !slices.ContainsFunc(inv.groups, func(g selectedGroup) bool { return g.err != nil })) {
		final = errors.Join(errors.New("test failures detected"), final)
	}
	encoder := output.Encoder{Writer: req.stderr, Color: output.ColorEnabled(req.stderr)}
	out := output.Encoder{Writer: req.stdout, Color: output.ColorEnabled(req.stdout)}
	report.Error = diagnostic
	writeErr := encoder.Report(&report, out)
	final = errors.Join(final, writeErr)
	if err := encoder.Errors(writeErr); err != nil {
		return errors.Join(final, err)
	}
	if final != nil {
		presented := diagnostic != nil || writeErr != nil ||
			report.Test && testReportsFailed(payload.rosters)
		presented = presented || reportHasFailures(&report)
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
		for warningIndex := range group {
			warning := group[warningIndex]
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

const (
	kustomizationAlias = "kustomization"
	helmReleaseAlias   = "helmrelease"
	versionFlag        = "version"
	kindKey            = "kind"
	nameKey            = "name"
	chartKey           = "chart"
	sourceKey          = "source"
)

func validateSelectedPaths(req *selectedRequest) error {
	c := &req.flags
	if req.pair {
		return nil
	}
	if c.path == "" {
		return errors.New("path is required")
	}
	if err := validatePathFlag("--path", c.path); err != nil {
		return err
	}
	if c.pathOrig == "" {
		return nil
	}
	if err := validatePathFlag("--path-orig", c.pathOrig); err != nil {
		return err
	}

	return nil
}

func (i *selectedInvocation) failBaseline(c *commonFlags, pair bool, err error) {
	i.err, i.remainder = err, err
	group := selectedGroup{cluster: "."}
	for _, input := range []*store.SourceArtifact{c.baseInput, c.input} {
		if pair || len(group.sides) == 0 {
			group.sides = append(group.sides, &selection.Selection{})
		}
		if input != nil {
			side := group.sides[len(group.sides)-1]
			side.Warnings = append(side.Warnings, input.Warnings...)
		}
	}
	i.groups = append(i.groups, group)
}

func (i *selectedInvocation) collectRuns(req *selectedRequest, runs []orchestrator.Rendered) {
	for start := 0; start < len(runs); {
		end := start + 1
		for end < len(runs) && runs[end].ID.Cluster == runs[start].ID.Cluster {
			end++
		}
		i.addGroup(req, runs[start:end])
		start = end
	}
}

func (i *selectedInvocation) addGroup(req *selectedRequest, selected []orchestrator.Rendered) {
	group := selectedGroup{cluster: string(selected[0].ID.Cluster), complete: true}
	var groupErr error
	for _, run := range selected {
		groupErr = errors.Join(groupErr, run.Err)
	}
	if !req.pair {
		base := selected[0]
		selected = selected[len(selected)-1:]
		if selected[0].ID.Side != manifest.TreeHead || selected[0].Absent {
			i.err, i.remainder = errors.Join(i.err, groupErr), errors.Join(i.remainder, groupErr)
			return
		}
		if selected[0].Orchestrator == nil && base.ID.Side == manifest.TreeBase && base.Result != nil {
			result := orchestrator.Result{}
			if selected[0].Result != nil {
				result = *selected[0].Result
			}
			result.Warnings = append(slices.Clone(base.Result.Warnings), result.Warnings...)
			result.ClusterWarnings = append(
				slices.Clone(base.Result.ClusterWarnings),
				result.ClusterWarnings...)
			selected[0].Result = &result
		}
	}
	for _, run := range selected {
		group.absent[run.ID.Side] = run.Absent
		group.complete = group.complete && (run.Absent || run.Orchestrator != nil && run.Result != nil)
	}
	group.sides, group.err, group.remainder = orchestrator.Select(selected, groupErr, req.roots, req.labels)
	i.groups = append(i.groups, group)
}

func (i *selectedInvocation) preparePayload(
	req *selectedRequest,
	build func(*selectedInvocation) selectedPayload,
) selectedPayload {
	root := req.roots[0]
	count, complete, incomplete := 0, false, false
	for _, group := range i.groups {
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
		i.err = errors.Join(missing, i.err)
		i.remainder = errors.Join(missing, i.remainder)
	} else if complete {
		if root.Name != "" {
			i.groups = slices.DeleteFunc(i.groups, func(group selectedGroup) bool {
				return group.complete &&
					!slices.ContainsFunc(
						group.sides,
						func(side *selection.Selection) bool { return side.Counts[root.Kind] != 0 },
					)
			})
		}
		result = build(i)
	}
	return result
}

func selectedFailureModel(
	req *selectedRequest,
	group *selectedGroup,
	side *selection.Selection,
	roster *output.TestReport,
) output.Model {
	var model output.Model
	if !req.test || len(req.roots) > 0 && req.roots[0].Name != "" && group.err != nil {
		model = failureModel(side)
	}
	if roster != nil {
		model.Primary = slices.DeleteFunc(model.Primary, func(p output.Primary) bool {
			return slices.ContainsFunc(
				roster.Cases,
				func(c output.Case) bool { return c.ID == p.ID && c.Outcome == output.OutcomeFailed },
			)
		})
	}

	model.Subjects = side.Subjects
	return model
}

func appendSelectedGroups(
	report *output.Report,
	req *selectedRequest,
	inv *selectedInvocation,
	payload *selectedPayload,
) (error, error) {
	var groupErrors, remainders error
	for i, group := range inv.groups {
		g := &report.Groups[i]
		g.Cluster = group.cluster
		if i < len(payload.rosters) {
			g.Test = &payload.rosters[i]
		}
		groupErr, remainder := selectedGroupErrors(&group, len(inv.groups) > 1)
		groupErrors, remainders = errors.Join(groupErrors, groupErr), errors.Join(remainders, remainder)

		for j, side := range group.sides {
			model := selectedFailureModel(req, &group, side, g.Test)
			context := ""
			if req.pair {
				context = []string{"orig snapshot", "current snapshot"}[j]
			}
			model.Context = context
			g.Models = append(g.Models, model)
			appendSideDiagnostics(report, i, side, context)
		}
		normalizeReportModels(g)

		if i < len(payload.warnings) {
			for warningIndex := range payload.warnings[i] {
				warning := payload.warnings[i][warningIndex]
				report.Warnings = append(report.Warnings, output.Warning{Warning: warning, Group: i})
			}
		}
	}
	return groupErrors, remainders
}

func reportHasFailures(report *output.Report) bool {
	for _, group := range report.Groups {
		if slices.ContainsFunc(
			group.Models,
			func(m output.Model) bool { return len(m.Primary)+len(m.Missing) > 0 || m.Blocked > 0 },
		) {
			return true
		}
	}
	return false
}

func appendSideDiagnostics(report *output.Report, i int, side *selection.Selection, context string) {
	g := &report.Groups[i]
	if len(side.Subjects) != 0 {
		if g.Subjects == nil {
			g.Subjects = make(map[manifest.NamedResource]manifest.DiagnosticIdentity)
		}
		maps.Copy(g.Subjects, side.Subjects)
	}
	for warningIndex := range side.ClusterWarnings {
		warning := side.ClusterWarnings[warningIndex]
		report.Warnings = append(report.Warnings, output.Warning{Warning: warning, Group: i})
	}
	{
		warningValues := selectedWarnings(side)
		for warningIndex := range warningValues {
			warning := warningValues[warningIndex]
			report.Warnings = append(report.Warnings, output.Warning{Warning: warning, Group: i, Context: context})
		}
	}
}

func normalizeReportModels(g *output.ReportGroup) {
	if len(g.Subjects) != 0 {
		for j := range g.Models {
			if g.Models[j].Subjects == nil {
				g.Models[j].Subjects = map[manifest.NamedResource]manifest.DiagnosticIdentity{}
			}
		}
	}
}

func testReportsFailed(reports []output.TestReport) bool {
	for i := range reports {
		if reports[i].AnyFailed() {
			return true
		}
	}
	return false
}

func selectedGroupErrors(group *selectedGroup, multi bool) (error, error) {
	groupErr, remainder := group.err, group.remainder
	if multi {
		if groupErr != nil {
			groupErr = fmt.Errorf("cluster %s: %w", group.cluster, groupErr)
		}
		if remainder != nil {
			remainder = fmt.Errorf("cluster %s: %w", group.cluster, remainder)
		}
	}
	return groupErr, remainder
}

const (
	kustomizationAliases = "kustomizations"
	helmReleaseAliases   = "helmreleases"
	namespaceKey         = "namespace"
	pathKey              = "path"
)

func (i *selectedInvocation) renderTrees(ctx context.Context, req *selectedRequest, cfg *orchestrator.Config) {
	cfg.HeadOnly = !req.pair && req.flags.baseInput != nil
	cfg.Selection.Roots = slices.Clone(req.roots)
	if req.roots[0].Namespace == "" && cfg.Selection.Matches(manifest.NamedResource{}) {
		cfg.Selection.Roots = nil
	}
	var stop func()
	if barSink != nil {
		cfg.StatusListener, stop = barSink.progress(ctx)
	}
	runs, runErr := orchestrator.RenderTrees(ctx, inputTrees(&req.flags), *cfg)
	if stop != nil {
		stop()
	}
	if len(runs) == 0 {
		i.err, i.remainder = runErr, runErr
	}
	i.collectRuns(req, runs)
}
