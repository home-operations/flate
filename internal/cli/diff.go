package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff rendered output against a previous revision",
	}
	cmd.AddCommand(
		diffCmd("all", nil,
			"Diff every Kustomization and HelmRelease against another path", ""),
		diffCmd("ks [name]", []string{"kustomization", "kustomizations"},
			"Diff Kustomizations against another path", manifest.KindKustomization),
		diffCmd("hr [name]", []string{"helmrelease", "helmreleases"},
			"Diff HelmReleases against another path", manifest.KindHelmRelease),
		newDiffImagesCmd(),
	)
	return cmd
}

// defaultStripAttrs is the default `--strip-attr` list — annotations
// and labels that Helm + kustomize rotate on every chart bump and
// which contribute pure noise to PR-time diff review. checksum/*
// annotations are templated as `sha256sum (include "secret.yaml")`
// in many charts; the rendered Secret values flate sees are
// wiped-to-PLACEHOLDER but the chart's helper pipeline still emits
// non-stable bytes across runs (e.g. random suffix from sprig
// randAlphaNum), producing churn-only diffs.
var defaultStripAttrs = []string{
	"helm.sh/chart",
	"checksum/config",
	"checksum/secret",
	"app.kubernetes.io/version",
	"chart",
}

type diffFlags struct {
	stripAttrs []string
}

func bindDiffFlags(cmd *cobra.Command, d *diffFlags) {
	cmd.Flags().StringArrayVar(&d.stripAttrs, "strip-attr", defaultStripAttrs,
		"metadata annotation/label key to strip before diffing (repeatable; supplying any value replaces the default set)")
}

func diffCmd(use string, aliases []string, short, kind string) *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	d := &diffFlags{}
	// `diff all` has no name filter — it's the catch-all combined
	// view, accepts no positional args. Per-kind variants accept an
	// optional single name positional to scope the diff.
	maxArgs := 1
	if kind == "" {
		maxArgs = 0
	}
	cmd := &cobra.Command{
		Use:     use,
		Aliases: aliases,
		Short:   short,
		Args:    cobra.MaximumNArgs(maxArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiff(cmd, c, h, d, kind, firstArg(args))
		},
	}
	bindCommon(cmd.Flags(), c, diffOutputFormats()...)
	bindHelmFlags(cmd.Flags(), h)
	bindDiffFlags(cmd, d)
	return cmd
}

func newDiffImagesCmd() *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	var includeRemoved bool
	cmd := &cobra.Command{
		Use:   "images",
		Short: "Diff container images between current and baseline",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDiffImages(cmd, c, h, includeRemoved)
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputYAML, format.OutputJSON, format.OutputName)
	bindHelmFlags(cmd.Flags(), h)
	cmd.Flags().BoolVar(&includeRemoved, "include-removed", false,
		"also emit images present only in --path-orig (default: only newly added images)")
	return cmd
}

func runDiffImages(cmd *cobra.Command, c *commonFlags, h *helmFlags, includeRemoved bool) error {
	// diff images emits a flat list of image refs — json/yaml/name
	// are the meaningful shapes. The CLI default `-o table` would
	// otherwise leak through `requireOutput`'s table-passthrough
	// (common.go:requireOutput) and fall into emitImageList's
	// newline-per-image branch, producing identical output to
	// `-o name` with no signposting that the format was effectively
	// coerced. Allow `-o name` explicitly, then route `table` →
	// `name` so the default and the explicit form match.
	if err := c.requireOutput(format.OutputYAML, format.OutputJSON, format.OutputName); err != nil {
		return err
	}
	stopProfile, err := startProfile(c.profileMode, c.profileOut)
	if err != nil {
		return err
	}
	defer stopProfile()
	orig, current, runErr := runDiffOrchestrators(cmdContext(cmd), c, h)
	if orig.O == nil || current.O == nil {
		return runErr
	}
	imgs := imageSetDiff(collectImages(orig.O, orig.Res, c), collectImages(current.O, current.Res, c), includeRemoved)
	if err := emitImageList(cmd.OutOrStdout(), imgs, string(c.outputOrDefault(format.OutputName))); err != nil {
		return errors.Join(err, scopedDiffRunError(orig, current, c, runErr))
	}
	return scopedDiffRunError(orig, current, c, runErr)
}

// imageSetDiff returns the sorted images added in current; when
// includeRemoved is set, images dropped from orig are included too.
func imageSetDiff(orig, current map[string]struct{}, includeRemoved bool) []string {
	out := make([]string, 0, len(current))
	for img := range current {
		if _, ok := orig[img]; !ok {
			out = append(out, img)
		}
	}
	if includeRemoved {
		for img := range orig {
			if _, ok := current[img]; !ok {
				out = append(out, img)
			}
		}
	}
	slices.Sort(out)
	return out
}

// diffOutputFormats lists the -o values `flate diff ks/hr/all` accepts,
// in help-display order (github default first, then the plain unified
// diff, the other dyff styles, and the flate aggregations). Shared by
// the flag help (bindCommon) and the runtime guard (requireOutput) so
// the advertised set and the enforced set can't drift.
func diffOutputFormats() []format.Output {
	return []format.Output{
		format.Output(diff.FormatGitHub),
		format.Output(diff.FormatDiff),
		format.Output(diff.FormatHuman),
		format.Output(diff.FormatBrief),
		format.Output(diff.FormatGitLab),
		format.Output(diff.FormatGitea),
		format.OutputYAML,
		format.OutputJSON,
		format.OutputMarkdown,
	}
}

func runDiff(cmd *cobra.Command, c *commonFlags, h *helmFlags, d *diffFlags, kind, name string) error {
	// diff has no `name` output mode. Reject anything outside the
	// supported set early so the user sees a clear error instead of
	// "unknown diff format" from pkg/diff. The format.Output string
	// values match diff.Format so the casts below route each one to its
	// renderer without an extra switch.
	if err := c.requireOutput(diffOutputFormats()...); err != nil {
		return err
	}
	stopProfile, err := startProfile(c.profileMode, c.profileOut)
	if err != nil {
		return err
	}
	defer stopProfile()
	orig, current, runErr := runDiffOrchestrators(cmdContext(cmd), c, h)
	if orig.O == nil || current.O == nil {
		return runErr
	}
	origDocs, origMatched := gatherAllArtifacts(orig.O, orig.Res, kind, name, c)
	currentDocs, currentMatched := gatherAllArtifacts(current.O, current.Res, kind, name, c)
	diffRunErr := scopedDiffRunError(orig, current, c, runErr)
	if name != "" && origMatched+currentMatched == 0 {
		return errors.Join(fmt.Errorf("no %s named %q in --path or --path-orig", kind, name), diffRunErr)
	}

	out := diff.Format(c.outputOrDefault(format.Output(diff.FormatGitHub)))
	formatted, err := diff.RenderDocs(origDocs, currentDocs, diff.Options{
		StripAttrs: d.stripAttrs,
		Format:     out,
	})
	if err != nil {
		return errors.Join(err, diffRunErr)
	}
	if _, err := cmd.OutOrStdout().Write(formatted); err != nil {
		return errors.Join(err, diffRunErr)
	}
	return diffRunErr
}

// diffSide pairs an Orchestrator with its render Result. Diff
// commands need both — orchestrator for filter/object lookup, Result
// for the rendered docs feeding the diff.
type diffSide struct {
	O   *orchestrator.Orchestrator
	Res *orchestrator.Result
	Err error
}

// runDiffOrchestrators boots two orchestrators with each side's
// --path-orig pointing at the other, so both resolve the same symmetric
// change set and only render resources that differ between paths. Both
// sides are independent (separate task service, helm client, staging
// cache, store), so they run concurrently — wall time roughly halves
// on changed-only diffs.
func runDiffOrchestrators(ctx context.Context, c *commonFlags, h *helmFlags) (diffSide, diffSide, error) {
	// diff REQUIRES a baseline — when neither --path-orig nor --base
	// is set, auto-detect via the merge-base ladder. Cleanup is
	// deferred (not bound to ctx) so the tempdir survives SIGINT
	// until both orchestrators' read paths have actually unwound.
	cleanup, err := resolveBaseline(ctx, c, true)
	if err != nil {
		return diffSide{}, diffSide{}, err
	}
	defer cleanup()
	currentCfg := buildOrchCfg(*c, *h)
	origCfg := currentCfg
	origCfg.Path, origCfg.PathOrig = c.pathOrig, c.path

	// One source.Cache shared across both orchestrators. They write into
	// the same on-disk cache root; without a shared *Cache each side has
	// its own mutex, and concurrent first-time clones of the same
	// (url, ref) slot can race past the mkdir/Readdirnames check and
	// step on each other.
	cacheRoot := cmp.Or(currentCfg.CacheDir, filepath.Join(os.TempDir(), "flate-cache"))
	shared := source.NewCache(cacheroot.New(cacheRoot))
	currentCfg.SourceCache = shared
	origCfg.SourceCache = shared

	var (
		orig, current    diffSide
		origErr, currErr error
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		o, res, err := runOrchestratorCfg(gctx, currentCfg)
		current = diffSide{O: o, Res: res, Err: err}
		currErr = err
		// Fatal Bootstrap errors return nil orchestrator — propagate
		// those so the errgroup cancels its sibling. Per-resource Run
		// failures keep the orchestrator non-nil; defer them so we
		// still produce a diff before the caller flips the exit code.
		if o == nil {
			return err
		}
		return nil
	})
	g.Go(func() error {
		o, res, err := runOrchestratorCfg(gctx, origCfg)
		orig = diffSide{O: o, Res: res, Err: err}
		origErr = err
		if o == nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return diffSide{}, diffSide{}, err
	}
	// Either side may have reconcile failures — return the combined
	// run-error alongside the orchestrators so the diff caller can
	// write its output, then flip the exit code.
	return orig, current, joinRunErrors(origErr, currErr)
}

// joinRunErrors composes the orig/current Run errors into a single
// non-nil error when either side had failures. Both nil → nil.
func joinRunErrors(orig, curr error) error {
	switch {
	case orig != nil && curr != nil:
		return errors.Join(
			errors.New("both snapshots had reconcile failures"),
			fmt.Errorf("orig snapshot: %w", orig),
			fmt.Errorf("current snapshot: %w", curr),
		)
	case orig != nil:
		return fmt.Errorf("orig snapshot: %w", orig)
	case curr != nil:
		return fmt.Errorf("current snapshot: %w", curr)
	}
	return nil
}

func scopedDiffRunError(orig, current diffSide, c *commonFlags, runErr error) error {
	if runErr == nil {
		return nil
	}
	return joinRunErrors(
		scopedRunError(orig.O, orig.Res, c, orig.Err),
		scopedRunError(current.O, current.Res, c, current.Err),
	)
}

// gatherAllArtifacts is gatherArtifacts with a kind="" shortcut for
// the `diff all` command: when kind is empty, collect both
// Kustomization- and HelmRelease-rendered manifests in one pass.
// Each kind's docs are gathered separately so the diff header
// attribution (parent KS vs parent HR) stays accurate.
func gatherAllArtifacts(o *orchestrator.Orchestrator, res *orchestrator.Result, kind, name string, c *commonFlags) ([]diff.Doc, int) {
	if kind != "" {
		return gatherArtifacts(o, res, kind, name, c)
	}
	out, matched := gatherArtifacts(o, res, manifest.KindKustomization, name, c)
	hrs, hrMatched := gatherArtifacts(o, res, manifest.KindHelmRelease, name, c)
	return append(out, hrs...), matched + hrMatched
}

// gatherArtifacts collects every rendered manifest produced by the
// Kustomizations or HelmReleases of the given kind, tagged with the
// parent that produced them. name optionally filters to a single
// resource. When c is non-nil the namespace scope from commonFlags +
// the orchestrator's change filter is honored.
//
// Reads res.Manifests for the rendered docs; falls back to the Store
// only to recover the producing object's spec.path (the diff header
// shows it for KS parents).
func gatherArtifacts(o *orchestrator.Orchestrator, res *orchestrator.Result, kind, name string, c *commonFlags) ([]diff.Doc, int) {
	var out []diff.Doc
	// Defensive re-drop of --skip-secrets / --skip-crds / --skip-kinds.
	// Orchestrator.Render already applies the same set to Result.Manifests
	// at the embed boundary; this still pulls weight for SDK consumers who
	// hand-build a Result and pass it through the CLI helpers in tests.
	var skip []string
	if c != nil {
		skip = c.skipResourceKinds()
	}
	objs := o.Store().ListObjects(kind)
	slices.SortFunc(objs, func(a, b manifest.BaseManifest) int {
		return a.Named().Compare(b.Named())
	})
	matched := 0
	for _, obj := range objs {
		id := obj.Named()
		if name != "" && id.Name != name {
			continue
		}
		if c != nil && !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		matched++
		docs, ok := res.Manifests[id]
		if !ok {
			continue
		}
		parent := diff.Parent{Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
		if ks, ok := store.Get[*manifest.Kustomization](o.Store(), id); ok {
			parent.Path = strings.TrimPrefix(ks.Path, "./")
		}
		for _, m := range manifest.DropKinds(docs, skip) {
			out = append(out, diff.Doc{Manifest: m, Parent: parent})
		}
	}
	return out, matched
}
