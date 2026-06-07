package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
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

type diffFlags struct {
	stripAttrs  []string
	stripFields []string
}

func bindDiffFlags(cmd *cobra.Command, d *diffFlags) {
	cmd.Flags().StringArrayVar(&d.stripAttrs, "strip-attr", diff.DefaultStripAttrs,
		"metadata annotation/label key to strip before diffing (repeatable; supplying any value replaces the default set)")
	cmd.Flags().StringArrayVar(&d.stripFields, "strip-field", diff.DefaultStripFields,
		"dotted spec field-path to delete before diffing, e.g. spec.restic.unlock (repeatable; supplying any value replaces the default set)")
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
	bindCommon(cmd.Flags(), c, format.OutputName, format.OutputYAML, format.OutputJSON)
	bindHelmFlags(cmd.Flags(), h)
	cmd.Flags().BoolVar(&includeRemoved, "include-removed", false,
		"also emit images present only in --path-orig (default: only newly added images)")
	return cmd
}

func runDiffImages(cmd *cobra.Command, c *commonFlags, h *helmFlags, includeRemoved bool) error {
	// diff images emits a flat list of image refs — name (one per line) is
	// the default; json/yaml are the structured shapes. The -o flag rejects
	// anything else at parse time.
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
	if err := emitImageList(cmd.OutOrStdout(), imgs, c.output); err != nil {
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
// in help-display order: human (the default, listed first), then github,
// the plain unified diff and its self-contained HTML rendering, then the
// remaining dyff styles. bindCommon
// registers the set on the -o flag, which drives both the help text and
// the parse-time rejection, so the advertised and enforced sets can't
// drift.
func diffOutputFormats() []format.Output {
	return []format.Output{
		format.Output(diff.FormatHuman),
		format.Output(diff.FormatGitHub),
		format.Output(diff.FormatDiff),
		format.Output(diff.FormatHTML),
		format.Output(diff.FormatBrief),
		format.Output(diff.FormatGitLab),
		format.Output(diff.FormatGitea),
	}
}

func runDiff(cmd *cobra.Command, c *commonFlags, h *helmFlags, d *diffFlags, kind, name string) error {
	// The -o flag (an outputValue enum) already rejects unsupported values
	// at parse time. The format.Output string values match diff.Format so
	// the cast below routes each one to its renderer without an extra switch.
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

	out := diff.Format(c.output)
	formatted, err := diff.RenderDocs(origDocs, currentDocs, diff.Options{
		StripAttrs:  d.stripAttrs,
		StripFields: d.stripFields,
		Format:      out,
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

// runDiffOrchestrators resolves the baseline, then hands the
// two-orchestrator render to orchestrator.RenderTrees (swapped PathOrig
// so each side renders changed-only against the other, one shared source
// cache, concurrent) and maps its two sides into the diffSide pair the
// diff/diff-images commands consume. base = --path-orig (left/old),
// head = --path (right/new).
func runDiffOrchestrators(ctx context.Context, c *commonFlags, h *helmFlags) (diffSide, diffSide, error) {
	// diff REQUIRES a baseline — when neither --path-orig nor --base
	// is set, auto-detect via the merge-base ladder. Cleanup is
	// deferred (not bound to ctx) so the tempdir survives SIGINT
	// until both orchestrators' read paths have actually unwound.
	cleanup, err := resolveBaseline(c, true)
	if err != nil {
		return diffSide{}, diffSide{}, err
	}
	defer cleanup()
	// Each side is a Tree: its scan entry point (Path) plus the source
	// root (RepoRoot) that its spec.path values resolve against. The CLI
	// resolves each root via the .git default (repoRootOf); RenderTrees
	// reconciles each side changed-only against the other tree's root.
	base, head, runErr := orchestrator.RenderTrees(ctx,
		orchestrator.Tree{RepoRoot: c.baselineRoot(), Path: c.pathOrig, SelfURLs: c.pathOrigSelfURLs},
		orchestrator.Tree{RepoRoot: repoRootOf(c.path), Path: c.path},
		buildOrchCfg(*c, *h))
	orig := diffSide{O: base.Orchestrator, Res: base.Result, Err: base.Err}
	current := diffSide{O: head.Orchestrator, Res: head.Result, Err: head.Err}
	return orig, current, runErr
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
	// Defensive re-drop of --skip-secrets / --skip-crds / --skip-kinds.
	// Orchestrator.Render already applies the same set to Result.Manifests
	// at the embed boundary; this still pulls weight for SDK consumers who
	// hand-build a Result and pass it through the CLI helpers in tests.
	var skip []string
	if c != nil {
		skip = c.skipResourceKinds()
	}
	// Filter the rendered manifests down to the in-scope parents
	// (name + namespace), then hand the submap to diff.DocsFromManifests
	// — the shared, store-free flattener SDK consumers use too — passing a
	// store-backed pathOf so KS parents carry their spec.path.
	sub := make(map[manifest.NamedResource][]map[string]any)
	matched := 0
	for _, obj := range o.Store().ListObjects(kind) {
		id := obj.Named()
		if name != "" && id.Name != name {
			continue
		}
		if c != nil && !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		matched++
		if docs, ok := res.Manifests[id]; ok {
			sub[id] = manifest.DropKinds(docs, skip)
		}
	}
	docs := diff.DocsFromManifests(sub, func(id manifest.NamedResource) string {
		if ks, ok := store.Get[*manifest.Kustomization](o.Store(), id); ok {
			return strings.TrimPrefix(ks.Path, "./")
		}
		return ""
	})
	return docs, matched
}
