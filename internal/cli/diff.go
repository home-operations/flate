package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/internal/report"
	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

func newDiffCmd() *cobra.Command {
	cmd := diffCmd("diff", nil, "Diff rendered output against a previous revision (defaults to all)", "")
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
	positional := cobra.MaximumNArgs(1)
	if kind == "" {
		positional = cobra.NoArgs
	}
	cmd := &cobra.Command{
		Use:     use,
		Aliases: aliases,
		Short:   short,
		Long:    short + ". With -o json, emit a 2-space-indented JSON object {\"changes\": [...], \"suppressed\": [...]} with normalized manifests and unavailable producer disclosures.",
		Args:    positional,
		RunE: func(cmd *cobra.Command, args []string) error {
			return d.run(cmd, c, h, manifest.NamedResource{Kind: kind, Name: firstArg(args)})
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
	stopProfile, err := startProfile(c.profileMode, c.profileOut)
	if err != nil {
		return err
	}
	defer stopProfile()
	orig, current, runErr := runDiffOrchestrators(cmdContext(cmd), c, h)
	selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{orig, current}, runErr,
		c.roots("", manifest.KindKustomization, manifest.KindHelmRelease), nil)
	if len(selected) == 0 {
		return runErr
	}
	imgs, suppressed, runErr := diff.CompareImages(
		diff.Snapshot{Objects: selected[0].Objects, Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable},
		diff.Snapshot{Objects: selected[1].Objects, Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable},
		runErr,
		includeRemoved,
	)
	for _, note := range suppressed {
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "flate warning: %s\n", note); err != nil {
			return errors.Join(err, runErr)
		}
	}
	return errors.Join(emitImageList(cmd.OutOrStdout(), imgs, c.output), runErr)
}

// diffOutputFormats lists the -o values `flate diff ks/hr/all` accepts,
// in help-display order, with the default first. bindCommon uses this set
// for both help and parse-time validation.
func diffOutputFormats() []format.Output {
	return []format.Output{
		format.Output(diff.FormatHuman),
		format.Output(diff.FormatGitHub),
		format.Output(diff.FormatDiff),
		format.Output(diff.FormatHTML),
		format.Output(diff.FormatBrief),
		format.Output(diff.FormatGitLab),
		format.Output(diff.FormatGitea),
		format.Output(diff.FormatJSON),
	}
}

func (d *diffFlags) run(cmd *cobra.Command, c *commonFlags, h *helmFlags, root manifest.NamedResource) error {
	stopProfile, err := startProfile(c.profileMode, c.profileOut)
	if err != nil {
		return err
	}
	defer stopProfile()
	orig, current, runErr := runDiffOrchestrators(cmdContext(cmd), c, h)
	kinds := []string{root.Kind}
	if root.Kind == "" {
		kinds = []string{manifest.KindKustomization, manifest.KindHelmRelease}
	}
	selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{orig, current}, runErr, c.roots(root.Name, kinds...), nil)
	if len(selected) == 0 {
		return runErr
	}
	for i, label := range []string{"orig", "current"} {
		res := selected[i]
		if len(res.Unrelated) > 0 {
			if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "%s snapshot:%s", label,
				report.Document(report.WarningsBlock(res.Unrelated, style.ColorEnabled(cmd.ErrOrStderr())))); err != nil {
				return errors.Join(err, runErr)
			}
		}
		if err := reportOmittedValues(cmd.ErrOrStderr(), res.Warnings, label); err != nil {
			return errors.Join(err, runErr)
		}
	}
	for i, label := range []string{"orig", "current"} {
		for _, warning := range selected[i].External {
			if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "flate warning: %s snapshot: %s skipped: %s\n", label, warning.Resource, warning.Message); err != nil {
				return errors.Join(err, runErr)
			}
		}
	}
	if root.Name != "" && selected[0].Counts[root.Kind]+selected[1].Counts[root.Kind] == 0 {
		return errors.Join(fmt.Errorf("no %s named %q in --path or --path-orig", root.Kind, root.Name), runErr)
	}
	formatted, err := diff.Compare(
		diff.Snapshot{Objects: selected[0].Objects, Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable},
		diff.Snapshot{Objects: selected[1].Objects, Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable},
		runErr,
		diff.Options{StripAttrs: d.stripAttrs, StripFields: d.stripFields, Format: diff.Format(c.output)},
	)
	if err != nil && formatted == nil && runErr == nil {
		return err
	}
	runErr = err
	if runErr != nil {
		if _, err := io.WriteString(cmd.ErrOrStderr(), "flate error: "+runErr.Error()+"\n"); err != nil && root.Name != "" {
			return errors.Join(err, runErr)
		}
	}
	if _, err := cmd.OutOrStdout().Write(formatted); err != nil {
		return errors.Join(err, runErr)
	}
	if runErr != nil {
		return reportedError{runErr}
	}
	return nil
}

func reportOmittedValues(w io.Writer, warnings []manifest.Warning, label string) error {
	for _, warning := range warnings {
		if warning.Category != manifest.WarnOmittedValues {
			continue
		}
		if _, err := fmt.Fprintf(w, "flate warning: %s snapshot: %s: %s\n", label, warning.Resource, warning.Message); err != nil {
			return err
		}
	}
	return nil
}

// runDiffOrchestrators resolves the baseline, then hands the
// two completed runs to selection. Baseline lifetime ends after both renders drain.
func runDiffOrchestrators(ctx context.Context, c *commonFlags, h *helmFlags) (orchestrator.Rendered, orchestrator.Rendered, error) {
	// diff REQUIRES a baseline — when neither --path-orig nor --base
	// is set, auto-detect via the merge-base ladder. Cleanup is
	// deferred (not bound to ctx) so the tempdir survives SIGINT
	// until both orchestrators' read paths have actually unwound.
	if err := c.loadSubstitutions(ctx); err != nil {
		return orchestrator.Rendered{}, orchestrator.Rendered{}, err
	}
	cleanup, err := resolveBaseline(ctx, c, true)
	if err != nil {
		return orchestrator.Rendered{}, orchestrator.Rendered{}, err
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
	return base, head, runErr
}
