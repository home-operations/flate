package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/output"
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
			return d.run(cmd, c, c.roots(firstArg(args), kind)[0])
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.DiffFormats...)
	bindDiffFlags(cmd, d)
	return cmd
}

func newDiffImagesCmd() *cobra.Command {
	c := &commonFlags{}
	var includeRemoved bool
	cmd := &cobra.Command{
		Use:   "images",
		Short: "Diff container images between current and baseline",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDiffImages(cmd, c, includeRemoved)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatName, output.FormatYAML, output.FormatJSON)
	cmd.Flags().BoolVar(&includeRemoved, "include-removed", false,
		"also emit images present only in --path-orig (default: only newly added images)")
	return cmd
}

func runDiffImages(cmd *cobra.Command, c *commonFlags, includeRemoved bool) error {
	stopProfile, err := startProfile()
	if err != nil {
		return err
	}
	defer stopProfile()
	orig, current, runErr := runDiffOrchestrators(cmd.Context(), c)
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
	return errors.Join((output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).Images(imgs), runErr)
}

func (d *diffFlags) run(cmd *cobra.Command, c *commonFlags, root manifest.NamedResource) error {
	stopProfile, err := startProfile()
	if err != nil {
		return err
	}
	defer stopProfile()
	orig, current, runErr := runDiffOrchestrators(cmd.Context(), c)
	kinds := []string{root.Kind}
	if root.Kind == "" {
		kinds = []string{manifest.KindKustomization, manifest.KindHelmRelease}
	}
	selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{orig, current}, runErr, c.roots(root.Name, kinds...), nil)
	if len(selected) == 0 {
		return runErr
	}
	color := false
	if len(selected[0].Unrelated) > 0 || len(selected[1].Unrelated) > 0 {
		color = output.ColorEnabled(cmd.ErrOrStderr())
	}
	for i, label := range []string{"orig", "current"} {
		res := selected[i]
		if len(res.Unrelated) > 0 {
			if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "%s snapshot:%s", label,
				output.Document(output.WarningsBlock(res.Unrelated, color))); err != nil {
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
// two completed runs to selection.
func runDiffOrchestrators(ctx context.Context, c *commonFlags) (orchestrator.Rendered, orchestrator.Rendered, error) {
	// Diff requires a baseline. Owner cleanup waits for both sides to join.
	if err := c.loadSubstitutions(ctx); err != nil {
		return orchestrator.Rendered{}, orchestrator.Rendered{}, err
	}
	cleanup, err := resolveBaseline(ctx, c, true)
	if err != nil {
		return orchestrator.Rendered{}, orchestrator.Rendered{}, err
	}
	// RenderTrees compares each scan entry against the other tree's source root.
	base, head, runErr := orchestrator.RenderTrees(ctx,
		orchestrator.Tree{RepoRoot: c.pathOrigRoot, Path: c.pathOrig, SelfURLs: c.pathOrigSelfURLs},
		orchestrator.Tree{RepoRoot: c.pathRoot, Path: c.path, SelfURLs: c.selfURLs},
		buildOrchCfg(*c))
	return base, head, errors.Join(runErr, cleanup())
}
