package cli

import (
	"context"
	"errors"

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
	return runSelected(cmd.Context(), selectedRequest{
		flags: *c, roots: c.roots("", renderKinds...), pair: true,
		stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), presentation: presentData,
	}, func(inv *selectedInvocation) selectedPayload {
		orig, current := inv.sides[0], inv.sides[1]
		imgs, suppressed, err := diff.CompareImages(
			diff.Snapshot{Objects: orig.Objects, Manifests: orig.Manifests, Unavailable: orig.Unavailable},
			diff.Snapshot{Objects: current.Objects, Manifests: current.Manifests, Unavailable: current.Unavailable},
			nil, includeRemoved,
		)
		var warnings []manifest.Warning
		for _, note := range suppressed {
			warnings = append(warnings, manifest.Warning{Message: note.String()})
		}
		return selectedPayload{warnings: warnings, err: errors.Join(err,
			(output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).Images(imgs))}
	})
}

func (d *diffFlags) run(cmd *cobra.Command, c *commonFlags, root manifest.NamedResource) error {
	kinds := renderKinds
	if root.Kind != "" {
		kinds = []string{root.Kind}
	}
	return runSelected(cmd.Context(), selectedRequest{
		flags: *c, roots: c.roots(root.Name, kinds...), pair: true,
		stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), presentation: presentDiff,
	}, func(inv *selectedInvocation) selectedPayload {
		orig, current := inv.sides[0], inv.sides[1]
		formatted, err := diff.Compare(
			diff.Snapshot{Objects: orig.Objects, Manifests: orig.Manifests, Unavailable: orig.Unavailable},
			diff.Snapshot{Objects: current.Objects, Manifests: current.Manifests, Unavailable: current.Unavailable},
			nil, diff.Options{StripAttrs: d.stripAttrs, StripFields: d.stripFields, Format: diff.Format(c.output)},
		)
		_, writeErr := cmd.OutOrStdout().Write(formatted)
		return selectedPayload{err: errors.Join(err, writeErr)}
	})
}

func runDiffOrchestrators(ctx context.Context, c *commonFlags) (orchestrator.Rendered, orchestrator.Rendered, error) {
	// Diff requires a baseline. Owner cleanup waits for both sides to join.
	if err := c.loadSubstitutions(ctx); err != nil {
		return orchestrator.Rendered{}, orchestrator.Rendered{}, err
	}
	cleanup, err := resolveBaseline(ctx, c, true)
	if err != nil {
		return orchestrator.Rendered{}, orchestrator.Rendered{}, err
	}
	base, head, runErr := orchestrator.RenderTrees(ctx,
		orchestrator.Tree{RepoRoot: c.pathOrigRoot, Path: c.pathOrig, SelfURLs: c.pathOrigSelfURLs},
		orchestrator.Tree{RepoRoot: c.pathRoot, Path: c.path, SelfURLs: c.selfURLs},
		buildOrchCfg(*c))
	return base, head, errors.Join(runErr, cleanup())
}
