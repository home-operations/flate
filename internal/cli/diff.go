package cli

import (
	"errors"
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

func newDiffCmd() *cobra.Command {
	cmd := diffCmd("diff", nil, "Diff rendered output against a previous revision (defaults to all)", "")
	cmd.AddCommand(
		diffCmd("all", nil,
			"Diff all Flux producer output against another path", ""),
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
		groups := make([]output.ImageGroup, len(inv.groups))
		result := selectedPayload{warnings: make([][]manifest.Warning, len(groups))}
		for i, group := range inv.groups {
			orig, current := group.sides[0], group.sides[1]
			imgs, suppressed, err := diff.CompareImages(
				diff.Snapshot{Manifests: orig.Manifests, Unavailable: orig.Unavailable},
				diff.Snapshot{Manifests: current.Manifests, Unavailable: current.Unavailable},
				nil, includeRemoved,
			)
			groups[i] = output.ImageGroup{Cluster: group.cluster, Images: imgs, Added: group.absent[manifest.TreeBase], Removed: group.absent[manifest.TreeHead]}
			result.warnings[i], result.err = appendSuppressionWarnings(nil, suppressed), errors.Join(result.err, err)
		}
		result.err = errors.Join(result.err, (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).ImageGroups(groups))
		return result
	})
}

func (d *diffFlags) run(cmd *cobra.Command, c *commonFlags, root manifest.NamedResource) error {
	kinds := runnableKinds
	if root.Kind != "" {
		kinds = []string{root.Kind}
	}
	return runSelected(cmd.Context(), selectedRequest{
		flags: *c, roots: c.roots(root.Name, kinds...), pair: true,
		stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), presentation: presentDiff,
	}, func(inv *selectedInvocation) selectedPayload {
		groups := make([]output.DiffGroup, len(inv.groups))
		result := selectedPayload{warnings: make([][]manifest.Warning, len(groups))}
		for i, group := range inv.groups {
			orig, current := group.sides[0], group.sides[1]
			formatted, warnings, suppressed, err := diff.Compare(
				diff.Snapshot{Manifests: orig.Manifests, Unavailable: orig.Unavailable},
				diff.Snapshot{Manifests: current.Manifests, Unavailable: current.Unavailable},
				nil, diff.Options{StripAttrs: d.stripAttrs, StripFields: d.stripFields, Format: diff.Format(c.output)},
			)
			groups[i] = output.DiffGroup{Cluster: group.cluster, Added: group.absent[manifest.TreeBase], Removed: group.absent[manifest.TreeHead], Body: formatted}
			result.warnings[i], result.err = appendSuppressionWarnings(warnings, suppressed), errors.Join(result.err, err)
		}
		result.err = errors.Join(result.err, (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).DiffGroups(groups))
		return result
	})
}

func appendSuppressionWarnings(warnings []manifest.Warning, suppressed []diff.Suppression) []manifest.Warning {
	if len(suppressed) == 0 {
		return warnings
	}
	warnings = slices.Grow(warnings, len(suppressed))
	for _, note := range suppressed {
		warnings = append(warnings, manifest.Warning{
			Resource: note.Parent.NamedResource, Category: manifest.WarnDiffSuppressed,
			Message: note.String(),
		})
	}
	slices.SortFunc(warnings, manifest.CompareWarning)
	return warnings
}
