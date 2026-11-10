package cli

import (
	"errors"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

func newDiffCmd() *cobra.Command {
	cmd := diffCmd("diff", nil, "Diff rendered output (defaults to all)", "")
	cmd.AddCommand(
		diffCmd("all", nil,
			"Diff all Flux producer output against a baseline", ""),
		diffCmd("ks [name]", []string{kustomizationAlias, kustomizationAliases},
			"Diff Kustomizations against another path", manifest.KindKustomization),
		diffCmd("hr [name]", []string{helmReleaseAlias, helmReleaseAliases},
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
	cmd.Flags().
		StringArrayVar(&d.stripAttrs, "strip-attr", diff.DefaultStripAttrs, "Ignore a metadata key in diffs (repeatable flag)")
	cmd.Flags().
		StringArrayVar(&d.stripFields, "strip-field", diff.DefaultStripFields, "Ignore a dotted field in diffs (repeatable flag)")
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
		Args:    positional,
		RunE: func(cmd *cobra.Command, args []string) error {
			return d.run(cmd, c, c.roots(firstArg(args), kind)[0])
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, diff.Formats()...)
	bindDiffFlags(cmd, d)
	return cmd
}

func newDiffImagesCmd() *cobra.Command {
	c := &commonFlags{}
	var includeRemoved bool
	cmd := &cobra.Command{
		Use:   "images",
		Short: "Diff container images against a baseline",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDiffImages(cmd, c, includeRemoved)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatName, output.FormatYAML, output.FormatJSON)
	cmd.Flags().BoolVar(&includeRemoved, "include-removed", false, "Include images found only in the baseline")
	return cmd
}

func runDiffImages(cmd *cobra.Command, c *commonFlags, includeRemoved bool) error {
	return runSelected(cmd.Context(), selectedRequest{
		flags: *c, roots: c.roots("", renderKinds...), pair: true,
		stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), stdin: cmd.InOrStdin(),
	}, func(inv *selectedInvocation) selectedPayload {
		groups := make([]output.ImageGroup, len(inv.groups))
		result := selectedPayload{warnings: make([][]manifest.Warning, len(groups))}
		for i, group := range inv.groups {
			orig, current := group.sides[0], group.sides[1]
			imgs, suppressed, err := diff.CompareImages(
				diff.Snapshot{
					Subjects:     orig.Subjects,
					Manifests:    orig.Manifests,
					Unavailable:  orig.Unavailable,
					ProducerKeys: orig.ProducerKeys,
				},
				diff.Snapshot{
					Subjects:     current.Subjects,
					Manifests:    current.Manifests,
					Unavailable:  current.Unavailable,
					ProducerKeys: current.ProducerKeys,
				},
				nil,
				includeRemoved,
			)
			groups[i] = output.ImageGroup{
				Cluster: group.cluster,
				Images:  imgs,
				Added:   group.absent[manifest.TreeBase],
				Removed: group.absent[manifest.TreeHead],
			}
			result.warnings[i], result.err = diff.MergeWarnings(nil, suppressed), errors.Join(result.err, err)
		}
		result.prepare = func(names []diff.Names) {
			for i := range groups {
				for _, image := range groups[i].Images {
					groups[i].Names = append(groups[i].Names, names[i].Prefix+image)
				}
			}
		}
		result.write = func() error {
			return (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).ImageGroups(groups)
		}
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
		stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), stdin: cmd.InOrStdin(),
	}, func(inv *selectedInvocation) selectedPayload {
		return d.payload(inv, output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)})
	})
}

func (d *diffFlags) payload(inv *selectedInvocation, encoder output.Encoder) selectedPayload {
	groups := make([]diff.Group, len(inv.groups))
	for i, group := range inv.groups {
		orig, current := group.sides[0], group.sides[1]
		groups[i] = diff.Group{
			Cluster: group.cluster,
			Orig: diff.Snapshot{
				Subjects:     orig.Subjects,
				Manifests:    orig.Manifests,
				Unavailable:  orig.Unavailable,
				ProducerKeys: orig.ProducerKeys,
			},
			Current: diff.Snapshot{
				Subjects:     current.Subjects,
				Manifests:    current.Manifests,
				Unavailable:  current.Unavailable,
				ProducerKeys: current.ProducerKeys,
			},
			Added:   group.absent[manifest.TreeBase],
			Removed: group.absent[manifest.TreeHead],
		}
	}
	comparison, err := diff.Compare(
		groups,
		diff.Options{Format: diff.Format(encoder.Format), StripAttrs: d.stripAttrs, StripFields: d.stripFields},
	)
	result := selectedPayload{
		err:      err,
		warnings: make([][]manifest.Warning, len(groups)),
		subjects: make([][]manifest.NamedResource, len(groups)),
	}
	for i, group := range comparison.Groups {
		result.warnings[i] = diff.MergeWarnings(group.Warnings, group.Suppressed)
		result.subjects[i] = group.Subjects
	}
	var preparedNames []diff.Names
	result.prepare = func(names []diff.Names) { preparedNames = names }
	result.write = func() error { return comparison.Write(encoder.Writer, preparedNames) }
	return result
}
