package cli

import (
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff rendered output against a previous revision",
	}
	cmd.AddCommand(newDiffKSCmd(), newDiffHRCmd(), newDiffImagesCmd())
	return cmd
}

type diffFlags struct {
	unified    int
	stripAttrs []string
	limitBytes int
}

func bindDiffFlags(cmd *cobra.Command, d *diffFlags) {
	cmd.Flags().IntVarP(&d.unified, "unified", "u", 3, "unified diff context lines")
	cmd.Flags().StringSliceVar(&d.stripAttrs, "strip-attrs", nil, "metadata annotation/label keys to strip before diffing")
	cmd.Flags().IntVar(&d.limitBytes, "limit-bytes", 0, "truncate per-resource diffs (0 = unlimited)")
}

func newDiffKSCmd() *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	d := &diffFlags{}
	cmd := &cobra.Command{
		Use:     "ks [name]",
		Aliases: []string{"kustomization", "kustomizations"},
		Short:   "Diff Kustomizations against another path",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiff(cmd, c, h, d, manifest.KindKustomization, firstArg(args))
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	bindDiffFlags(cmd, d)
	return cmd
}

func newDiffHRCmd() *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	d := &diffFlags{}
	cmd := &cobra.Command{
		Use:     "hr [name]",
		Aliases: []string{"helmrelease", "helmreleases"},
		Short:   "Diff HelmReleases against another path",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiff(cmd, c, h, d, manifest.KindHelmRelease, firstArg(args))
		},
	}
	bindCommon(cmd.Flags(), c)
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
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	cmd.Flags().BoolVar(&includeRemoved, "include-removed", false,
		"also emit images present only in --path-orig (default: only newly added images)")
	return cmd
}

func runDiffImages(cmd *cobra.Command, c *commonFlags, h *helmFlags, includeRemoved bool) error {
	orig, current, err := runDiffOrchestrators(cmdContext(cmd), c, h)
	if err != nil {
		return err
	}
	imgs := imageSetDiff(collectImages(orig, c), collectImages(current, c), includeRemoved)

	w, closeFn, err := c.resolveWriter(cmd.OutOrStdout())
	if err != nil {
		return err
	}
	defer func() { _ = closeFn() }()
	return emitImageList(w, imgs, c.output)
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

func runDiff(cmd *cobra.Command, c *commonFlags, h *helmFlags, d *diffFlags, kind, name string) error {
	orig, current, err := runDiffOrchestrators(cmdContext(cmd), c, h)
	if err != nil {
		return err
	}
	origDocs := gatherArtifacts(orig, kind, name, c)
	currentDocs := gatherArtifacts(current, kind, name, c)

	outFormat := c.output
	if outFormat == "table" {
		outFormat = "diff"
	}
	diffs, err := diff.Run(origDocs, currentDocs, diff.Options{
		Format:     diff.Format(outFormat),
		Context:    d.unified,
		LimitBytes: d.limitBytes,
		StripAttrs: d.stripAttrs,
	})
	if err != nil {
		return err
	}
	formatted, err := diff.Render(diffs, diff.Format(outFormat))
	if err != nil {
		return err
	}
	w, closeFn, err := c.resolveWriter(cmd.OutOrStdout())
	if err != nil {
		return err
	}
	defer func() { _ = closeFn() }()
	_, err = w.Write(formatted)
	return err
}
