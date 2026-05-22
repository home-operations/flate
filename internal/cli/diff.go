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
	cmd.AddCommand(
		diffCmd("ks [name]", []string{"kustomization", "kustomizations"},
			"Diff Kustomizations against another path", manifest.KindKustomization),
		diffCmd("hr [name]", []string{"helmrelease", "helmreleases"},
			"Diff HelmReleases against another path", manifest.KindHelmRelease),
		newDiffImagesCmd(),
	)
	return cmd
}

type diffFlags struct {
	unified    int
	stripAttrs []string
	limitBytes int
}

var defaultStripAttrs = []string{
	"helm.sh/chart",
	"checksum/config",
	"app.kubernetes.io/version",
	"chart",
}

func bindDiffFlags(cmd *cobra.Command, d *diffFlags) {
	cmd.Flags().IntVarP(&d.unified, "unified", "u", 6, "unified diff context lines")
	cmd.Flags().StringArrayVar(&d.stripAttrs, "strip-attr", defaultStripAttrs, "metadata annotation/label key to strip before diffing (repeatable; supplying any value replaces the default set)")
	cmd.Flags().IntVar(&d.limitBytes, "limit-bytes", 65536, "truncate per-resource diffs (0 = unlimited; default matches GitHub issue body limit)")
}

func diffCmd(use string, aliases []string, short, kind string) *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	d := &diffFlags{}
	cmd := &cobra.Command{
		Use:     use,
		Aliases: aliases,
		Short:   short,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiff(cmd, c, h, d, kind, firstArg(args))
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
	return emitImageList(cmd.OutOrStdout(), imgs, c.output)
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
	_, err = cmd.OutOrStdout().Write(formatted)
	return err
}
