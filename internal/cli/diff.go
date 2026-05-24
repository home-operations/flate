package cli

import (
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
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

// defaultStripAttrs is the default `--strip-attr` list — annotations
// and labels that Helm + kustomize rotate on every chart bump and
// which contribute pure noise to PR-time diff review.
var defaultStripAttrs = []string{
	"helm.sh/chart",
	"checksum/config",
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
	if rendersHelm([]string{kind}) {
		bindHelmFlags(cmd.Flags(), h)
	}
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
	// diff images emits a flat list — only json/yaml are honored.
	if err := c.requireOutput(format.OutputYAML, format.OutputJSON); err != nil {
		return err
	}
	orig, current, runErr := runDiffOrchestrators(cmdContext(cmd), c, h)
	if orig.O == nil || current.O == nil {
		return runErr
	}
	imgs := imageSetDiff(collectImages(orig.O, orig.Res, c), collectImages(current.O, current.Res, c), includeRemoved)
	if err := emitImageList(cmd.OutOrStdout(), imgs, c.output); err != nil {
		return err
	}
	return runErr
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
	// diff has no `name` output mode; only diff/yaml/json/object are
	// meaningful. Reject early so the user sees a clear error instead
	// of "unknown diff format" from pkg/diff.
	if err := c.requireOutput(format.OutputYAML, format.OutputJSON); err != nil {
		return err
	}
	orig, current, runErr := runDiffOrchestrators(cmdContext(cmd), c, h)
	if orig.O == nil || current.O == nil {
		return runErr
	}
	origDocs := gatherArtifacts(orig.O, orig.Res, kind, name, c)
	currentDocs := gatherArtifacts(current.O, current.Res, kind, name, c)

	outFormat := c.output
	if outFormat == "table" {
		outFormat = "diff"
	}
	diffs, err := diff.Run(origDocs, currentDocs, diff.Options{
		Format:     diff.Format(outFormat),
		StripAttrs: d.stripAttrs,
	})
	if err != nil {
		return err
	}
	formatted, err := diff.Render(diffs, diff.Format(outFormat))
	if err != nil {
		return err
	}
	if _, err := cmd.OutOrStdout().Write(formatted); err != nil {
		return err
	}
	return runErr
}
