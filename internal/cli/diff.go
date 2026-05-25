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
	bindCommon(cmd.Flags(), c)
	// `diff all` renders HRs as part of its scope, so always bind
	// helm flags (matches `build all` / `test all` / `get all`).
	if kind == "" || kind == manifest.KindHelmRelease {
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
	orig, current, runErr := runDiffOrchestrators(cmdContext(cmd), c, h)
	if orig.O == nil || current.O == nil {
		return runErr
	}
	imgs := imageSetDiff(collectImages(orig.O, orig.Res, c), collectImages(current.O, current.Res, c), includeRemoved)
	out := c.output
	if out == string(format.OutputTable) {
		out = string(format.OutputName)
	}
	if err := emitImageList(cmd.OutOrStdout(), imgs, out); err != nil {
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
	origDocs := gatherAllArtifacts(orig.O, orig.Res, kind, name, c)
	currentDocs := gatherAllArtifacts(current.O, current.Res, kind, name, c)

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
