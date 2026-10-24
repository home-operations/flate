package cli

import (
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

type buildFlags struct {
	onlyKinds []string
}

func bindBuildFlags(fs *pflag.FlagSet, b *buildFlags) {
	fs.StringSliceVar(&b.onlyKinds, "only-kinds", nil, "emit only these kinds (empty includes all kinds)")
}

func newBuildCmd() *cobra.Command {
	cmd := buildCmd(&cobra.Command{
		Use:   "build",
		Short: "Render Flux objects to YAML (defaults to all)",
		Args:  cobra.NoArgs,
	}, renderKinds...)
	cmd.AddCommand(
		buildCmd(&cobra.Command{
			Use:     "ks [name]",
			Aliases: []string{"kustomization", "kustomizations"},
			Short:   "Render Kustomizations",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindKustomization),
		buildCmd(&cobra.Command{
			Use:     "hr [name]",
			Aliases: []string{"helmrelease", "helmreleases"},
			Short:   "Render HelmReleases",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindHelmRelease),
		buildCmd(&cobra.Command{
			Use:   "all",
			Short: "Render all Kustomization and HelmRelease objects",
			Args:  cobra.NoArgs,
		}, renderKinds...),
	)
	return cmd
}

func buildCmd(cmd *cobra.Command, kinds ...string) *cobra.Command {
	c := &commonFlags{}
	b := &buildFlags{}
	cmd.RunE = func(cmd *cobra.Command, argv []string) error {
		if !cmd.Flags().Changed("skip-kinds") {
			c.skipKinds = slices.DeleteFunc(c.skipKinds, func(kind string) bool {
				return slices.Contains(b.onlyKinds, kind)
			})
		}
		return runSelected(cmd.Context(), selectedRequest{
			flags: *c, roots: c.roots(firstArg(argv), kinds...),
			stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), presentation: presentBuild,
		}, func(inv *selectedInvocation) selectedPayload {
			return selectedPayload{err: (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).Documents(collectRendered(inv.sides[0], b))}
		})
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatYAML, output.FormatJSON)
	bindBuildFlags(cmd.Flags(), b)
	return cmd
}

func collectRendered(res *selection.Selection, b *buildFlags) []map[string]any {
	out := []map[string]any{}
	for _, batch := range res.Manifests {
		out = append(out, emissionDocs(batch.Documents, b)...)
	}
	return out
}

// emissionDocs sorts a cloned batch without changing the selected snapshot.
func emissionDocs(mans []map[string]any, b *buildFlags) []map[string]any {
	docs := slices.Clone(mans)
	slices.SortStableFunc(docs, compareDocs)
	if len(b.onlyKinds) > 0 {
		return slices.DeleteFunc(docs, func(doc map[string]any) bool {
			return !slices.Contains(b.onlyKinds, manifest.DocKind(doc))
		})
	}
	return docs
}

func compareDocs(a, b map[string]any) int {
	return manifest.DocIdentity(a).Compare(manifest.DocIdentity(b))
}
