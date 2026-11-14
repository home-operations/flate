package cli

import (
	"errors"
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

type buildFlags struct {
	onlyKinds []string
}

func bindBuildFlags(fs *pflag.FlagSet, b *buildFlags) {
	fs.StringSliceVar(&b.onlyKinds, "only-kinds", nil, "Include only output kinds (comma-separated values)")
}

func newBuildCmd(cfg RunConfig) *cobra.Command {
	cmd := buildCmd(cfg, &cobra.Command{
		Use:   "build",
		Short: "Render Flux objects to YAML (defaults to all)",
		Args:  cobra.NoArgs,
	}, runnableKinds...)
	cmd.AddCommand(
		buildCmd(cfg, &cobra.Command{
			Use:     useKustomization,
			Aliases: []string{kustomizationAlias, kustomizationAliases},
			Short:   "Render Kustomizations",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindKustomization),
		buildCmd(cfg, &cobra.Command{
			Use:     useHelmRelease,
			Aliases: []string{helmReleaseAlias, helmReleaseAliases},
			Short:   "Render HelmReleases",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindHelmRelease),
		buildCmd(cfg, &cobra.Command{
			Use:   useAll,
			Short: "Render all Flux producer output",
			Args:  cobra.NoArgs,
		}, runnableKinds...),
	)
	return cmd
}

func buildCmd(cfg RunConfig, cmd *cobra.Command, kinds ...string) *cobra.Command {
	c := &commonFlags{gitTransports: cfg.GitTransports}
	b := &buildFlags{}
	cmd.RunE = func(cmd *cobra.Command, argv []string) error {
		if !cmd.Flags().Changed("skip-kinds") {
			c.skipKinds = slices.DeleteFunc(c.skipKinds, func(kind string) bool {
				return slices.Contains(b.onlyKinds, kind)
			})
		}
		return runSelected(cmd.Context(), selectedRequest{
			flags: *c, roots: c.roots(firstArg(argv), kinds...),
			stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), stdin: cmd.InOrStdin(),
		}, func(inv *selectedInvocation) selectedPayload {
			groups := make([]output.DocumentGroup, len(inv.groups))
			inventory := make([][2][]manifest.NamedResource, len(inv.groups))
			var payloadErr error
			for i, group := range inv.groups {
				groups[i].Cluster = group.cluster
				var err error
				groups[i].Documents, inventory[i][0], err = collectRendered(group.sides[0], b)
				payloadErr = errors.Join(payloadErr, err)
			}
			return selectedPayload{output: inventory, err: payloadErr, write: func() error {
				return (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).DocumentGroups(
					groups,
				)
			}}
		})
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatYAML, output.FormatJSON)
	bindBuildFlags(cmd.Flags(), b)
	return cmd
}

func collectRendered(res *selection.Result, b *buildFlags) ([]map[string]any, []manifest.NamedResource, error) {
	count := renderedDocumentCount(res)
	out := make([]map[string]any, 0, count)
	identities := make([]manifest.NamedResource, 0, count)
	for j, batch := range res.Manifests {
		identity, err := batch.IdentityAccessor()
		if err != nil {
			return nil, nil, fmt.Errorf("batch %d: %w", j, err)
		}
		for _, i := range emissionOrder(batch.Documents, b) {
			out = append(out, batch.Documents[i])
			identities = append(identities, identity(i))
		}
	}
	return out, identities, nil
}

func emissionOrder(documents []map[string]any, b *buildFlags) []int {
	order := make([]int, len(documents))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return compareDocs(documents[a], documents[b]) })
	if len(b.onlyKinds) > 0 {
		return slices.DeleteFunc(order, func(i int) bool {
			return !slices.Contains(b.onlyKinds, manifest.DocKind(documents[i]))
		})
	}
	return order
}

func compareDocs(a, b map[string]any) int {
	return manifest.DocIdentity(a).Compare(manifest.DocIdentity(b))
}

func renderedDocumentCount(res *selection.Result) int {
	total := 0
	for _, batch := range res.Manifests {
		total += len(batch.Documents)
	}
	return total
}
