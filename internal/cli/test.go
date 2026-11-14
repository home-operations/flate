package cli

import (
	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/pkg/manifest"
)

func newTestCmd(cfg RunConfig) *cobra.Command {
	var kinds []string
	for info := range manifest.Kinds() {
		if info.Test {
			kinds = append(kinds, info.Kind)
		}
	}
	cmd := testCmd(cfg, &cobra.Command{
		Use:   "test",
		Short: "Report reconcile status (defaults to all)",
		Args:  cobra.NoArgs,
	}, kinds...)
	cmd.AddCommand(
		testCmd(cfg, &cobra.Command{
			Use:     useKustomization,
			Aliases: []string{kustomizationAlias, kustomizationAliases},
			Short:   "Validate Kustomizations",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindKustomization),
		testCmd(cfg, &cobra.Command{
			Use:     useHelmRelease,
			Aliases: []string{helmReleaseAlias, helmReleaseAliases},
			Short:   "Validate HelmReleases",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindHelmRelease),
		testCmd(cfg, &cobra.Command{
			Use:   useAll,
			Short: "Validate Flux resources",
			Args:  cobra.NoArgs,
		}, kinds...),
	)
	return cmd
}

func testCmd(cfg RunConfig, cmd *cobra.Command, kinds ...string) *cobra.Command {
	c := &commonFlags{gitTransports: cfg.GitTransports}
	cmd.RunE = func(cmd *cobra.Command, argv []string) error {
		return runSelected(cmd.Context(), selectedRequest{
			flags: *c, roots: c.roots(firstArg(argv), kinds...),
			stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), stdin: cmd.InOrStdin(), test: true,
		}, func(inv *selectedInvocation) selectedPayload {
			rosters := make([]output.TestReport, len(inv.groups))
			inventory := make([][2][]manifest.NamedResource, len(inv.groups))
			for i, group := range inv.groups {
				rosters[i] = testrunner.Run(group.sides[0])
				for _, cases := range [2][]output.Case{rosters[i].Cases, rosters[i].Blocked} {
					for _, test := range cases {
						inventory[i][0] = append(inventory[i][0], test.ID)
					}
				}
			}
			return selectedPayload{rosters: rosters, output: inventory}
		})
	}
	bindCommon(cmd.Flags(), c)
	return cmd
}
