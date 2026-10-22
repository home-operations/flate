package cli

import (
	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/pkg/manifest"
)

func newTestCmd() *cobra.Command {
	var kinds []string
	for info := range manifest.Kinds() {
		if info.Test {
			kinds = append(kinds, info.Kind)
		}
	}
	cmd := testCmd(&cobra.Command{
		Use:   "test",
		Short: "Report Kustomization + HelmRelease reconcile status (defaults to all)",
		Args:  cobra.NoArgs,
	}, kinds...)
	cmd.AddCommand(
		testCmd(&cobra.Command{
			Use:     "ks [name]",
			Aliases: []string{"kustomization", "kustomizations"},
			Short:   "Validate Kustomizations",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindKustomization),
		testCmd(&cobra.Command{
			Use:     "hr [name]",
			Aliases: []string{"helmrelease", "helmreleases"},
			Short:   "Validate HelmReleases",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindHelmRelease),
		testCmd(&cobra.Command{
			Use:   "all",
			Short: "Validate every Kustomization, HelmRelease, and Flux source CR",
			Args:  cobra.NoArgs,
		}, kinds...),
	)
	return cmd
}

func testCmd(cmd *cobra.Command, kinds ...string) *cobra.Command {
	c := &commonFlags{}
	cmd.RunE = func(cmd *cobra.Command, argv []string) error {
		return runSelected(cmd.Context(), selectedRequest{
			flags: *c, roots: c.roots(firstArg(argv), kinds...),
			stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(), presentation: presentTest,
		}, func(inv *selectedInvocation) selectedPayload {
			roster := testrunner.Run(inv.sides[0])
			return selectedPayload{roster: &roster}
		})
	}
	bindCommon(cmd.Flags(), c)
	return cmd
}
