package cli

import (
	"errors"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/pkg/manifest"
)

func newTestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Report Kustomization + HelmRelease reconcile status",
	}
	cmd.AddCommand(
		testCmd("ks [name]", []string{"kustomization", "kustomizations"},
			"Validate Kustomizations", cobra.MaximumNArgs(1),
			manifest.KindKustomization),
		testCmd("hr [name]", []string{"helmrelease", "helmreleases"},
			"Validate HelmReleases", cobra.MaximumNArgs(1),
			manifest.KindHelmRelease),
		testCmd("all", nil,
			"Validate every Kustomization, HelmRelease, and Flux source CR", cobra.NoArgs,
			// Source kinds are included so a soft-skipped source (e.g.
			// --allow-missing-secrets on an OCIRepository whose auth Secret
			// is materialized live via ExternalSecret) shows up on its own
			// line as SKIPPED instead of only as a parenthetical reason
			// on its downstream KS/HR.
			manifest.KindKustomization, manifest.KindHelmRelease,
			manifest.KindGitRepository, manifest.KindOCIRepository,
			manifest.KindHelmRepository, manifest.KindBucket),
	)
	return cmd
}

func testCmd(use string, aliases []string, short string, args cobra.PositionalArgs, kinds ...string) *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use:     use,
		Aliases: aliases,
		Short:   short,
		Args:    args,
		RunE: func(cmd *cobra.Command, argv []string) error {
			o, _, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
			// testrunner.Report.AnyFailed already covers per-resource
			// reconcile failures, so the runErr is informational here —
			// the structured report is what the user reads. We still
			// surface a non-zero exit on any failure.
			report := testrunner.Run(testrunner.Job{
				Store: o.Store(),
				Kinds: kinds,
				Name:  firstArg(argv),
			})
			report.Write(cmd.OutOrStdout())
			if report.AnyFailed() {
				return errors.New("test failures detected")
			}
			return runErr
		},
	}
	bindCommon(cmd.Flags(), c)
	if rendersHelm(kinds) {
		bindHelmFlags(cmd.Flags(), h)
	}
	return cmd
}
