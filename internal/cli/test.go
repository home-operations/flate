package cli

import (
	"errors"
	"time"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/pkg/manifest"
)

// `test` emits a single human-readable, pytest-style reconcile report and
// has no output variants, so it binds no -o flag (bindCommon with no
// formats). When adding json/yaml here later, pass them to bindCommon.

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
			stopProfile, err := startProfile(c.profileMode, c.profileOut)
			if err != nil {
				return err
			}
			defer stopProfile()
			start := time.Now()
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
			elapsed := time.Since(start)
			// testrunner.Report.AnyFailed already covers per-resource
			// reconcile failures, so the runErr is informational here —
			// the structured report is what the user reads. We still
			// surface a non-zero exit on any failure.
			name := firstArg(argv)
			report := testrunner.Run(testrunner.Job{
				Store: o.Store(),
				Kinds: kinds,
				Name:  name,
				Include: func(id manifest.NamedResource) bool {
					return c.includeNamespace(o.Filter(), id.Namespace)
				},
			})
			runErr = scopedRunError(o, res, c, runErr)
			if name != "" && report.Matched == 0 {
				return errors.Join(noNamedError(testKindName(kinds), name), runErr)
			}
			// Colorize only when stdout can show it — style.ColorEnabled
			// (colorprofile) honors NO_COLOR / CLICOLOR / TTY-ness; piped or
			// redirected output stays plain.
			color := style.ColorEnabled(cmd.OutOrStdout())
			if err := report.Write(cmd.OutOrStdout(), color, elapsed); err != nil {
				return errors.Join(err, runErr)
			}
			if report.AnyFailed() {
				return errors.Join(errors.New("test failures detected"), runErr)
			}
			return runErr
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

func testKindName(kinds []string) string {
	if len(kinds) == 1 {
		return kinds[0]
	}
	return "resource"
}
