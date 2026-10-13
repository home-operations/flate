package cli

import (
	"errors"
	"slices"
	"time"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/report"
	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

// `test` emits a single human-readable, pytest-style reconcile report and
// has no output variants, so it binds no -o flag (bindCommon with no
// formats). When adding json/yaml here later, pass them to bindCommon.

func newTestCmd() *cobra.Command {
	// Include source kinds so soft-skipped sources appear in the report.
	kinds := []string{manifest.KindKustomization, manifest.KindHelmRelease,
		manifest.KindGitRepository, manifest.KindOCIRepository,
		manifest.KindHelmRepository, manifest.KindBucket}
	cmd := testCmd("test", nil, "Report Kustomization + HelmRelease reconcile status (defaults to all)", cobra.NoArgs, kinds...)
	cmd.AddCommand(
		testCmd("ks [name]", []string{"kustomization", "kustomizations"},
			"Validate Kustomizations", cobra.MaximumNArgs(1),
			manifest.KindKustomization),
		testCmd("hr [name]", []string{"helmrelease", "helmreleases"},
			"Validate HelmReleases", cobra.MaximumNArgs(1),
			manifest.KindHelmRelease),
		testCmd("all", nil,
			"Validate every Kustomization, HelmRelease, and Flux source CR", cobra.NoArgs,
			kinds...),
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
			name := firstArg(argv)
			selected, runErr, remainder := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr,
				c.roots(name, kinds...), nil)
			if len(selected) == 0 {
				return runErr
			}
			res = selected[0]
			roster := testrunner.Run(res)
			if name != "" && roster.Matched == 0 {
				return errors.Join(noNamedError(testKindName(kinds), name), runErr)
			}
			// Colorize only when stdout can show it — style.ColorEnabled
			// (colorprofile) honors NO_COLOR / CLICOLOR / TTY-ness; piped or
			// redirected output stays plain.
			color := style.ColorEnabled(cmd.OutOrStdout())
			// One consolidated report on stdout: the per-resource report (failures
			// already appear inline on their rows), then render advisories
			// (warnings) and deferred logs (notes), then a single verdict.
			// Dependency failures outside the selected roster need a separate
			// diagnostic so a passing selected row cannot hide a fatal cause.
			warnings := slices.Concat(res.Warnings, res.Unrelated)
			notes := drainLogNotes()
			if err := roster.Write(cmd.OutOrStdout(), warnings, notes, color, elapsed); err != nil {
				return errors.Join(err, runErr)
			}
			if name != "" && runErr != nil {
				diagnostics := report.Build(res)
				diagnostics.Warnings = nil
				if err := reportFailures(cmd.ErrOrStderr(), diagnostics, true, reportErrors{selected: runErr, remainder: remainder}); err != nil {
					return err
				}
			}
			final := runErr
			if roster.AnyFailed() {
				final = errors.Join(errors.New("test failures detected"), runErr)
			}
			// The report is rendered; wrap so run's top-level printer doesn't
			// reprint the error.
			if final != nil {
				return reportedError{final}
			}
			return nil
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
