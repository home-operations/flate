package cli

import (
	"errors"
	"slices"
	"time"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

// `test` emits a single human-readable, pytest-style reconcile report and
// has no output variants, so it binds no -o flag (bindCommon with no
// formats). When adding json/yaml here later, pass them to bindCommon.

func newTestCmd() *cobra.Command {
	var kinds []string
	for info := range manifest.Kinds() {
		if info.Includes("test") {
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
	h := &helmFlags{}
	cmd.RunE = func(cmd *cobra.Command, argv []string) error {
		stopProfile, err := startProfile()
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
		encoder := output.Encoder{Writer: cmd.OutOrStdout(), Color: output.ColorEnabled(cmd.OutOrStdout())}
		// Dependency failures outside the selected roster need a separate
		// diagnostic so a passing selected row cannot hide a fatal cause.
		warnings := slices.Concat(res.Warnings, res.Unrelated)
		notes := drainLogNotes()
		if err := encoder.Test(roster, warnings, notes, elapsed); err != nil {
			return errors.Join(err, runErr)
		}
		if name != "" && runErr != nil {
			diagnostics := failureModel(res)
			diagnostics.Warnings = nil
			if err := reportFailures(cmd.ErrOrStderr(), diagnostics, true, reportErrors{selected: runErr, remainder: remainder}); err != nil {
				return err
			}
		}
		final := runErr
		if roster.AnyFailed() {
			final = errors.Join(errors.New("test failures detected"), runErr)
		}
		// The top-level printer must not repeat errors already rendered in the report.
		if final != nil {
			return reportedError{final}
		}
		return nil
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
