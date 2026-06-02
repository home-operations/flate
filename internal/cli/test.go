package cli

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/pkg/manifest"
)

// `test` emits a human-readable reconcile report. Plain text is the
// default; `-o markdown` swaps in the GitHub-flavored report shape
// (pipe-table summary + per-outcome task-list sections) for PR
// comments. Any other `-o` value is rejected so users don't silently
// get the same plain-text output regardless of what they ask for.
// When adding json/yaml here later, pass them through requireOutput.

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
			if err := c.requireOutput(format.OutputMarkdown); err != nil {
				return err
			}
			stopProfile, err := startProfile(c.profileMode, c.profileOut)
			if err != nil {
				return err
			}
			defer stopProfile()
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
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
				return errors.Join(fmt.Errorf("no %s named %q in --path", testKindName(kinds), name), runErr)
			}
			if err := emitTestReport(cmd.OutOrStdout(), report, c.outputOrDefault(format.OutputText)); err != nil {
				return errors.Join(err, runErr)
			}
			if report.AnyFailed() {
				return errors.Join(errors.New("test failures detected"), runErr)
			}
			return runErr
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputMarkdown)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

func testKindName(kinds []string) string {
	if len(kinds) == 1 {
		return kinds[0]
	}
	return "resource"
}

// emitTestReport dispatches the report to the renderer that matches
// the requested -o. The text path (default) keeps the pytest-style
// output testrunner.Report.Write produces; the markdown path emits
// the GitHub-flavored shape from testrunner.Report.WriteMarkdown.
func emitTestReport(w io.Writer, r testrunner.Report, out format.Output) error {
	switch out {
	case format.OutputMarkdown:
		return r.WriteMarkdown(w)
	default:
		return r.Write(w)
	}
}
