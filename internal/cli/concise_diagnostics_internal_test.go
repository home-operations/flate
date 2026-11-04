package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ConciseFailureDiagnostics(t *testing.T) {
	current, orig := writeFixture(t), writeFixture(t)
	testutil.WriteFile(
		t,
		current,
		"flux/cluster.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild: {substitute: {flate-campaign: value}}\n",
		),
	)
	for _, command := range []string{"test", "build", "diff"} {
		t.Run(command, func(t *testing.T) { runConciseFailureDiagnosticsCommand(t, &command, &current, &orig) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ConciseEntryFailureDiagnostics(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [missing.yaml]\n")
	var first string
	for _, level := range []string{"error", "debug"} {
		cli := runCLI(
			t,
			"build",
			"all",
			"--path",
			root,
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
			"--log-level",
			level,
		)
		stderr, code := cli.stderr, cli.code
		assert.FatalIf(t, code != 1, "exit=%d stderr=%s", code, stderr)
		found := false
		for row := range strings.SplitSeq(stderr, "\n") {
			if !strings.Contains(row, "✗") || !strings.Contains(row, "missing.yaml") {
				continue
			}
			found = true
			assert.FatalIf(t, first != "" && first != row, "debug changed entry failure: %q != %q", row, first)
			first = row
		}
		assert.FatalIf(t, !found || !strings.Contains(stderr, "missing.yaml"), "missing entry failure: %s", stderr)
		for _, message := range []string{"orchestrator: entry failed", "cli: invocation failed"} {
			assert.FatalIf(
				t,
				strings.Contains(stderr, message) != (level == "debug"),
				"diagnostic %q at %s: %s",
				message,
				level,
				stderr,
			)
		}
	}
}

func runConciseFailureDiagnosticsCommand(t *testing.T, command, current, orig *string) {
	t.Helper()

	var first string
	for _, level := range []string{"error", "debug"} {
		args := []string{
			(*command),
			"all",
			"--path",
			(*current),
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
			"--log-level",
			level,
		}
		if (*command) == "diff" {
			args = append(args, "--path-orig", (*orig))
		}
		cli := runCLI(t, args...)
		stdout, stderr, code := cli.stdout, cli.stderr, cli.code
		assert.FatalIf(t, code != 1, "exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		report := stderr
		if (*command) == "test" {
			report = stdout
		}
		rows := strings.Split(report, "\n")
		found := false
		for _, row := range rows {
			if !strings.Contains(row, "✗") || !strings.Contains(row, "flux-system/apps") {
				continue
			}
			found = true
			assert.FatalIf(t, strings.Count(row, "flux-system/apps") != 1 ||
				strings.Count(row, "invalid substitute variable flate-campaign") != 1 ||
				strings.Contains(row, "flux error") ||
				strings.Contains(row, "invalid substitute reference"), "failure row: %s", row)
			assert.FatalIf(t, first != "" && row != first, "debug changed the report: %q != %q", row, first)
			first = row
		}
		assert.FatalIf(t, !found, "missing failure row: %s", report)
		assert.FatalIf(
			t,
			strings.Contains(stderr, "must match") != (level == "debug"),
			"debug details at %s: %s",
			level,
			stderr,
		)
	}
}
