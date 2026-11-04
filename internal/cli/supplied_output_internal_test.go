package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func testSuppliedSharedCommand(t *testing.T, verb, root, input string) {
	t.Helper()
	cli := runCLI(t, verb, "all", "--help")
	help, diagnostic, code := cli.stdout, cli.stderr, cli.code
	if code != 0 {
		t.Fatal(diagnostic)
	}
	for _, flag := range []string{"supply", "allow-missing-kinds", "strict-substitutions"} {
		for _, want := range []string{"--" + flag, envKey(flag)} {
			if !strings.Contains(help, want) {
				t.Fatalf("help missing %s", want)
			}
		}
	}
	args := []string{
		verb,
		"all",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"--supply",
		input,
		"--strict-substitutions",
		"--log-level",
		"debug",
	}
	if verb == "diff" {
		base := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}-base")
		args = append(args, "--path-orig", base, "-o", "diff", "--skip-kinds", "CustomResourceDefinition")
	}
	cli2 := runCLI(t, args...)
	out, diagnostic, code := cli2.stdout, cli2.stderr, cli2.code
	if code != 0 {
		t.Fatalf("shared inputs failed: %s", diagnostic)
	}
	testSuppliedCommandOutput(t, verb, out, diagnostic)
}

func testSuppliedCommandOutput(t *testing.T, verb, out, diagnostic string) {
	t.Helper()
	summaries := 0
	for line := range strings.Lines(diagnostic) {
		if !strings.Contains(line, "orchestrator: reconcile complete") {
			continue
		}
		summaries++
		assert.FatalIf(
			t,
			!strings.Contains(line, "kustomizations=1 "),
			"supplied declaration counted in summary: %s",
			line,
		)
	}
	assert.FatalIf(t, summaries == 0, "%v", "missing render accounting proof")
	assert.FatalIf(
		t,
		verb == "test" && !strings.Contains(out, "1 passed"),
		"supplied declarations counted as tests: %s",
		out,
	)
	assert.FatalIf(t, strings.Contains(out, "unused-supply") || strings.Contains(out, "supplied.example.io") ||
		strings.Contains(out, "private-fixture-value"), "supplied declaration appeared in %s: %s", verb, out)
	assert.FatalIf(t, verb == "diff" && (strings.Contains(out, "name: external") || !strings.Contains(out, "supplied")),
		"diff emitted input or omitted selected consumer: %s", out)
}
