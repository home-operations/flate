package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffKS_NamedExternalSourceDisclosure(t *testing.T) {
	current, orig := writeCustomSourceDiffFixture(t, "ssh://git@github.com/example/home-ops.git")
	for _, dir := range []string{current, orig} {
		testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [cluster.yaml, second.yaml, external.yaml]\n")
		for _, rel := range []string{"flux/cluster.yaml", "flux/second.yaml"} {
			data, err := os.ReadFile(filepath.Join(dir, rel))
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, rel, strings.ReplaceAll(string(data), "gitops-system", "alpha"))
		}
		testutil.WriteFile(t, dir, "flux/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: beta}
spec: {url: "https://example.invalid/external.git", interval: 1h}
`+"---\n"+
			testutil.KustomizationYAML("external-apps", "beta", `  path: ./external
  sourceRef: {kind: GitRepository, name: external}
  interval: 1h
`))
	}
	for _, app := range []string{"apps", "second-apps"} {
		name := "hello"
		if app == "second-apps" {
			name = "second"
		}
		testutil.WriteFile(
			t,
			orig,
			app+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {greeting: hola}\n",
		)
	}
	for _, tc := range []struct {
		name      string
		args      []string
		warn      bool
		code      int
		fieldDiff bool
	}{
		{name: "named inferred namespace", args: []string{"external-apps"}, warn: true, code: 1},
		{name: "named at error log level", args: []string{"external-apps", "--log-level", "error"}, warn: true, code: 1},
		{name: "unnamed", warn: true, fieldDiff: true},
		{name: "explicit namespace", args: []string{"external-apps", "--namespace", "beta"}, warn: true},
		{name: "excluded namespace", args: []string{"external-apps", "--namespace", "alpha"}, code: 1},
		{name: "local name", args: []string{"apps"}, fieldDiff: true},
		{name: "nonexistent name", args: []string{"missing"}, code: 1},
	} {
		t.Run(tc.name, func(t *testing.T) { runDiffKSNamedExternalSourceDisclosureName(t, &tc, &current, &orig) })
	}
}

func runDiffKSNamedExternalSourceDisclosureName(t *testing.T, tc *struct {
	name      string
	args      []string
	warn      bool
	code      int
	fieldDiff bool
}, current *string, orig *string,
) {
	t.Helper()

	args := append([]string{"diff", "ks"}, tc.args...)
	args = append(args, "--path", (*current), "--path-orig", (*orig), "--cache-dir", t.TempDir(),
		"--concurrency", "2", "-o", "diff")
	stdout, stderr, code := runCLI(t, args...)
	assert.FatalIf(t, code != tc.code, "diff exited %d, want %d: %s", code, tc.code, stderr)
	origWarning := strings.Index(stderr, "orig snapshot:")
	currentWarning := strings.Index(stderr, "current snapshot:")
	errorAt := strings.Index(stderr, "no Kustomization named")
	switch {
	case tc.warn:

		assert.ErrorIf(t, origWarning < 0 || currentWarning <= origWarning ||
			strings.Count(
				stderr,
				"Kustomization beta/external-apps: external source GitRepository.source.toolkit.fluxcd.io/beta/external",
			) != 2 ||
			strings.Count(
				stderr,
				"not rendered",
			) != 2, "want orig then current external-source disclosure: %s", stderr)
		assert.ErrorIf(
			t,
			tc.code != 0 && (errorAt < 0 || errorAt >= origWarning),
			"missing-name error must precede warnings: %s",
			stderr,
		)

	case strings.Contains(stderr, "external source"):

		t.Errorf("unrelated external source was disclosed: %s", stderr)
	}
	assert.ErrorIf(t, tc.code != 0 && errorAt < 0, "missing-name error absent: %s", stderr)
	switch {
	case tc.fieldDiff:

		for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
			assert.ErrorIf(t, !strings.Contains(stdout, want), "missing local field diff %q:\n%s", want, stdout)
		}

	case stdout != "":

		t.Errorf("external selection produced resource output:\n%s", stdout)
	}
}
