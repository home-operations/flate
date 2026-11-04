package scope_test

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_OrphanParentSource(t *testing.T) {
	for _, tc := range []struct {
		name     string
		external bool
	}{
		{name: "local parent"},
		{name: "deeper external parent", external: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(
				t,
				dir,
				"kubernetes/flux/ks.yaml",
				testutil.KustomizationYAML(
					"local-apps",
					"flux-system",
					"  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			resources := "resources: [configmap.yaml]\n"
			if tc.external {
				resources = "resources: [configmap.yaml, external.yaml]\n"
				testutil.WriteFile(t, dir, "kubernetes/apps/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec:
  provider: github
  url: https://fixture.invalid/external
  secretRef: {name: missing-auth}
`+"---\n"+
					testutil.KustomizationYAML("external-parent", "flux-system", `  path: ./kubernetes/apps/orphan
  sourceRef: {kind: GitRepository, name: external}
`))
			}
			testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", resources)
			testutil.WriteFile(t, dir, "kubernetes/apps/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: healthy-output}
data: {value: rendered}
`)
			testutil.WriteFile(
				t,
				dir,
				"kubernetes/apps/orphan/ks.yaml",
				testutil.KustomizationYAML(
					"orphan",
					"flux-system",
					"  path: ./missing\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			stdout, stderr, code := harness.RunCLIBuffers(
				"build",
				"all",
				"--path",
				dir,
				"--allow-missing-kinds=Secret,ConfigMap",
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			)
			assert.FatalIf(t, code != 0 || !strings.Contains(stdout, "name: healthy-output") ||
				strings.Contains(
					stdout,
					"name: orphan\n",
				), "unreferenced child failed a healthy local build: exit=%d\n%s\n%s", code, stdout, stderr)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_OrphanedKustomizationNotDiscovered(t *testing.T) {
	src := harness.TestdataPath(t, "orphans")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	// "wired" is referenced and should pass.
	wiredLine := harness.MustExtractLine(t, out, "flux-system/wired")
	assert.ErrorIf(t, !strings.Contains(wiredLine, "✓"), "wired should pass: %s", wiredLine)
	// "orphan" must not appear anywhere in the test output — it was
	// never loaded because the kustomize graph doesn't reach it.
	assert.ErrorIf(
		t,
		strings.Contains(out, "flux-system/orphan"),
		"orphan Kustomization should be invisible to graph-driven loader; output contained reference:\n%s",
		out,
	)
}
