package scope_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_AuthoritativeManifestFailures(t *testing.T) {
	const release = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: invalid, namespace: flux-system}
spec:
  interval: []
  suspend: true
  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system}}}
`
	for _, tc := range []struct {
		name, document, patch, extra, producer, symptom string
		failed                                          bool
	}{
		{name: "typed release", document: release, producer: "Kustomization/flux-system/owner", symptom: "cannot unmarshal", failed: true},
		{name: "typed kustomization", document: testutil.KustomizationYAML("invalid", "flux-system", "  interval: []\n  path: ./child\n  sourceRef: {kind: GitRepository, name: flux-system}\n"), producer: "Kustomization/flux-system/owner", symptom: "cannot unmarshal", failed: true},
		{name: "generated release", document: "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: generator, namespace: flux-system}\nspec:\n  resourcesTemplate: |\n    " + strings.ReplaceAll(strings.TrimSpace(release), "\n", "\n    ") + "\n", producer: "ResourceSet/flux-system/generator", symptom: "cannot unmarshal", failed: true},
		{name: "patch-created release", document: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: invalid, namespace: flux-system}\n", patch: "patches:\n- target: {kind: ConfigMap, name: invalid}\n  patch: |-\n    - op: replace\n      path: /apiVersion\n      value: helm.toolkit.fluxcd.io/v2\n    - op: replace\n      path: /kind\n      value: HelmRelease\n    - op: add\n      path: /spec\n      value: {interval: [], suspend: true}\n", producer: "Kustomization/flux-system/owner", symptom: "cannot unmarshal", failed: true},
		{name: "required syntax", document: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: broken}\nbroken: [\n", producer: "Kustomization/flux-system/owner", symptom: "did not find expected", failed: true},
		{name: "repaired raw release", document: release, patch: "patches:\n- target: {kind: HelmRelease, name: invalid}\n  patch: |-\n    - op: replace\n      path: /spec/interval\n      value: 10m\n"},
		{name: "incidental malformed and scalar", document: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\n", extra: "configMapGenerator:\n- name: settings\n  files: [settings.yaml]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.GitInit(t, dir)
			testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [drivers.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"flux/drivers.yaml",
				testutil.KustomizationYAML(
					"owner",
					"flux-system",
					"  path: ./app\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				)+"---\n"+testutil.KustomizationYAML(
					"healthy",
					"flux-system",
					"  path: ./healthy\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources: [child.yaml]\n"+tc.patch+tc.extra)
			testutil.WriteFile(t, dir, "app/child.yaml", tc.document)
			testutil.WriteFile(t, dir, "app/unrelated.yaml", "broken: [\n")
			testutil.WriteFile(t, dir, "app/settings.yaml", "just scalar content\n")
			testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"healthy/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent-output}\n",
			)
			args := []string{
				"build",
				"all",
				"--path",
				filepath.Join(dir, "flux"),
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			want := 0
			if tc.failed {
				want = 1
			}
			assert.FatalIf(
				t,
				code != want || !strings.Contains(stdout, "independent-output"),
				"exit=%d want=%d\n%s\n%s",
				code,
				want,
				stdout,
				stderr,
			)
			producer := strings.SplitN(tc.producer, "/", 2)
			assert.FatalIf(t, tc.failed &&
				(!strings.Contains(stderr, producer[0]) || !strings.Contains(stderr, producer[1]) || strings.Count(stderr, tc.symptom) != 1), "unstable or unowned failure:\n%s", stderr)
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}
