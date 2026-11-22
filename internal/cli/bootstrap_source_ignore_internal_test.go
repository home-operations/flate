package cli

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

//nolint:paralleltest // CLI invocations replace process-wide logging.
func TestRun_Build_OCIBootstrapSourceIgnore(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: root-source
  namespace: flux-system
spec:
  interval: 1m
  url: oci://registry.invalid/manifests
  ref:
    tag: stable
  ignore: /apps/excluded.yaml
`)
	const sourceRef = "  sourceRef: {kind: OCIRepository, name: root-source}\n"
	testutil.WriteFile(t, root, "entry/cluster.yaml", testutil.KustomizationYAML(
		"cluster", "flux-system", "  interval: 1m\n  path: ./entry\n"+sourceRef,
	))
	testutil.WriteFile(t, root, "entry/apps.yaml", testutil.KustomizationYAML(
		"apps", "flux-system", "  interval: 1m\n  path: ./apps\n"+sourceRef,
	))
	testutil.WriteFile(t, root, "apps/excluded.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ignore-witness-excluded, namespace: flux-system}\n")
	testutil.WriteFile(t, root, "apps/kept.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ignore-witness-kept, namespace: flux-system}\n")

	got := runCLI(t, "build", "all", "--path", filepath.Join(root, "entry"), "--only-kinds", "ConfigMap",
		"--no-progress", "--concurrency", "2", "--cache-dir", "")
	assert.Equal(t, got.code, 0)
	assert.Equal(t, got.stderr, "")
	assert.Diff(t, got.stdout, `---
apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: apps
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: ignore-witness-kept
  namespace: flux-system
`)
}
