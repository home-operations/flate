package cli

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func writeGetRelease(t *testing.T, root, dir string) {
	t.Helper()
	testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/app/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: installed, namespace: workloads}\ndata: {value: rendered}\n",
	)
	testutil.WriteFile(t, root, dir+"/app/kustomization.yaml", "resources: [pod.yaml, release.yaml]\n")
	testutil.WriteFile(t, root, dir+"/app/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: shared, namespace: apps}
spec:
  targetNamespace: workloads
  releaseName: installed
  chart:
    spec:
      chart: ./charts/app
      version: 0.1.0
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
}
