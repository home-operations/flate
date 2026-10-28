package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestRun_OptionalAdmissionFailure(t *testing.T) {
	preservePersistenceNoticeState(t)
	before, current := t.TempDir(), t.TempDir()
	for i, root := range []string{before, current} {
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  inputs: [{name: app}]
  resourcesTemplate: |
    apiVersion: helm.toolkit.fluxcd.io/v2
    kind: HelmRelease
    metadata: {name: consumer, namespace: apps}
    spec:
      interval: 10m
      chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}
      valuesFrom: [{kind: ConfigMap, name: input, optional: true}]
      values: {version: `+[]string{"before", "after"}[i]+`}
`)
		testutil.WriteFile(t, root, "flux/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {values.yaml: '{}'}\n")
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: should-not-render}\n")
	}
	control, diagnostic, code := runCLI(t, "build", "all", "--path", filepath.Join(before, "flux"), "--concurrency", "2", "--cache-dir", t.TempDir())
	if code != 0 || !strings.Contains(control, "should-not-render") {
		t.Fatalf("optional input control failed: code=%d stdout=%s stderr=%s", code, control, diagnostic)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	testutil.WriteFile(t, current, "flux/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: [unterminated\n")
	code = Run([]string{"diff", "all", "--path", filepath.Join(current, "flux"), "--path-orig", filepath.Join(before, "flux"), "--concurrency", "2", "--cache-dir", t.TempDir(), "--log-level", "debug"}, &stdout, &stderr)
	assert.Equal(t, code, 1)
	assert.Equal(t, strings.Contains(stderr.String(), "input.yaml"), true)
	if !strings.Contains(stderr.String(), "YAML") {
		t.Fatalf("missing input parse failure diagnostic: %s", stderr.String())
	}
	assert.Equal(t, strings.Contains(stderr.String(), "required valuesFrom ConfigMap/apps/input omitted"), false)
	assert.Equal(t, strings.Contains(stdout.String(), "should-not-render"), false)
}
