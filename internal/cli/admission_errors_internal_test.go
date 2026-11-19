package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

//nolint:paralleltest // preservePersistenceNoticeState changes FLATE_* variables and restores shared CLI logging and persistence state.
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
		testutil.WriteFile(
			t,
			root,
			"flux/input.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: {values.yaml: '{}'}\n",
		)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(
			t,
			root,
			"charts/demo/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: should-not-render}\n",
		)
	}
	cli := runCLI(
		t,
		"build",
		"all",
		"--path",
		filepath.Join(before, "flux"),
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	)
	control, diagnostic, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 0 || !strings.Contains(control, "should-not-render"),
		"optional input control failed: code=%d stdout=%s stderr=%s",
		code,
		control,
		diagnostic,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	testutil.WriteFile(
		t,
		current,
		"flux/input.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\ndata: [unterminated\n",
	)
	code = Run(
		[]string{
			"diff",
			"all",
			"--path",
			filepath.Join(current, "flux"),
			"--path-orig",
			filepath.Join(before, "flux"),
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
			"--log-level",
			"debug",
		},
		&stdout,
		&stderr, RunConfig{},
	)
	assert.Equal(t, code, 1)
	assert.Equal(t, strings.Contains(stderr.String(), "input.yaml"), true)
	assert.FatalIf(
		t,
		!strings.Contains(stderr.String(), "YAML"),
		"missing input parse failure diagnostic: %s",
		stderr.String(),
	)
	assert.Equal(t, strings.Contains(stderr.String(), "required valuesFrom ConfigMap/apps/input omitted"), false)
	assert.Equal(t, strings.Contains(stdout.String(), "should-not-render"), false)
}

//nolint:paralleltest // cli.Run uses process-wide logging and configuration.
func TestRun_DiffBootstrapPatchKeepsFatalScope(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = ""
	const url = "https://fixture.invalid/repo"
	const configMap = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: value, namespace: apps}\ndata: {value: "
	const source = "  sourceRef: {kind: GitRepository, name: self}\n"
	const substitution = "  postBuild: {substituteFrom: [{kind: Secret, name: absent}]}\n"
	const builder = `resources: [sync.yaml, cm.yaml]
patches:
- target: {kind: ConfigMap}
  patch: |-
    - op: replace
      path: /data/value
      value: new
`
	roots := [2]string{t.TempDir(), t.TempDir()}
	for i, root := range roots {
		writeBaselineGit(t, root, url)
		testutil.WriteFile(t, root, "entry/sync/kustomization.yaml", builder)
		testutil.WriteFile(t, root, "entry/sync/sync.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: self, namespace: apps}
spec: {url: "`+url+`"}
---
`+testutil.KustomizationYAML("root", "apps", "  path: ./entry\n"+source))
		testutil.WriteFile(t, root, "entry/sync/cm.yaml", configMap+"old}\n")
		testutil.WriteFile(t, root, "entry/apps/ks.yaml", testutil.KustomizationYAML(
			"apps", "apps", "  path: ./apps\n"+source+"  dependsOn: [{name: prereq}]\n"+substitution,
		))
		testutil.WriteFile(t, root, "entry/prereq/ks.yaml", testutil.KustomizationYAML(
			"prereq", "apps", "  path: ./prereq\n"+source+substitution,
		))
		for _, path := range []string{"apps/value.yaml", "prereq/value.yaml"} {
			testutil.WriteFile(t, root, path, configMap+[]string{"before", "after"}[i]+"}\n")
		}
	}
	got := runCLI(
		t, "diff", "all", "--path", filepath.Join(roots[1], "entry"),
		"--path-orig", filepath.Join(roots[0], "entry"),
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(),
	)
	assert.Equal(t, got.code, 0)
	assert.Equal(t, got.stdout, "")
	assert.Equal(t, got.stderr, "")
}
