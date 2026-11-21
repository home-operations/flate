package cli_test

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	gitconfig "github.com/go-git/go-git/v6/config"

	"github.com/home-operations/flate/internal/cli"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

type offlineRenderFailureCase struct {
	name     string
	kind     string
	resource string
	file     string
	rendered string
	key      string
	cause    string
}

//nolint:paralleltest // cli.Run changes process-global logging.
func TestE2E_OfflineRenderFailure(t *testing.T) {
	for _, tc := range []offlineRenderFailureCase{
		{
			name: "substitution", kind: "Kustomization", resource: "ks", key: "TOKEN",
			file: "app/object.yaml", rendered: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '${TOKEN}'}\n",
			cause: "document 1: ConfigMap missing metadata.name",
		},
		{
			name: "helm", kind: "HelmRelease", resource: "hr", key: "values.yaml",
			file: "chart/templates/object.yaml", rendered: `{{ required "token required" .Values.token }}`,
			cause: "helm template: execution error at (chart/templates/object.yaml:1:3): token required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-global logging.
func TestE2E_OfflineFluxInstanceGenerationFailure(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	const origin = "https://github.com/example/fleet.git"
	_, err := repo.CreateRemote(&gitconfig.RemoteConfig{Name: "origin", URLs: []string{origin}})
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "apps/instance.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec:
  distribution: {version: "2.x", registry: ghcr.io/fluxcd}
  sync: {kind: GitRepository, url: https://github.com/example/fleet.git, ref: refs/heads/main, path: apps}
`)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", `resources: [instance.yaml, media]
patches:
- target: {kind: FluxInstance, name: flux}
  patch: |-
    - op: add
      path: /spec/kustomize
      value:
        patches:
        - target: {kind: Kustomization, name: flux-system}
          patch: |
            - op: add
              path: /missing/path
              value: invalid
`)
	testutil.WriteFile(t, root, "apps/media/kustomization.yaml", "resources: [configmap.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/media/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: media}\n",
	)
	for _, tc := range []struct {
		name, path, stdout, stderr string
		code                       int
	}{
		{
			name: "private probe", path: "apps/media",
			stdout: "---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: media\n",
			stderr: "\n  ⚠ warnings (1)\n" +
				"    [EmptyScan] no Flux Kustomization or HelmRelease objects found under the input path\n",
		},
		{
			name: "requested producer", path: "apps",
			stderr: `flate error: kustomize build: transform patch: add operation does not apply: doc is missing path: "/missing/path": missing value` + "\n",
			code:   1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := cli.Run([]string{
				"build", "all", "--path", filepath.Join(root, tc.path),
				"--concurrency=2", "--cache-dir", "", "--no-progress",
			}, &stdout, &stderr, cli.RunConfig{})
			assert.Equal(t, code, tc.code)
			assert.Diff(t, stdout.String(), tc.stdout)
			assert.Diff(t, stderr.String(), tc.stderr)
		})
	}
}

func (tc *offlineRenderFailureCase) run(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [consumer.yaml, input.yaml]\n")
	if tc.resource == "ks" {
		testutil.WriteFile(t, root, "consumer.yaml", testutil.KustomizationYAML("consumer", "apps",
			"  path: ./app\n  postBuild: {substituteFrom: [{kind: Secret, name: input}]}\n"))
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [object.yaml]\n")
	} else {
		testutil.WriteFile(t, root, "consumer.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: consumer, namespace: apps}
spec:
  chart:
    spec:
      chart: ./chart
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  valuesFrom: [{kind: Secret, name: input}]
`)
		testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 0.1.0\n")
	}
	testutil.WriteFile(t, root, tc.file, tc.rendered)
	cache := t.TempDir()
	for _, input := range []string{"encrypted", "readable"} {
		value := "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"
		if input == "readable" {
			value = ""
			if tc.resource == "hr" {
				value = "token: ''"
			}
		}
		testutil.WriteFile(
			t,
			root,
			"input.yaml",
			"apiVersion: v1\nkind: Secret\nmetadata: {name: input, namespace: apps}\nstringData:\n  "+tc.key+": "+"'"+strings.ReplaceAll(
				value,
				"'",
				"''",
			)+"'"+"\n",
		)
		for range 2 {
			var stdout, stderr bytes.Buffer
			code := cli.Run([]string{
				"test", tc.resource, "consumer", "--path", root,
				"--concurrency=2", "--cache-dir", cache, "--no-progress", "--log-level=error",
			}, &stdout, &stderr, cli.RunConfig{})
			tc.assertStreams(t, input, [2]string{entrySourceTestOutput(stdout.String()), stderr.String()}, code)
		}
	}
}

func (tc *offlineRenderFailureCase) assertStreams(t *testing.T, input string, got [2]string, code int) {
	t.Helper()
	glyph, verdict, counts, reason, wantCode := "‒", "✓", "0 passed · 1 skipped", tc.cause, 0
	if input == "encrypted" {
		use := "substituteFrom"
		if tc.resource == "hr" {
			use = "valuesFrom"
		}
		reason = "cannot render offline: unreadable inputs " + use + " Secret apps/input key \"" + tc.key + "\" (reference 1): " + tc.cause
	} else {
		glyph, verdict, counts, wantCode = "✗", "✗", "0 passed · 1 failed", 1
	}
	want := "\n  " + glyph + "  " + tc.kind + "  apps/consumer  " + reason + "\n\n  " + verdict + " " + counts + "   <DUR>\n"
	wantStderr := ""
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		wantStderr = "\n  ⚠ warnings (1)\n    [RenderPersistenceDisabled] render persistence is disabled: " + reason + "; source caching remains available\n"
	}
	assert.Equal(t, code, wantCode)
	assert.Equal(t, got[0], want)
	assert.Equal(t, got[1], wantStderr)
}
