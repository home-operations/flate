package cli_test

import (
	"bytes"
	"strings"
	"testing"

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
