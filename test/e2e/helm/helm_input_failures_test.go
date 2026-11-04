package helm_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_UnreadableParentDoesNotFillRequiredValues(t *testing.T) {
	dir := sourceValuesFixture(t, "[]", false, false)
	testutil.WriteFile(t, dir, "charts/jenkins-deployment/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}-values'}
data: {greeting: {{ required "missing required value" .Values.greeting | quote }}}
`)
	parentPath := filepath.Join(dir, "apps/demo/ks.yaml")
	parent, err := os.ReadFile(parentPath)
	assert.NoError(t, err)
	testutil.WriteFileAt(
		t,
		parentPath,
		string(parent)+"  postBuild: {substituteFrom: [{kind: Secret, name: unreadable}]}\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/demo/app/producer.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: unreadable, namespace: apps}\nspec: {target: {name: unreadable}}\n",
	)
	testutil.WriteFile(t, dir, "apps/demo/app/kustomization.yaml", "resources: [release.yaml, producer.yaml]\n")
	releasePath := filepath.Join(dir, "apps/demo/app/release.yaml")
	release, err := os.ReadFile(releasePath)
	assert.NoError(t, err)
	testutil.WriteFileAt(t, releasePath, string(release)+"  values: {greeting: valid}\n")
	original := t.TempDir()
	testutil.CopyTree(t, dir, original)
	for _, root := range []string{dir, original} {
		testutil.GitCommitAll(t, testutil.GitInit(t, root), "init", &gogit.CommitOptions{
			Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
		})
	}
	testutil.WriteFileAt(t, releasePath, string(release)+"  values: {greeting: \"\"}\n")
	for _, mode := range []string{"full", "changed-only"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{
				"build",
				"all",
				"--path",
				filepath.Join(dir, "cluster"),
				"--allow-missing-kinds=Secret,ConfigMap",
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			}
			if mode == "changed-only" {
				args = append(args, "--base", "HEAD")
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			assert.FatalIf(t, code != 1 || !strings.Contains(stderr, "missing required value") ||
				!strings.Contains(
					stderr,
					"apps/demo",
				), "required chart failure: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			assert.FatalIf(t, !strings.Contains(stderr, "[UnresolvedSubstitution]") ||
				!strings.Contains(
					stderr,
					`substituteFrom Secret "unreadable"`,
				), "parent omission warning missing: %s", stderr)
			assert.FatalIf(
				t,
				strings.Contains(stdout, "name: demo-values") || strings.Contains(stdout, "..PLACEHOLDER_"),
				"failed chart published fabricated output: %s",
				stdout,
			)
			assert.FatalIf(
				t,
				strings.Contains(stdout, "name: sibling-values") != (mode == "full"),
				"unchanged sibling selection: mode=%s stdout=%s stderr=%s",
				mode,
				stdout,
				stderr,
			)
		})
	}
	for _, tc := range []struct{ current, original, side string }{
		{dir, original, "current"},
		{original, dir, "orig"},
	} {
		t.Run("diff/"+tc.side, func(t *testing.T) {
			stdout, stderr, code := harness.RunCLIBuffers(
				"diff",
				"all",
				"--path",
				filepath.Join(tc.current, "cluster"),
				"--path-orig",
				filepath.Join(tc.original, "cluster"),
				"-o",
				"json",
				"--allow-missing-kinds=Secret,ConfigMap",
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			assert.FatalIf(t, code != 1 || !strings.Contains(stderr, "missing required value") ||
				!strings.Contains(
					stderr,
					"diff suppressed",
				), "failed-side chart diagnostic: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			assert.FatalIf(t, !strings.Contains(stderr, "[UnresolvedSubstitution]") ||
				!strings.Contains(
					stderr,
					`substituteFrom Secret "unreadable"`,
				), "diff lost parent omission warning: %s", stderr)
			assert.FatalIf(
				t,
				strings.Contains(stdout, "demo-values") || strings.Contains(stdout, "..PLACEHOLDER_"),
				"failed chart leaked into diff: %s",
				stdout,
			)
			var result struct {
				Suppressed []struct {
					Parent struct {
						Kind      string `json:"Kind"`
						Name      string `json:"Name"`
						Namespace string `json:"Namespace"`
					} `json:"Parent"`
					Side  string `json:"Side"`
					Count int    `json:"Count"`
				} `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
			assert.FatalIf(t, len(result.Suppressed) != 1, "missing failed-release suppression: %s", stdout)
			note := result.Suppressed[0]
			assert.FatalIf(
				t,
				note.Parent.Kind != "HelmRelease" || note.Parent.Name != "demo" || note.Parent.Namespace != "apps" ||
					note.Side != tc.side ||
					note.Count != 1,
				"failed-release suppression changed: %s",
				stdout,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MalformedChartFailsProducer(t *testing.T) {
	for _, tc := range []struct{ name, body, symptom string }{
		{"duplicate keys", "null\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: invalid}\ndata:\n  key: first\n  key: duplicate\n", "already defined"},
		{"typed child", "null\n---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: invalid}\nspec:\n  interval: []\n  suspend: true\n  chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: charts}}}\n", "cannot unmarshal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := simpleFixture(t)
			testutil.WriteFile(
				t,
				dir,
				"charts/mychart/templates/invalid.yaml",
				tc.body+"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-sibling}\n",
			)
			args := []string{
				"build",
				"hr",
				"--path",
				filepath.Join(dir, "cluster"),
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			assert.FatalIf(
				t,
				code != 1 || (!strings.Contains(stderr, "HelmRelease") || !strings.Contains(stderr, "apps/demo")) ||
					strings.Count(stderr, tc.symptom) != 1 ||
					!strings.Contains(stdout, "chart-sibling"),
				"exit=%d\n%s\n%s",
				code,
				stdout,
				stderr,
			)
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}
