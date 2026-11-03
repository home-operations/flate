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
func TestE2E_InvalidHelmList(t *testing.T) {
	dir := simpleFixture(t)
	testutil.WriteFile(
		t,
		dir,
		"charts/mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, metadata: {name: invalid}}]\n",
	)
	for _, tc := range []struct {
		name string
		skip bool
	}{
		{name: "included"},
		{name: "excluded", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{
				"build",
				"hr",
				"--path",
				filepath.Join(dir, "cluster"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			}
			if tc.skip {
				args = append(args, "--skip-kinds=ConfigMapList")
			}
			out, stderr, code := harness.RunCLIBuffers(args...)
			diagnostic := harness.MustExtractLine(t, stderr, "Object 'Kind' is missing")
			assert.FatalIf(t, code != 1 || strings.Count(stderr, "Object 'Kind' is missing") != 1 ||
				strings.Count(
					diagnostic,
					"apps/demo",
				) != 1, "invalid list exit=%d stdout=%s stderr=%s", code, out, stderr)
			for _, text := range []string{"name: invalid", "kind: ConfigMapList"} {
				{
					got := strings.Contains(out, text)
					assert.ErrorIf(t, got != !tc.skip, "output contains %q=%v, want %v:\n%s", text, got, !tc.skip, out)
				}
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_HelmListMemberDiff(t *testing.T) {
	before, after := simpleFixture(t), t.TempDir()
	body := `apiVersion: v1
kind: ConfigMapList
items:
- metadata: {name: stable}
  data: {value: fixed}
`
	testutil.WriteFile(t, before, "charts/mychart/templates/configmap.yaml", body)
	testutil.CopyTree(t, before, after)
	testutil.WriteFile(
		t,
		after,
		"charts/mychart/templates/configmap.yaml",
		body+"- metadata: {name: added}\n  data: {value: extra}\n",
	)
	for _, tc := range []struct{ current, original, status string }{{after, before, "added"}, {before, after, "removed"}} {
		t.Run(tc.status, func(t *testing.T) {
			out, stderr := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"-o",
				"json",
				"--path",
				filepath.Join(tc.current, "cluster"),
				"--path-orig",
				filepath.Join(tc.original, "cluster"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			var result struct {
				Changes []struct {
					Name   string `json:"Name"`
					Status string `json:"Status"`
					Parent struct {
						Kind      string `json:"Kind"`
						Name      string `json:"Name"`
						Namespace string `json:"Namespace"`
					} `json:"Parent"`
				} `json:"Changes"`
				Suppressed []any `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(out), &result))
			assert.FatalIf(
				t,
				len(result.Changes) != 1 || len(result.Suppressed) != 0,
				"list diff = %s\n%s",
				out,
				stderr,
			)
			change := result.Changes[0]
			assert.FatalIf(
				t,
				change.Name != "added" || change.Status != tc.status || change.Parent.Kind != "HelmRelease" ||
					change.Parent.Name != "demo" ||
					change.Parent.Namespace != "apps",
				"list member lost pairing/parent: %s",
				out,
			)
		})
	}
}

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

func e2EHelmSourceRootValuesFilesChangedOnlyCase(t *testing.T, operation *string, chartRef *bool) {
	t.Helper()

	dir := sourceValuesFixture(t, "[./config/prod.yaml]", (*operation) != "strict delete", (*chartRef))
	valuesPath := filepath.Join(dir, "config", "prod.yaml")
	if (*operation) == "add" {
		assert.NoError(t, os.Remove(valuesPath))
	}
	repo := testutil.GitInit(t, dir)
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	want := "production"
	switch *operation {
	case "edit":
		testutil.WriteFile(t, dir, "config/prod.yaml", "greeting: edited\n")
		want = "edited"
	case "add":
		testutil.WriteFile(t, dir, "config/prod.yaml", "greeting: production\n")
	case "delete", "strict delete":
		assert.NoError(t, os.Remove(valuesPath))
		want = "default"
	}
	args := []string{
		"build", "all", "--path", filepath.Join(dir, "cluster"), "--base", "HEAD",
		"--concurrency", "2", "--cache-dir", t.TempDir(),
	}
	if (*operation) == "strict delete" {
		out, code := harness.RunCLIExpectErr(t, args...)
		assert.Equal(t, code, 1)
		assert.FatalIf(
			t,
			!strings.Contains(out, "config/prod.yaml"),
			"missing selected-file failure:\n%s",
			out,
		)
		return
	}
	out := harness.RunCLIStdout(t, args...)
	assert.FatalIf(
		t,
		!strings.Contains(out, "name: demo-values") || !strings.Contains(out, "greeting: "+want),
		"selected release did not render %q:\n%s",
		want,
		out,
	)
	assert.FatalIf(t, strings.Contains(out, "name: sibling-values"), "unchanged sibling rendered:\n%s", out)
	assert.Equal(t, harness.RunCLIStdout(t, args...), out)
	args[0] = "test"
	status := harness.RunCLI(t, args...)
	for _, kind := range []string{"Kustomization", "HelmRelease"} {
		found := false
		for line := range strings.SplitSeq(status, "\n") {
			if strings.Contains(line, kind) && strings.Contains(line, "apps/demo") &&
				strings.Contains(line, "✓") {
				found = true
			}
		}
		assert.ErrorIf(t, !found, "selected %s did not pass:\n%s", kind, status)
	}
	harness.MustExtractSkipLine(t, status, "apps/sibling")
}
