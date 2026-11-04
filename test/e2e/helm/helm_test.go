// Package helm tests Helm rendering, listing and diagnostics.
package helm_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_GetHR_YAMLExposesProjectedFields(t *testing.T) {
	for _, tc := range []struct{ name, spec, want string }{
		{"target namespace", "  targetNamespace: target\n", "target-demo"},
		{"explicit override", "  targetNamespace: target\n  releaseName: chosen\n", "chosen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := simpleFixture(t)
			file := filepath.Join(dir, "cluster", "helmrelease.yaml")
			body, err := os.ReadFile(file)
			assert.NoError(t, err)
			testutil.WriteFileAt(t, file, string(body)+tc.spec)
			out := harness.RunCLIOutputOnly(t, "get", "hr", "--path", filepath.Join(dir, "cluster"), "-o", "yaml")
			for _, want := range []string{"sourceRef:", "releaseName: " + tc.want} {
				assert.ErrorIf(t, !strings.Contains(out, want), "missing %q in projected hr YAML:\n%s", want, out)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_BuildHR(t *testing.T) {
	dir := simpleFixture(t)
	testutil.WriteFile(t, dir, "charts/mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMapList
items:
- metadata: {name: '{{ .Release.Name }}-cm'}
  data: {greeting: {{ .Values.greeting | quote }}, replicas: "{{ .Values.replicas }}"}
`)

	testutil.WriteFile(t, dir, "charts/mychart/templates/secret.yaml", `apiVersion: v1
kind: Secret
metadata: {name: demo-secret}
stringData: {greeting: hello-from-secret}
`)
	for _, tc := range []struct {
		name, skip string
		wantSecret bool
	}{
		{name: "secret excluded", skip: "Secret"},
		{name: "secret included", wantSecret: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := harness.RunCLIStdout(t, "build", "hr", "--path", filepath.Join(dir, "cluster"),
				"--skip-kinds="+tc.skip, "--concurrency", "2", "--cache-dir", t.TempDir())
			assert.ErrorIf(t, !strings.Contains(out, "demo-cm"), "missing rendered HR ConfigMap:\n%s", out)
			assert.ErrorIf(t, !strings.Contains(out, "greeting: hello-from-helm"), "values not applied:\n%s", out)
			{
				got := strings.Contains(out, "kind: Secret")
				assert.ErrorIf(t, got != tc.wantSecret, "Secret present=%v, want %v:\n%s", got, tc.wantSecret, out)
			}
			if !tc.wantSecret {
				again := harness.RunCLIStdout(t, "build", "hr", "--path", filepath.Join(dir, "cluster"),
					"--skip-kinds="+tc.skip, "--concurrency", "2", "--cache-dir", t.TempDir())
				assert.ErrorIf(
					t,
					out != again,
					"build hr output differs between runs (length %d vs %d)",
					len(out),
					len(again),
				)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MalformedChartCRD(t *testing.T) {
	for _, tc := range []struct {
		name string
		skip string
	}{
		{"omitted", "Secret,CustomResourceDefinition"},
		{"included", "Secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := simpleFixture(t)
			testutil.WriteFile(t, dir, "charts/mychart/crds/bad.yaml", "apiVersion: [\n")
			runCLIExpectErrResult1 := harness.RunCLIExpectErr(
				t,
				"build",
				"all",
				"--path",
				filepath.Join(dir, "cluster"),
				"--skip-kinds="+tc.skip,
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			out, code := runCLIExpectErrResult1.Output, runCLIExpectErrResult1.Code
			assert.FatalIf(t, code != 1, "got exit %d, want 1:\n%s", code, out)
			assert.ErrorIf(
				t,
				!strings.Contains(out, "did not find expected node content"),
				"missing malformed CRD parser error:\n%s",
				out,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ChangedOnlyHRDependsOnUnchangedDep(t *testing.T) {
	initGitFixtureFromResult1 := initGitFixtureFrom(t, harness.TestdataPath(t, "dependson"))
	clusterPath, repoRoot := initGitFixtureFromResult1.path, initGitFixtureFromResult1.root

	// Edit a value in lidarr's HelmRelease so only lidarr is in changed
	// scope; qbittorrent (its dependsOn target) stays unchanged, so its
	// Kustomization is skipped and the qbittorrent HR is never emitted.
	hr := filepath.Join(repoRoot, "apps", "downloads", "lidarr", "app", "helmrelease.yaml")
	harness.MutateFile(t, hr, "hello-from-lidarr", "hello-from-lidarr-edited")

	out := harness.RunCLIOutputOnly(t, "test", "all", "--path", clusterPath, "--base", "HEAD")

	assert.ErrorIf(
		t,
		strings.Contains(out, "dependency not found"),
		"changed-only HR dependsOn on an unchanged dep must not fail:\n%s",
		out,
	)
	{
		lidarr := harness.MustExtractLine(t, out, "downloads/lidarr")
		assert.ErrorIf(
			t,
			!strings.Contains(lidarr, "✓"),
			"lidarr HR should PASS once the unchanged dep is pruned; got: %s",
			lidarr,
		)
	}
	// Confirm the test actually exercises the bug: qbittorrent's KS (the
	// dep's producer) must be skipped, otherwise the dep would render and
	// the assertion above would pass trivially.
	{
		line := harness.MustExtractSkipLine(t, out, "downloads/qbittorrent")
		assert.ErrorIf(t, !strings.Contains(line, "‒"), "expected qbittorrent KS skipped (unchanged); got: %s", line)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ChangedOnlyRenderEmittedDependency(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	repo := testutil.GitInit(t, after)
	_, err := repo.CreateRemote(
		&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/cluster.git"}},
	)
	assert.NoError(t, err)
	testutil.WriteFile(t, after, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
	testutil.WriteFile(t, after, "flux/emitter.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: flux-system}
spec:
  chart:
    spec:
      chart: charts/emitter
      sourceRef: {kind: GitRepository, name: cluster}
`)
	testutil.WriteFile(t, after, "charts/emitter/Chart.yaml", "apiVersion: v2\nname: emitter\nversion: 1.0.0\n")
	testutil.WriteFile(t, after, "charts/emitter/templates/dependency.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: {{ printf "rendered-%s" "dependency" }}
  namespace: flux-system
spec:
  path: ./apps/dependency
  sourceRef: {kind: GitRepository, name: cluster}
`)
	testutil.WriteFile(t, after, "apps/dependency/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, after, "apps/dependency/output.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: dependency-output, namespace: flux-system}
data: {value: stable}
`)
	testutil.WriteFile(
		t,
		after,
		"flux/consumer.yaml",
		testutil.KustomizationYAML("consumer", "flux-system", `  path: ./apps/consumer
  sourceRef: {kind: GitRepository, name: cluster}
  dependsOn: [{name: rendered-dependency}]
  postBuild:
    substitute: {VALUE: before}
`),
	)
	testutil.WriteFile(t, after, "apps/consumer/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, after, "apps/consumer/output.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: consumer-output, namespace: flux-system}
data: {value: '${VALUE}'}
`)
	base := testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	testutil.CopyTree(t, after, before)
	harness.MutateFile(t, filepath.Join(after, "flux", "consumer.yaml"), "VALUE: before", "VALUE: after")

	var snapshots, unchanged [2]string
	for i, root := range []string{before, after} {
		requireCLIOKResult2 := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
			"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
		full, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
		value := []string{"before", "after"}[i]
		assert.FatalIf(
			t,
			!strings.Contains(full, "name: dependency-output") || !strings.Contains(full, "value: stable"),
			"full %s build omitted dependency output:\n%s",
			value,
			full,
		)
		consumer := ""
		var consumerSb204 strings.Builder
		for doc := range strings.SplitSeq(full, "---\n") {
			switch {
			case strings.Contains(doc, "name: consumer-output\n"):

				consumerSb204.WriteString("---\n" + doc)

			case doc != "":

				unchanged[i] += "---\n" + doc
			}
		}
		consumer += consumerSb204.String()
		assert.FatalIf(
			t,
			!strings.Contains(consumer, "value: "+value),
			"full %s build omitted consumer value:\n%s",
			value,
			full,
		)
		snapshots[i] = t.TempDir()
		testutil.GitInit(t, snapshots[i])
		testutil.WriteFile(
			t,
			snapshots[i],
			"flux/consumer.yaml",
			testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps/consumer\n"),
		)
		testutil.WriteFile(t, snapshots[i], "apps/consumer/kustomization.yaml", "resources: [output.yaml]\n")
		testutil.WriteFile(t, snapshots[i], "apps/consumer/output.yaml", consumer)
	}
	assert.Diff(t, unchanged[1], unchanged[0])
	requireCLIOKResult3 := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(snapshots[1], "flux"),
		"--path-orig", filepath.Join(snapshots[0], "flux"), "-o", "diff",
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	fullDiff, _ := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
	assert.FatalIf(
		t,
		!strings.Contains(fullDiff, "-  value: before") || !strings.Contains(fullDiff, "+  value: after"),
		"full-render comparison omitted consumer change:\n%s",
		fullDiff,
	)
	for range 2 {
		requireCLIOKResult4 := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(after, "flux"),
			"--base", base.String(), "-o", "diff", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
		changed, _ := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
		assert.Equal(t, changed, fullDiff)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_HelmSourceRootValuesFiles(t *testing.T) {
	for _, tc := range []struct {
		name, files string
		ignore      bool
	}{
		{"reported strict", "[./charts/jenkins-deployment/values.yaml, ./charts/jenkins-deployment/values.prod.yaml]", false},
		{"reported ignore missing", "[./charts/jenkins-deployment/values.yaml, ./charts/jenkins-deployment/values.prod.yaml]", true},
		{"outside chart", "[./config/prod.yaml]", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := sourceValuesFixture(t, tc.files, tc.ignore, false)
			testutil.GitInit(t, dir)
			args := []string{
				"build", "hr", "--path", filepath.Join(dir, "cluster"),
				"--concurrency", "2", "--cache-dir", t.TempDir(),
			}
			out := harness.RunCLIStdout(t, args...)
			assert.FatalIf(t, !strings.Contains(out, "greeting: production"), "source-root values missing:\n%s", out)
			assert.Equal(t, harness.RunCLIStdout(t, args...), out)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_HelmSourceRootValuesFilesChangedOnly(t *testing.T) {
	for _, chartRef := range []bool{false, true} {
		for _, operation := range []string{"edit", "add", "delete", "strict delete"} {
			t.Run(
				fmt.Sprintf("chartRef=%v/%s", chartRef, operation),
				func(t *testing.T) { e2EHelmSourceRootValuesFilesChangedOnlyCase(t, &operation, &chartRef) },
			)
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_HelmReleaseTargetNamespaceIdentity(t *testing.T) {
	dir := sourceValuesFixture(t, "[]", false, false)
	testutil.WriteFile(t, dir, "charts/jenkins-deployment/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}-values', namespace: '{{ .Release.Namespace }}'}
`)
	before := t.TempDir()
	testutil.CopyTree(t, dir, before)
	for _, root := range []string{dir, before} {
		testutil.GitCommitAll(t, testutil.GitInit(t, root), "init", &gogit.CommitOptions{
			Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
		})
	}
	cache := t.TempDir()
	args := []string{
		"build",
		"all",
		"--path",
		filepath.Join(dir, "cluster"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		cache,
	}
	baseline := harness.RunCLIStdout(t, args...)
	assert.FatalIf(t, !strings.Contains(baseline, "name: demo-values"), "baseline identity missing:\n%s", baseline)
	file := filepath.Join(dir, "apps", "demo", "app", "release.yaml")
	body, err := os.ReadFile(file)
	assert.NoError(t, err)
	testutil.WriteFileAt(t, file, string(body)+"  targetNamespace: target\n")
	for _, changedOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed-only=%t", changedOnly), func(t *testing.T) {
			selected := append([]string(nil), args...)
			if changedOnly {
				selected = append(selected, "--base", "HEAD")
			}
			out := harness.RunCLIStdout(t, selected...)
			for _, want := range []string{"name: target-demo-values", "namespace: target"} {
				assert.FatalIf(t, !strings.Contains(out, want), "missing %q:\n%s", want, out)
			}
			assert.FatalIf(t, strings.Contains(out, "name: demo-values") ||
				strings.Contains(
					out,
					"name: sibling-values",
				) == changedOnly, "incorrect identity or sibling selection:\n%s", out)
			assert.Equal(t, harness.RunCLIStdout(t, selected...), out)
		})
	}
	for _, tc := range []struct{ current, original, added, removed, addedNS, removedNS string }{
		{dir, before, "target-demo-values", "demo-values", "target", "apps"},
		{before, dir, "demo-values", "target-demo-values", "apps", "target"},
	} {
		t.Run("diff/"+tc.added, func(t *testing.T) {
			requireCLIOKResult5 := harness.RequireCLIOK(
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
				cache,
			)
			out, stderr := requireCLIOKResult5.Stdout, requireCLIOKResult5.Stderr
			var result struct {
				Changes []struct {
					Kind      string `json:"Kind"`
					Name      string `json:"Name"`
					Namespace string `json:"Namespace"`
					Status    string `json:"Status"`
					Parent    struct {
						Kind      string `json:"Kind"`
						Name      string `json:"Name"`
						Namespace string `json:"Namespace"`
					} `json:"Parent"`
				} `json:"Changes"`
				Suppressed []any `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(out), &result))
			assert.Equal(t, len(result.Suppressed), 0)
			found := 0
			for _, change := range result.Changes {
				if change.Kind != "ConfigMap" {
					continue
				}
				added := change.Name == tc.added && change.Namespace == tc.addedNS && change.Status == "added"
				removed := change.Name == tc.removed && change.Namespace == tc.removedNS && change.Status == "removed"
				assert.FatalIf(t, !added && !removed, "unexpected identity change: %s\n%s", out, stderr)
				assert.Equal(t, change.Parent.Kind, "HelmRelease")
				assert.Equal(t, change.Parent.Name, "demo")
				assert.Equal(t, change.Parent.Namespace, "apps")
				found++
			}
			assert.Equal(t, found, 2)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_HelmMissingOCISource(t *testing.T) {
	dir := sourceValuesFixture(t, "[]", false, false)
	testutil.WriteFile(t, dir, "apps/demo/app/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  chartRef: {kind: OCIRepository, name: automatic, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		dir,
		"demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: automatic}\n",
	)
	source, err := os.ReadFile(filepath.Join(dir, "cluster", "source.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, dir, "cluster/source.yaml", strings.Replace(string(source),
		"sourceRef: {kind: GitRepository, name: flux-system}",
		"sourceRef: {kind: OCIRepository, name: automatic}", 1))
	testutil.GitCommitAll(t, testutil.GitInit(t, dir), "init", &gogit.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	runCLIExpectErrResult6 := harness.RunCLIExpectErr(t, "build", "all", "--path", filepath.Join(dir, "cluster"),
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	out, code := runCLIExpectErrResult6.Output, runCLIExpectErrResult6.Code
	assert.Equal(t, code, 1)
	assert.FatalIf(t, !strings.Contains(out, "OCIRepository  flux-system/automatic  not found") ||
		strings.Contains(
			out,
			"track oci chart version",
		) || strings.Contains(out, "flux error:"), "missing OCI source failure changed:\n%s", out)
}

func simpleFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "simple"), dir)
	testutil.GitInit(t, dir)
	file := filepath.Join(dir, "cluster", "helmrelease.yaml")
	body, err := os.ReadFile(file)
	assert.NoError(t, err)
	_, release, found := strings.Cut(string(body), "---\n")
	assert.FatalIf(t, !found, "%v", "simple fixture has no HelmRelease document")
	testutil.WriteFileAt(t, file, release)
	return dir
}

func sourceValuesFixture(t *testing.T, files string, ignore, chartRef bool) string {
	t.Helper()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "cluster/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.test/cluster.git
  ref: {branch: main}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", `resources: [demo/ks.yaml, sibling/ks.yaml]
`)
	chart := fmt.Sprintf(`chart: charts/jenkins-deployment
sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
valuesFiles: %s
ignoreMissingValuesFiles: %v
`, files, ignore)
	for _, name := range []string{"demo", "sibling"} {
		chartSpec := chart
		if name == "sibling" {
			chartSpec = strings.ReplaceAll(chartSpec, files, "[]")
		}
		testutil.WriteFile(t, dir, "apps/"+name+"/ks.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s, namespace: apps}
spec:
  interval: 10m
  path: ./apps/%s/app
  targetNamespace: apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`, name, name))
		selection := "  chart:\n    spec:\n      " + strings.ReplaceAll(
			strings.TrimSpace(chartSpec),
			"\n",
			"\n      ",
		) + "\n"
		if chartRef {
			selection = "  chartRef: {kind: HelmChart, name: " + name + ", namespace: flux-system}\n"
		}
		body := fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s, namespace: apps}
spec:
  interval: 10m
%s`, name, selection)
		if chartRef {
			testutil.WriteFile(
				t,
				dir,
				"cluster/"+name+"-chart.yaml",
				fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: %s, namespace: flux-system}
spec:
  interval: 10m
  %s
`, name, strings.ReplaceAll(strings.TrimSpace(chartSpec), "\n", "\n  ")),
			)
		}
		testutil.WriteFile(t, dir, "apps/"+name+"/app/release.yaml", body)
		testutil.WriteFile(t, dir, "apps/"+name+"/app/kustomization.yaml", "resources: [release.yaml]\n")
	}
	testutil.WriteFile(t, dir, "charts/jenkins-deployment/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(t, dir, "charts/jenkins-deployment/values.yaml", "greeting: default\n")
	testutil.WriteFile(t, dir, "charts/jenkins-deployment/values.prod.yaml", "greeting: production\n")
	testutil.WriteFile(t, dir, "config/prod.yaml", "greeting: production\n")
	testutil.WriteFile(t, dir, "charts/jenkins-deployment/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-values
data:
  greeting: {{ .Values.greeting }}
`)
	return dir
}

type gitFixture struct {
	path string
	root string
}

func initGitFixtureFrom(t *testing.T, src string) gitFixture {
	t.Helper()
	dir := t.TempDir()
	testutil.CopyTree(t, src, dir)
	repo := testutil.GitInit(t, dir)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	return gitFixture{path: filepath.Join(dir, "flux"), root: dir}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ReadyExpr_HelmRelease(t *testing.T) {
	dir := simpleFixture(t)
	for _, name := range []string{"dependency", "consumer"} {
		dependsOn := ""
		if name == "consumer" {
			dependsOn = `  dependsOn:
  - name: dependency
    readyExpr: dep.spec.values.releaseVersion == self.spec.values.releaseVersion && dep.spec.values.nested.replicas == 2 && dep.metadata.generation == dep.status.observedGeneration
`
		}
		testutil.WriteFile(t, dir, "cluster/"+name+".yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s, namespace: default}
spec:
  chart:
    spec:
      chart: charts/mychart
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
%s  values: {greeting: desired-spec, releaseVersion: v1, nested: {replicas: 2}}
`, name, dependsOn))
	}
	testutil.WriteFile(t, dir, "charts/mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}-output'}
data: {greeting: '{{ .Values.greeting }}', version: '{{ .Values.releaseVersion }}'}
`)
	var previousOut, previousErr string
	for run := range 2 {
		requireCLIOKResult7 := harness.RequireCLIOK(
			t,
			"build",
			"all",
			"--path",
			filepath.Join(dir, "cluster"),
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
			"--no-progress",
			"--log-level",
			"warn",
		)
		stdout, stderr := requireCLIOKResult7.Stdout, requireCLIOKResult7.Stderr
		assert.FatalIf(
			t,
			!strings.Contains(stdout, "name: consumer-output") || !strings.Contains(stdout, "greeting: desired-spec") ||
				!strings.Contains(stdout, "version: v1") ||
				strings.Contains(stderr, "readyExpr"),
			"Helm desired-spec readiness: stdout=%s stderr=%s",
			stdout,
			stderr,
		)
		assert.FatalIf(
			t,
			run == 1 && (stdout != previousOut || stderr != previousErr),
			"Helm output or warnings changed: stdout=%s stderr=%s",
			stdout,
			stderr,
		)
		previousOut, previousErr = stdout, stderr
	}
}
