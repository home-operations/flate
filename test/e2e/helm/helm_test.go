// Package helm tests Helm rendering, listing and diagnostics.
package helm

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
				if !strings.Contains(out, want) {
					t.Errorf("missing %q in projected hr YAML:\n%s", want, out)
				}
			}
		})
	}
}

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
			if !strings.Contains(out, "demo-cm") {
				t.Errorf("missing rendered HR ConfigMap:\n%s", out)
			}
			if !strings.Contains(out, "greeting: hello-from-helm") {
				t.Errorf("values not applied:\n%s", out)
			}
			if got := strings.Contains(out, "kind: Secret"); got != tc.wantSecret {
				t.Errorf("Secret present=%v, want %v:\n%s", got, tc.wantSecret, out)
			}
			if !tc.wantSecret {
				again := harness.RunCLIStdout(t, "build", "hr", "--path", filepath.Join(dir, "cluster"),
					"--skip-kinds="+tc.skip, "--concurrency", "2", "--cache-dir", t.TempDir())
				if out != again {
					t.Errorf("build hr output differs between runs (length %d vs %d)", len(out), len(again))
				}
			}
		})
	}
}

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
			out, code := harness.RunCLIExpectErr(t, "build", "all", "--path", filepath.Join(dir, "cluster"),
				"--skip-kinds="+tc.skip, "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if code != 1 {
				t.Fatalf("got exit %d, want 1:\n%s", code, out)
			}
			if !strings.Contains(out, "did not find expected node content") {
				t.Errorf("missing malformed CRD parser error:\n%s", out)
			}
		})
	}
}

func TestE2E_ChangedOnlyHRDependsOnUnchangedDep(t *testing.T) {
	clusterPath, repoRoot := initGitFixtureFrom(t, harness.TestdataPath(t, "dependson"))

	// Edit a value in lidarr's HelmRelease so only lidarr is in changed
	// scope; qbittorrent (its dependsOn target) stays unchanged, so its
	// Kustomization is skipped and the qbittorrent HR is never emitted.
	hr := filepath.Join(repoRoot, "apps", "downloads", "lidarr", "app", "helmrelease.yaml")
	harness.MutateFile(t, hr, "hello-from-lidarr", "hello-from-lidarr-edited")

	out := harness.RunCLIOutputOnly(t, "test", "all", "--path", clusterPath, "--base", "HEAD")

	if strings.Contains(out, "dependency not found") {
		t.Errorf("changed-only HR dependsOn on an unchanged dep must not fail:\n%s", out)
	}
	if lidarr := harness.MustExtractLine(t, out, "downloads/lidarr"); !strings.Contains(lidarr, "✓") {
		t.Errorf("lidarr HR should PASS once the unchanged dep is pruned; got: %s", lidarr)
	}
	// Confirm the test actually exercises the bug: qbittorrent's KS (the
	// dep's producer) must be skipped, otherwise the dep would render and
	// the assertion above would pass trivially.
	if line := harness.MustExtractSkipLine(t, out, "downloads/qbittorrent"); !strings.Contains(line, "‒") {
		t.Errorf("expected qbittorrent KS skipped (unchanged); got: %s", line)
	}
}

func TestE2E_ChangedOnlyRenderEmittedDependency(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	repo := testutil.GitInit(t, after)
	_, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/cluster.git"}})
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
	testutil.WriteFile(t, after, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system", `  path: ./apps/consumer
  sourceRef: {kind: GitRepository, name: cluster}
  dependsOn: [{name: rendered-dependency}]
  postBuild:
    substitute: {VALUE: before}
`))
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
	harness.MutateFile(t, filepath.Join(after, "flux/consumer.yaml"), "VALUE: before", "VALUE: after")

	var snapshots, unchanged [2]string
	for i, root := range []string{before, after} {
		full, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
			"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
		value := []string{"before", "after"}[i]
		if !strings.Contains(full, "name: dependency-output") || !strings.Contains(full, "value: stable") {
			t.Fatalf("full %s build omitted dependency output:\n%s", value, full)
		}
		consumer := ""
		for doc := range strings.SplitSeq(full, "---\n") {
			if strings.Contains(doc, "name: consumer-output\n") {
				consumer += "---\n" + doc
			} else if doc != "" {
				unchanged[i] += "---\n" + doc
			}
		}
		if !strings.Contains(consumer, "value: "+value) {
			t.Fatalf("full %s build omitted consumer value:\n%s", value, full)
		}
		snapshots[i] = t.TempDir()
		testutil.GitInit(t, snapshots[i])
		testutil.WriteFile(t, snapshots[i], "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps/consumer\n"))
		testutil.WriteFile(t, snapshots[i], "apps/consumer/kustomization.yaml", "resources: [output.yaml]\n")
		testutil.WriteFile(t, snapshots[i], "apps/consumer/output.yaml", consumer)
	}
	assert.Diff(t, unchanged[1], unchanged[0])
	fullDiff, _ := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(snapshots[1], "flux"),
		"--path-orig", filepath.Join(snapshots[0], "flux"), "-o", "diff",
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	if !strings.Contains(fullDiff, "-  value: before") || !strings.Contains(fullDiff, "+  value: after") {
		t.Fatalf("full-render comparison omitted consumer change:\n%s", fullDiff)
	}
	for range 2 {
		changed, _ := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(after, "flux"),
			"--base", base.String(), "-o", "diff", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
		assert.Equal(t, changed, fullDiff)
	}
}

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
			args := []string{"build", "hr", "--path", filepath.Join(dir, "cluster"),
				"--concurrency", "2", "--cache-dir", t.TempDir()}
			out := harness.RunCLIStdout(t, args...)
			if !strings.Contains(out, "greeting: production") {
				t.Fatalf("source-root values missing:\n%s", out)
			}
			assert.Equal(t, harness.RunCLIStdout(t, args...), out)
		})
	}
}

func TestE2E_HelmSourceRootValuesFilesChangedOnly(t *testing.T) {
	for _, chartRef := range []bool{false, true} {
		for _, operation := range []string{"edit", "add", "delete", "strict delete"} {
			t.Run(fmt.Sprintf("chartRef=%v/%s", chartRef, operation), func(t *testing.T) {
				dir := sourceValuesFixture(t, "[./config/prod.yaml]", operation != "strict delete", chartRef)
				valuesPath := filepath.Join(dir, "config", "prod.yaml")
				if operation == "add" {
					assert.NoError(t, os.Remove(valuesPath))
				}
				repo := testutil.GitInit(t, dir)
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{
					Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
				})
				want := "production"
				switch operation {
				case "edit":
					testutil.WriteFile(t, dir, "config/prod.yaml", "greeting: edited\n")
					want = "edited"
				case "add":
					testutil.WriteFile(t, dir, "config/prod.yaml", "greeting: production\n")
				case "delete", "strict delete":
					assert.NoError(t, os.Remove(valuesPath))
					want = "default"
				}
				args := []string{"build", "all", "--path", filepath.Join(dir, "cluster"), "--base", "HEAD",
					"--concurrency", "2", "--cache-dir", t.TempDir()}
				if operation == "strict delete" {
					out, code := harness.RunCLIExpectErr(t, args...)
					assert.Equal(t, code, 1)
					if !strings.Contains(out, "config/prod.yaml") {
						t.Fatalf("missing selected-file failure:\n%s", out)
					}
					return
				}
				out := harness.RunCLIStdout(t, args...)
				if !strings.Contains(out, "name: demo-values") || !strings.Contains(out, "greeting: "+want) {
					t.Fatalf("selected release did not render %q:\n%s", want, out)
				}
				if strings.Contains(out, "name: sibling-values") {
					t.Fatalf("unchanged sibling rendered:\n%s", out)
				}
				assert.Equal(t, harness.RunCLIStdout(t, args...), out)
				args[0] = "test"
				status := harness.RunCLI(t, args...)
				for _, kind := range []string{"Kustomization", "HelmRelease"} {
					found := false
					for line := range strings.SplitSeq(status, "\n") {
						if strings.Contains(line, kind) && strings.Contains(line, "apps/demo") && strings.Contains(line, "✓") {
							found = true
						}
					}
					if !found {
						t.Errorf("selected %s did not pass:\n%s", kind, status)
					}
				}
				harness.MustExtractSkipLine(t, status, "apps/sibling")
			})
		}
	}
}

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
	args := []string{"build", "all", "--path", filepath.Join(dir, "cluster"), "--concurrency", "2", "--no-progress", "--cache-dir", cache}
	baseline := harness.RunCLIStdout(t, args...)
	if !strings.Contains(baseline, "name: demo-values") {
		t.Fatalf("baseline identity missing:\n%s", baseline)
	}
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
				if !strings.Contains(out, want) {
					t.Fatalf("missing %q:\n%s", want, out)
				}
			}
			if strings.Contains(out, "name: demo-values") || strings.Contains(out, "name: sibling-values") == changedOnly {
				t.Fatalf("incorrect identity or sibling selection:\n%s", out)
			}
			assert.Equal(t, harness.RunCLIStdout(t, selected...), out)
		})
	}
	for _, tc := range []struct{ current, original, added, removed, addedNS, removedNS string }{
		{dir, before, "target-demo-values", "demo-values", "target", "apps"},
		{before, dir, "demo-values", "target-demo-values", "apps", "target"},
	} {
		t.Run("diff/"+tc.added, func(t *testing.T) {
			out, stderr := harness.RequireCLIOK(t, "diff", "all", "-o", "json", "--path", filepath.Join(tc.current, "cluster"), "--path-orig", filepath.Join(tc.original, "cluster"), "--concurrency", "2", "--no-progress", "--cache-dir", cache)
			var result struct {
				Changes []struct {
					Kind, Name, Namespace, Status string
					Parent                        struct{ Kind, Name, Namespace string }
				}
				Suppressed []any
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
				if !added && !removed {
					t.Fatalf("unexpected identity change: %s\n%s", out, stderr)
				}
				assert.Equal(t, change.Parent.Kind, "HelmRelease")
				assert.Equal(t, change.Parent.Name, "demo")
				assert.Equal(t, change.Parent.Namespace, "apps")
				found++
			}
			assert.Equal(t, found, 2)
		})
	}
}

func TestE2E_HelmAutomaticAliasRevision(t *testing.T) {
	dir := sourceValuesFixture(t, "[]", false, false)
	testutil.WriteFile(t, dir, "apps/demo/app/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  chartRef: {kind: OCIRepository, name: automatic, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(t, dir, "demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: automatic}\n")
	source, err := os.ReadFile(filepath.Join(dir, "cluster", "source.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, dir, "cluster/source.yaml", strings.Replace(string(source),
		"sourceRef: {kind: GitRepository, name: flux-system}",
		"sourceRef: {kind: OCIRepository, name: automatic}", 1))
	testutil.GitCommitAll(t, testutil.GitInit(t, dir), "init", &gogit.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	out, code := harness.RunCLIExpectErr(t, "build", "all", "--path", filepath.Join(dir, "cluster"),
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	assert.Equal(t, code, 1)
	if !strings.Contains(out, `track oci chart version: invalid artifact revision ""`) ||
		strings.Count(out, "HelmRelease  apps/demo  ") != 1 || strings.Contains(out, "flux error:") {
		t.Fatalf("alias revision failure changed:\n%s", out)
	}
}

func simpleFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "simple"), dir)
	testutil.GitInit(t, dir)
	file := filepath.Join(dir, "cluster/helmrelease.yaml")
	body, err := os.ReadFile(file)
	assert.NoError(t, err)
	_, release, found := strings.Cut(string(body), "---\n")
	if !found {
		t.Fatal("simple fixture has no HelmRelease document")
	}
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
		selection := "  chart:\n    spec:\n      " + strings.ReplaceAll(strings.TrimSpace(chartSpec), "\n", "\n      ") + "\n"
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
			testutil.WriteFile(t, dir, "cluster/"+name+"-chart.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: %s, namespace: flux-system}
spec:
  interval: 10m
  %s
`, name, strings.ReplaceAll(strings.TrimSpace(chartSpec), "\n", "\n  ")))
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

func initGitFixtureFrom(t *testing.T, src string) (clusterPath, repoRoot string) {
	t.Helper()
	dir := t.TempDir()
	testutil.CopyTree(t, src, dir)
	repo := testutil.GitInit(t, dir)
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
	return filepath.Join(dir, "flux"), dir
}

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
		stdout, stderr := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(dir, "cluster"), "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress", "--log-level", "warn")
		if !strings.Contains(stdout, "name: consumer-output") || !strings.Contains(stdout, "greeting: desired-spec") || !strings.Contains(stdout, "version: v1") || strings.Contains(stderr, "readyExpr") {
			t.Fatalf("Helm desired-spec readiness: stdout=%s stderr=%s", stdout, stderr)
		}
		if run == 1 && (stdout != previousOut || stderr != previousErr) {
			t.Fatalf("Helm output or warnings changed: stdout=%s stderr=%s", stdout, stderr)
		}
		previousOut, previousErr = stdout, stderr
	}
}

func TestE2E_InvalidHelmList(t *testing.T) {
	dir := simpleFixture(t)
	testutil.WriteFile(t, dir, "charts/mychart/templates/configmap.yaml", "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, metadata: {name: invalid}}]\n")
	for _, tc := range []struct {
		name string
		skip bool
	}{
		{name: "included"},
		{name: "excluded", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"build", "hr", "--path", filepath.Join(dir, "cluster"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
			if tc.skip {
				args = append(args, "--skip-kinds=ConfigMapList")
			}
			out, stderr, code := harness.RunCLIBuffers(args...)
			diagnostic := harness.MustExtractLine(t, stderr, "Object 'Kind' is missing")
			if code != 1 || strings.Count(stderr, "Object 'Kind' is missing") != 1 || strings.Count(diagnostic, "apps/demo") != 1 {
				t.Fatalf("invalid list exit=%d stdout=%s stderr=%s", code, out, stderr)
			}
			for _, text := range []string{"name: invalid", "kind: ConfigMapList"} {
				if got := strings.Contains(out, text); got != !tc.skip {
					t.Errorf("output contains %q=%v, want %v:\n%s", text, got, !tc.skip, out)
				}
			}
		})
	}
}

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
	testutil.WriteFile(t, after, "charts/mychart/templates/configmap.yaml", body+"- metadata: {name: added}\n  data: {value: extra}\n")
	for _, tc := range []struct{ current, original, status string }{{after, before, "added"}, {before, after, "removed"}} {
		t.Run(tc.status, func(t *testing.T) {
			out, stderr := harness.RequireCLIOK(t, "diff", "all", "-o", "json", "--path", filepath.Join(tc.current, "cluster"), "--path-orig", filepath.Join(tc.original, "cluster"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			var result struct {
				Changes []struct {
					Name, Status string
					Parent       struct{ Kind, Name, Namespace string }
				}
				Suppressed []any
			}
			if err := json.Unmarshal([]byte(out), &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Changes) != 1 || len(result.Suppressed) != 0 {
				t.Fatalf("list diff = %s\n%s", out, stderr)
			}
			change := result.Changes[0]
			if change.Name != "added" || change.Status != tc.status || change.Parent.Kind != "HelmRelease" || change.Parent.Name != "demo" || change.Parent.Namespace != "apps" {
				t.Fatalf("list member lost pairing/parent: %s", out)
			}
		})
	}
}

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
	testutil.WriteFileAt(t, parentPath, string(parent)+"  postBuild: {substituteFrom: [{kind: Secret, name: unreadable}]}\n")
	testutil.WriteFile(t, dir, "apps/demo/app/producer.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: unreadable, namespace: apps}\nspec: {target: {name: unreadable}}\n")
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
			args := []string{"build", "all", "--path", filepath.Join(dir, "cluster"),
				"--allow-missing-kinds=Secret,ConfigMap", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
			if mode == "changed-only" {
				args = append(args, "--base", "HEAD")
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			if code != 1 || !strings.Contains(stderr, "missing required value") || !strings.Contains(stderr, "apps/demo") {
				t.Fatalf("required chart failure: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if !strings.Contains(stderr, "[UnresolvedSubstitution]") || !strings.Contains(stderr, `substituteFrom Secret "unreadable"`) {
				t.Fatalf("parent omission warning missing: %s", stderr)
			}
			if strings.Contains(stdout, "name: demo-values") || strings.Contains(stdout, "..PLACEHOLDER_") {
				t.Fatalf("failed chart published fabricated output: %s", stdout)
			}
			if strings.Contains(stdout, "name: sibling-values") != (mode == "full") {
				t.Fatalf("unchanged sibling selection: mode=%s stdout=%s stderr=%s", mode, stdout, stderr)
			}
		})
	}
	for _, tc := range []struct{ current, original, side string }{
		{dir, original, "current"},
		{original, dir, "orig"},
	} {
		t.Run("diff/"+tc.side, func(t *testing.T) {
			stdout, stderr, code := harness.RunCLIBuffers("diff", "all", "--path", filepath.Join(tc.current, "cluster"),
				"--path-orig", filepath.Join(tc.original, "cluster"), "-o", "json", "--allow-missing-kinds=Secret,ConfigMap",
				"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if code != 1 || !strings.Contains(stderr, "missing required value") || !strings.Contains(stderr, "diff suppressed") {
				t.Fatalf("failed-side chart diagnostic: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if !strings.Contains(stderr, "[UnresolvedSubstitution]") || !strings.Contains(stderr, `substituteFrom Secret "unreadable"`) {
				t.Fatalf("diff lost parent omission warning: %s", stderr)
			}
			if strings.Contains(stdout, "demo-values") || strings.Contains(stdout, "..PLACEHOLDER_") {
				t.Fatalf("failed chart leaked into diff: %s", stdout)
			}
			var result struct {
				Suppressed []struct {
					Parent struct{ Kind, Name, Namespace string }
					Side   string
					Count  int
				}
			}
			assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
			if len(result.Suppressed) != 1 {
				t.Fatalf("missing failed-release suppression: %s", stdout)
			}
			note := result.Suppressed[0]
			if note.Parent.Kind != "HelmRelease" || note.Parent.Name != "demo" || note.Parent.Namespace != "apps" || note.Side != tc.side || note.Count != 1 {
				t.Fatalf("failed-release suppression changed: %s", stdout)
			}
		})
	}
}

func TestE2E_MalformedChartFailsProducer(t *testing.T) {
	for _, tc := range []struct{ name, body, symptom string }{
		{"duplicate keys", "null\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: invalid}\ndata:\n  key: first\n  key: duplicate\n", "already defined"},
		{"typed child", "null\n---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: invalid}\nspec:\n  interval: []\n  suspend: true\n  chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: charts}}}\n", "cannot unmarshal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := simpleFixture(t)
			testutil.WriteFile(t, dir, "charts/mychart/templates/invalid.yaml", tc.body+"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-sibling}\n")
			args := []string{"build", "hr", "--path", filepath.Join(dir, "cluster"), "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir()}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			if code != 1 || (!strings.Contains(stderr, "HelmRelease") || !strings.Contains(stderr, "apps/demo")) || strings.Count(stderr, tc.symptom) != 1 || !strings.Contains(stdout, "chart-sibling") {
				t.Fatalf("exit=%d\n%s\n%s", code, stdout, stderr)
			}
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}
