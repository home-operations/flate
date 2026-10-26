// Package helm tests Helm rendering, listing and diagnostics.
package helm

import (
	"fmt"
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

func TestE2E_GetHR_YAMLExposesProjectedFields(t *testing.T) {
	out := harness.RunCLIOutputOnly(t, "get", "hr", "--path", harness.TestdataPath(t, "simple"), "-o", "yaml")
	// HelmRelease projection should carry sourceRef (chart's resolved
	// ref) and releaseName.
	for _, want := range []string{"sourceRef:", "releaseName:"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in projected hr YAML:\n%s", want, out)
		}
	}
}

func TestE2E_BuildHR(t *testing.T) {
	dir := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "simple"), dir)
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
			out := harness.RunCLIStdout(t, "build", "hr", "--path", dir,
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
				again := harness.RunCLIStdout(t, "build", "hr", "--path", dir,
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
			dir := t.TempDir()
			testutil.CopyTree(t, harness.TestdataPath(t, "simple"), dir)
			testutil.WriteFile(t, dir, "charts/mychart/crds/bad.yaml", "apiVersion: [\n")
			out, code := harness.RunCLIExpectErr(t, "build", "all", "--path", dir,
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
			args := []string{"build", "hr", "--path", dir,
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
				for _, kind := range []string{"Kustomization.", "HelmRelease."} {
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
	dir := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "simple"), dir)
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
		stdout, stderr := harness.RequireCLIOK(t, "build", "all", "--path", dir, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress", "--log-level", "warn")
		if !strings.Contains(stdout, "name: consumer-output") || !strings.Contains(stdout, "greeting: desired-spec") || !strings.Contains(stdout, "version: v1") || strings.Contains(stderr, "readyExpr") {
			t.Fatalf("Helm desired-spec readiness: stdout=%s stderr=%s", stdout, stderr)
		}
		if run == 1 && (stdout != previousOut || stderr != previousErr) {
			t.Fatalf("Helm output or warnings changed: stdout=%s stderr=%s", stdout, stderr)
		}
		previousOut, previousErr = stdout, stderr
	}
}
