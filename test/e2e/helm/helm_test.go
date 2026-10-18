// Package helm tests Helm rendering, listing and diagnostics.
package helm

import (
	"path/filepath"
	"strings"
	"testing"

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
	dir := harness.CopyTree(t, harness.TestdataPath(t, "simple"))
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
			dir := harness.CopyTree(t, harness.TestdataPath(t, "simple"))
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

func initGitFixtureFrom(t *testing.T, src string) (clusterPath, repoRoot string) {
	t.Helper()
	dir := harness.CopyTree(t, src)
	repo := harness.GitInit(t, dir)
	harness.GitCommitAll(t, repo)
	return filepath.Join(dir, "flux"), dir
}
