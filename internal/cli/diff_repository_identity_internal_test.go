package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func runDiffKSPathOrigRepositoryIdentityName(t *testing.T, tc *struct {
	name     string
	linked   bool
	nested   bool
	matching bool
},
) {
	t.Helper()
	const currentURL = "ssh://git@github.com/example/home-ops.git"
	const baselineURL = "https://example.invalid/upstream.git"

	current, orig := writeCustomSourceDiffFixture(t, currentURL)
	writeBaselineGit(t, current, currentURL)
	url := baselineURL
	if tc.matching {
		url = currentURL
	}
	cluster, err := os.ReadFile(filepath.Join(orig, "flux", "cluster.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, orig, "flux/cluster.yaml", strings.ReplaceAll(string(cluster), currentURL, url))
	if tc.linked {
		common := t.TempDir()
		writeBaselineGit(t, common, url)
		writeBaselineWorktree(t, orig, common)
	} else {
		writeBaselineGit(t, orig, url)
	}
	for _, app := range []string{"apps", "second-apps"} {
		name := "hello"
		if app == "second-apps" {
			name = "second"
		}
		testutil.WriteFile(
			t,
			orig,
			app+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {greeting: hola}\n",
		)
	}
	for _, dir := range []string{current, orig} {
		testutil.WriteFile(
			t,
			dir,
			"flux/kustomization.yaml",
			"resources: [cluster.yaml, second.yaml, external.yaml]\n",
		)
		testutil.WriteFile(t, dir, "flux/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: unrelated, namespace: beta}
spec: {url: "https://example.invalid/unrelated.git", interval: 1h}
`+"---\n"+
			testutil.KustomizationYAML("external-apps", "beta", `  path: ./external
  sourceRef: {kind: GitRepository, name: unrelated}
  interval: 1h
`))
		testutil.WriteFile(t, dir, "external/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(
			t,
			dir,
			"external/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: must-not-render}\n",
		)
	}
	currentScan, origScan := current, orig
	if tc.nested {
		currentScan, origScan = filepath.Join(current, "flux"), filepath.Join(orig, "flux")
	}
	c := commonFlags{path: currentScan, pathOrig: origScan}
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err = resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(c.pathOrigSelfURLs) != 1 || c.pathOrigSelfURLs[0] != url,
		"baseline identity = %v, want [%s]; stopping before render to prevent an external fetch",
		c.pathOrigSelfURLs,
		url,
	)
	args := []string{
		"diff", "ks", "--path", currentScan, "--path-orig", origScan,
		"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff",
	}
	stdout, stderr, code := runCLI(t, args...)
	assert.FatalIf(t, code != 0, "diff exited %d: %s", code, stderr)
	for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
		{
			count := strings.Count(stdout, want)
			assert.ErrorIf(t, count != 2, "field diff %q occurred %d times, want 2:\n%s", want, count, stdout)
		}
	}
	for _, unwanted := range []string{"must-not-render", "+apiVersion:", "-apiVersion:", "suppressed"} {
		assert.ErrorIf(
			t,
			strings.Contains(stdout, unwanted),
			"unexpected %q in field diff:\n%s",
			unwanted,
			stdout,
		)
	}
	assert.ErrorIf(
		t,
		strings.Count(stderr, "external source") != 2 || strings.Contains(stderr, "home-kubernetes"),
		"want only unrelated external-source disclosures: %s",
		stderr,
	)
	if tc.name == "different origins" {
		secondOut, secondErr, secondCode := runCLI(t, args...)
		assert.ErrorIf(
			t,
			secondOut != stdout || secondErr != stderr || secondCode != code,
			"repeated diff changed output or exit code:\n%s\n%s",
			secondOut,
			secondErr,
		)
	}
}
