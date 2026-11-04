package scope_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_Diff_MalformedManifestTransitions(t *testing.T) {
	for _, tc := range []struct{ name, side string }{
		{name: "malformed to valid", side: "orig"},
		{name: "valid to malformed", side: "current"},
	} {
		t.Run(tc.name, func(t *testing.T) { e2EDiffMalformedManifestTransitionsName(t, &tc) })
	}
}

func e2EDiffMalformedManifestTransitionsName(t *testing.T, tc *struct {
	name string
	side string
},
) {
	t.Helper()

	orig, current := t.TempDir(), t.TempDir()
	for _, dir := range []string{orig, current} {
		testutil.GitInit(t, dir)
		testutil.WriteFile(
			t,
			dir,
			"flux/ks.yaml",
			testutil.KustomizationYAML(
				"owner",
				"flux-system",
				"  path: ./app\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			)+"---\n"+testutil.KustomizationYAML(
				"healthy",
				"flux-system",
				"  path: ./healthy\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
		testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources: [child.yaml]\n")
		interval := "10m"
		if tc.side == "orig" && dir == orig || tc.side == "current" && dir == current {
			interval = "[]"
		}
		testutil.WriteFile(
			t,
			dir,
			"app/child.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: child, namespace: flux-system}\nspec: {interval: "+interval+", suspend: true, chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system}}}}\n",
		)
		testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
		value := "old"
		if dir == current {
			value = "new"
		}
		testutil.WriteFile(
			t,
			dir,
			"healthy/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent-output}\ndata: {value: "+value+"}\n",
		)
	}
	cache := t.TempDir()
	args := []string{
		"diff",
		"all",
		"--path",
		filepath.Join(current, "flux"),
		"--path-orig",
		filepath.Join(orig, "flux"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		cache,
		"-o",
		"json",
	}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "✗  "+tc.side+" snapshot: Kustomization  flux-system/owner"),
		"snapshot failure lost its owner: exit=%d\n%s\n%s",
		code,
		stdout,
		stderr,
	)
	assert.FatalIf(
		t,
		strings.Count(stderr, "snapshot: Kustomization  flux-system/owner") != 1,
		"corrected side inherited failure:\n%s",
		stderr,
	)
	var result struct {
		Changes []struct {
			Name string `json:"Name"`
		} `json:"Changes"`
		Suppressed []struct {
			Parent struct {
				Name string `json:"Name"`
			} `json:"Parent"`
			Side   string `json:"Side"`
			Reason string `json:"Reason"`
		} `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
	assert.FatalIf(t, len(result.Changes) != 1 || result.Changes[0].Name != "independent-output" ||
		len(result.Suppressed) != 2, "healthy diff or suppression lost:\n%s\n%s", stdout, stderr)
	for _, note := range result.Suppressed {
		switch {
		case note.Parent.Name == "owner":

			assert.FatalIf(
				t,
				note.Side != tc.side || !strings.Contains(note.Reason, "cannot unmarshal"),
				"owner failure belongs to the wrong snapshot:\n%s",
				stdout,
			)

		case note.Parent.Name != "child" || note.Side != "both" || !strings.Contains(note.Reason, "suspended on"):

			t.Fatalf("unexpected descendant suppression:\n%s", stdout)
		}
	}
	nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
	assert.Equal(t, nextCode, code)
	assert.Equal(t, nextOut, stdout)
	assert.Equal(t, nextErr, stderr)
}
