package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestRun_TestAll_IncompleteInputCoverage(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml]\n")
	testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./apps\n  interval: 5m\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\ntransformers: [../cfg]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\n")
	testutil.WriteFile(t, root, "cfg/kustomization.yaml", "resources: [ns.yaml]\nimages: [{name: unused, newTag: latest}]\n")
	testutil.WriteFile(t, root, "cfg/ns.yaml", "apiVersion: builtin\nkind: NamespaceTransformer\nmetadata: {name: namespace}\nnamespace: apps\n")
	stdout, stderr, code := runCLI(t, "test", "all", "--path", root, "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
	if code != 0 || !strings.Contains(stdout, "✓") || !strings.Contains(stdout, "1 passed") || strings.Contains(stdout, "failed") {
		t.Fatalf("full test failed: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if strings.Contains(stdout+stderr, "input coverage is incomplete") {
		t.Fatalf("full test reported changed-only uncertainty: stdout=%s stderr=%s", stdout, stderr)
	}
}

func TestRun_Diff_IncompleteInputCoverage(t *testing.T) {
	testComparisonInputWarnings(t, "all")
}

func TestRun_DiffImages_IncompleteInputCoverage(t *testing.T) {
	testComparisonInputWarnings(t, "images")
}

func testComparisonInputWarnings(t *testing.T, verb string) {
	t.Helper()
	write := func(interval string) string {
		t.Helper()
		root := t.TempDir()
		testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [affected.yaml, selected.yaml]\n")
		testutil.WriteFile(t, root, "flux/affected.yaml", testutil.KustomizationYAML("affected", "apps", "  path: ./apps\n  interval: 5m\n"))
		testutil.WriteFile(t, root, "flux/selected.yaml", testutil.KustomizationYAML("selected", "infra", "  path: ./infra\n  interval: "+interval+"\n"))
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "generators: [../config]\n")
		testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: [plugin.yaml]\npatches: [{target: {kind: ConfigMapGenerator}, patch: '[{\"op\":\"replace\",\"path\":\"/files/0\",\"value\":\"hidden.txt\"}]' }]\n")
		testutil.WriteFile(t, root, "config/plugin.yaml", "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: stable}\nfiles: [value.txt]\n")
		testutil.WriteFile(t, root, "apps/value.txt", "stable")
		testutil.WriteFile(t, root, "apps/hidden.txt", "stable")
		testutil.WriteFile(t, root, "infra/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "infra/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unchanged}\ndata: {value: unchanged}\n")
		return root
	}
	orig, current := write("5m"), write("10m")
	for _, namespace := range []string{"", "infra"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			args := []string{"diff", verb, "--path", current, "--path-orig", orig,
				"--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir()}
			if namespace != "" {
				args = append(args, "--namespace", namespace)
			}
			stdout, stderr, code := runCLI(t, args...)
			if code != 0 || stdout != "" {
				t.Fatalf("comparison changed output or failed: code=%d stdout=%q stderr=%s", code, stdout, stderr)
			}
			for _, snapshot := range []string{"orig", "current"} {
				want := "flate warning: " + snapshot + " snapshot: Kustomization.kustomize.toolkit.fluxcd.io/apps/affected: changed-only input coverage is incomplete"
				if strings.Contains(stderr, want) != (namespace == "") {
					t.Fatalf("warning scope differs: want %q visible=%v; stderr=%s", want, namespace == "", stderr)
				}
			}
			if namespace == "" && (!strings.Contains(stderr, "configuration transformations may rewrite plugin file operands") || !strings.Contains(stderr, "flate build all without --base or --path-orig")) {
				t.Fatalf("missing actionable warning details: %s", stderr)
			}
		})
	}
}
