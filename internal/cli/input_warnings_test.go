package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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

func TestRun_ChangedOnly_IncompleteInputCoverage(t *testing.T) {
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			orig, current := writeInputWarningsFixture(t, "5m"), writeInputWarningsFixture(t, "10m")
			stdout, stderr, code := runCLI(t, verb, "all", "--path", current, "--path-orig", orig,
				"--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
			if code != 0 {
				t.Fatalf("changed-only %s failed: code=%d stdout=%s stderr=%s", verb, code, stdout, stderr)
			}
			advisories, other := stderr, stdout
			if verb == "test" {
				advisories, other = stdout, stderr
			}
			message := "changed-only input coverage is incomplete"
			if strings.Count(advisories, message) != 1 {
				t.Fatalf("expected one coverage warning: %s", advisories)
			}
			for _, diagnostic := range []string{
				"Kustomization.kustomize.toolkit.fluxcd.io apps/affected: " + message,
				message,
				"configuration transformations may rewrite plugin file operands",
				"flate build all without --base or --path-orig",
			} {
				if !strings.Contains(advisories, diagnostic) || strings.Contains(other, diagnostic) {
					t.Errorf("diagnostic %q at wrong destination: stdout=%s stderr=%s", diagnostic, stdout, stderr)
				}
			}
			if strings.Contains(stdout+stderr, "orig snapshot:") || strings.Contains(stdout+stderr, "current snapshot:") {
				t.Errorf("single run has comparison labels: stdout=%s stderr=%s", stdout, stderr)
			}
			if verb == "build" {
				docs, err := manifest.SplitDocs([]byte(stdout))
				assert.NoError(t, err)
				assert.Diff(t, docs, []map[string]any{{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name": "unchanged",
						"labels": map[string]any{
							"kustomize.toolkit.fluxcd.io/name":      "selected",
							"kustomize.toolkit.fluxcd.io/namespace": "infra",
						},
					},
					"data": map[string]any{"value": "unchanged"},
				}})
				return
			}
			roster, _, _ := strings.Cut(strings.TrimSpace(stdout), "\n\n")
			assert.Equal(t, roster, "✓  Kustomization.kustomize.toolkit.fluxcd.io  infra/selected")
			verdict := strings.TrimSpace(stdout[strings.LastIndex(stdout, "\n\n")+2:])
			counts, _, _ := strings.Cut(verdict, "   ")
			assert.Equal(t, counts, "✓ 1 passed")
		})
	}
}

func TestRun_BuildAll_FullTreeIncompleteInputCoverage(t *testing.T) {
	root := writeInputWarningsFixture(t, "10m")
	stdout, stderr, code := runCLI(t, "build", "all", "--path", root,
		"--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
	if code != 0 || strings.Contains(stdout+stderr, "input coverage is incomplete") {
		t.Fatalf("full build failed or reported changed-only uncertainty: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	docs, err := manifest.SplitDocs([]byte(stdout))
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 2)
	if !strings.Contains(stdout, "name: unchanged") || !strings.Contains(stdout, "name: stable-") ||
		!strings.Contains(stdout, "hidden.txt: stable") {
		t.Fatalf("full build lost normal manifest output: %s", stdout)
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
	orig, current := writeInputWarningsFixture(t, "5m"), writeInputWarningsFixture(t, "10m")
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
			message := "changed-only input coverage is incomplete"
			if namespace != "" {
				if strings.Contains(stderr, message) || strings.Contains(stderr, "apps/affected") {
					t.Fatalf("out-of-scope warning: %s", stderr)
				}
				return
			}
			origStart := strings.Index(stderr, "orig snapshot:")
			currentStart := strings.Index(stderr, "current snapshot:")
			if origStart < 0 || currentStart <= origStart || strings.Count(stderr, "orig snapshot:") != 1 || strings.Count(stderr, "current snapshot:") != 1 {
				t.Fatalf("expected distinct orig then current warning sections: %s", stderr)
			}
			for _, side := range []struct {
				name, block string
			}{{"orig", stderr[origStart:currentStart]}, {"current", stderr[currentStart:]}} {
				want := "Kustomization.kustomize.toolkit.fluxcd.io apps/affected: " + message
				if strings.Count(side.block, message) != 1 || !strings.Contains(side.block, want) {
					t.Fatalf("expected one attributed %s warning: %s", side.name, side.block)
				}
				if !strings.Contains(side.block, "configuration transformations may rewrite plugin file operands") || !strings.Contains(side.block, "flate build all without --base or --path-orig") {
					t.Fatalf("missing actionable %s warning details: %s", side.name, side.block)
				}
			}
		})
	}
}

func writeInputWarningsFixture(t *testing.T, interval string) string {
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
