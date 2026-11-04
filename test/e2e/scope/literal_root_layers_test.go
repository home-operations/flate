package scope_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_RootLayerDeclarations(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	for _, root := range roots {
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [first, second]\n")
		for _, name := range []string{"first", "second"} {
			testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [ks.yaml, plain.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/"+name+"/ks.yaml",
				testutil.KustomizationYAML(name, "apps", "  path: ./workloads/"+name+"\n  interval: 1m\n"),
			)
			testutil.WriteFile(
				t,
				root,
				"apps/"+name+"/plain.yaml",
				"apiVersion: v1\nkind: ServiceAccount\nmetadata: {name: "+name+", namespace: apps}\n",
			)
			testutil.WriteFile(t, root, "workloads/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"workloads/"+name+"/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\n",
			)
		}
	}
	flags := []string{
		"--path",
		filepath.Join(roots[1], "apps"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	requireCLIOKResult1 := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	first, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
	requireCLIOKResult2 := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	second, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
	assert.Equal(t, first, second)
	docs, err := testutil.DecodeDocs(strings.NewReader(first))
	assert.NoError(t, err)
	counts := map[string]int{}
	for _, doc := range docs {
		counts[manifest.DocKind(doc)]++
	}
	assert.Diff(t, counts, map[string]int{"Kustomization": 2, "ServiceAccount": 2, "ConfigMap": 2})
	requireCLIOKResult3 := harness.RequireCLIOK(t, append([]string{"get", "ks", "-o", "json"}, flags...)...)
	roster, _ := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
	var objects []map[string]any
	assert.NoError(t, json.Unmarshal([]byte(roster), &objects))
	assert.Equal(t, len(objects), 2)
	harness.MutateFile(t, filepath.Join(roots[1], "apps", "first", "ks.yaml"), "interval: 1m", "interval: 2m")
	requireCLIOKResult4 := harness.RequireCLIOK(
		t,
		append([]string{"diff", "ks", "-o", "json", "--path-orig", filepath.Join(roots[0], "apps")}, flags...)...)
	output, _ := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
	var comparison struct {
		Changes []struct {
			Kind   string `json:"Kind"`
			Name   string `json:"Name"`
			Status string `json:"Status"`
		} `json:"Changes"`
	}
	assert.NoError(t, json.Unmarshal([]byte(output), &comparison))
	assert.FatalIf(t, len(comparison.Changes) != 1 || comparison.Changes[0].Kind != manifest.KindKustomization ||
		comparison.Changes[0].Name != "first" ||
		comparison.Changes[0].Status != "changed", "interval-only root declaration missing: %s", output)
	requireCLIOKResult5 := harness.RequireCLIOK(
		t,
		append([]string{"build", "ks", "--path-orig", filepath.Join(roots[0], "apps")}, flags...)...)
	selected, _ := requireCLIOKResult5.Stdout, requireCLIOKResult5.Stderr
	assert.FatalIf(
		t,
		!strings.Contains(selected, "interval: 2m"),
		"changed-only output lost the root layer: %s",
		selected,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_LiteralRootOverlayCollision(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [production, staging]\n")
	for _, overlay := range []string{"production", "staging"} {
		testutil.WriteFile(t, root, overlay+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			overlay+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: collision, namespace: apps}\n",
		)
	}
	runCLIBuffersResult6 := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--no-progress")
	out, stderr, code := runCLIBuffersResult6.Stdout, runCLIBuffersResult6.Stderr, runCLIBuffersResult6.Code
	assert.FatalIf(
		t,
		code != 1 || out != "" || !strings.Contains(stderr, "production") || !strings.Contains(stderr, "staging") ||
			!strings.Contains(
				stderr,
				"-p",
			),
		"literal root collision was hidden: code=%d out=%s stderr=%s",
		code,
		out,
		stderr,
	)
	requireCLIOKResult7 := harness.RequireCLIOK(
		t,
		"build",
		"all",
		"-p",
		filepath.Join(root, "production"),
		"--concurrency",
		"2",
		"--no-progress",
	)
	out, _ = requireCLIOKResult7.Stdout, requireCLIOKResult7.Stderr
	docs, err := testutil.DecodeDocs(strings.NewReader(out))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(docs) != 1 || manifest.DocIdentity(docs[0]).Name != "collision",
		"explicit valid overlay failed: %s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_LiteralRootFailureNamespace(t *testing.T) {
	for _, command := range []string{"build", "test"} {
		for _, kind := range []string{"all", "ks"} {
			t.Run(command+"/"+kind, func(t *testing.T) {
				root := t.TempDir()
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml, ks.yaml, missing.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root-output, namespace: apps}\n",
				)
				testutil.WriteFile(
					t,
					root,
					"ks.yaml",
					testutil.KustomizationYAML(
						"child",
						"apps",
						"  path: ./app\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom:\n      - kind: ConfigMap\n        name: unavailable-input\n",
					),
				)
				testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: []\n")
				runCLIBuffersResult8 := harness.RunCLIBuffers(
					command,
					kind,
					"-n",
					"apps",
					"--path",
					root,
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
				)
				out, stderr, code := runCLIBuffersResult8.Stdout, runCLIBuffersResult8.Stderr, runCLIBuffersResult8.Code
				assert.FatalIf(
					t,
					code != 1 || !strings.Contains(out+stderr, "missing.yaml"),
					"namespace selection hid the failed literal root: code=%d out=%s stderr=%s",
					code,
					out,
					stderr,
				)
				assert.FatalIf(t, command == "build" && out != "", "failed literal root published output: %s", out)
				assert.FatalIf(t, command == "test" &&
					!strings.Contains(
						out,
						"apps/child  blocked by apps/unavailable-input (not found)",
					), "root failure hid the blocked child: %s", out)
				if command != "test" || kind != "all" {
					return
				}
				runCLIBuffersResult9 := harness.RunCLIBuffers(
					"get",
					"ks",
					"-o",
					"json",
					"-n",
					"apps",
					"--path",
					root,
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
				)
				listed, _, _ := runCLIBuffersResult9.Stdout, runCLIBuffersResult9.Stderr, runCLIBuffersResult9.Code
				var objects []struct {
					Name string `json:"Name"`
				}
				{
					err := json.Unmarshal(
						[]byte(listed),
						&objects,
					)
					assert.FatalIf(t, err != nil || len(objects) != 1 ||
						objects[0].Name != "child", "get exposed the internal root: %s (decode: %v)", listed, err)
				}
				runCLIBuffersResult10 := harness.RunCLIBuffers(
					"get",
					"all",
					"-o",
					"json",
					"-n",
					"apps",
					"--path",
					root,
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
				)
				roster, _, _ := runCLIBuffersResult10.Stdout, runCLIBuffersResult10.Stderr, runCLIBuffersResult10.Code
				var resources []struct {
					Kind string `json:"Kind"`
					Name string `json:"Name"`
				}
				{
					err := json.Unmarshal(
						[]byte(roster),
						&resources,
					)
					assert.FatalIf(
						t,
						err != nil || len(resources) != 1 || resources[0].Kind != manifest.KindKustomization ||
							resources[0].Name != "child",
						"get counted the internal root: %s (decode: %v)",
						roster,
						err,
					)
				}
			})
		}
	}
}
