package scope_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SelectedKustomizeClassification(t *testing.T) {
	for _, tc := range []struct {
		name, file, body string
		fail             bool
	}{
		{name: "yaml", file: "kustomization.yaml", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "yml", file: "kustomization.yml", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "capitalized", file: "Kustomization", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "plain"},
		{name: "malformed", file: "kustomization.yaml", body: "resources: [\n", fail: true},
		{name: "multiple", file: "kustomization.yaml", body: "resources: [cm.yaml]\n", fail: true},
		{name: "component", file: "kustomization.yaml", body: "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [missing.yaml]\n", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {value: original}\n",
			)
			if tc.file != "" {
				testutil.WriteFile(t, root, tc.file, tc.body)
			}
			if tc.name == "multiple" {
				testutil.WriteFile(t, root, "kustomization.yml", tc.body)
			}
			if tc.name == "plain" {
				testutil.WriteFile(
					t,
					root,
					"entry.yaml",
					testutil.KustomizationYAML("plain", "apps", "  path: ./unavailable\n  suspend: true\n"),
				)
				out, _ := harness.RequireCLIOK(
					t,
					"get",
					"ks",
					"--path",
					root,
					"-o",
					"json",
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
				)
				var owners []struct {
					Name string `json:"Name"`
				}
				{
					err := json.Unmarshal(
						[]byte(out),
						&owners,
					)
					assert.FatalIf(t, err != nil || len(owners) != 1 ||
						owners[0].Name != "plain", "plain folder roots = %s, %v", out, err)
				}
				return
			}
			out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root,
				"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if tc.fail {
				assert.FatalIf(
					t,
					code != 1 || strings.Contains(out, "value: original") || stderr == "",
					"upstream failure exposed raw fallback: code=%d stdout=%s stderr=%s",
					code,
					out,
					stderr,
				)
				return
			}
			want := "input-dev"
			if tc.name == "plain" {
				want = "input"
			}
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(out, "name: "+want+"\n"),
				"entry classification lost its resource: code=%d stdout=%s stderr=%s",
				code,
				out,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SelectedKustomizeRootDiff(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		before, after                            string
		patch, sibling, plain, declaration, list bool
		want                                     map[string]string
	}{
		{name: "referenced patch", before: "resources: [deployment.yaml, cm.yaml]\npatches: [{path: patch.yaml}]\n", patch: true, want: map[string]string{"web": "changed"}},
		{name: "resource removed", before: "resources: [deployment.yaml, cm.yaml]\n", after: "resources: [deployment.yaml]\n", want: map[string]string{"config": "removed"}},
		{name: "resource added", before: "resources: [deployment.yaml]\n", after: "resources: [deployment.yaml, cm.yaml]\n", want: map[string]string{"config": "added"}},
		{name: "list member added", before: "resources: [deployment.yaml, cm.yaml]\n", list: true, want: map[string]string{"config": "added"}},
		{name: "list member removed", before: "resources: [deployment.yaml, cm.yaml]\n", list: true, want: map[string]string{"config": "removed"}},
		{name: "declaration removed", before: "resources: [deployment.yaml, child.yaml]\nnameSuffix: -dev\n", after: "resources: [deployment.yaml]\nnameSuffix: -dev\n", declaration: true, want: map[string]string{"child-dev": "removed"}},
		{name: "declaration added", before: "resources: [deployment.yaml]\nnameSuffix: -dev\n", after: "resources: [deployment.yaml, child.yaml]\nnameSuffix: -dev\n", declaration: true, want: map[string]string{"child-dev": "added"}},
		{name: "excluded sibling", before: "resources: [deployment.yaml, cm.yaml]\n", sibling: true},
		{name: "plain dependency", plain: true, sibling: true, want: map[string]string{"excluded": "changed"}},
	} {
		t.Run(tc.name, func(t *testing.T) { e2ESelectedKustomizeRootDiffName(t, &tc) })
	}
	for _, tc := range []struct {
		name          string
		before, after []string
	}{
		{name: "owner added", after: []string{"owner"}},
		{name: "owner removed", before: []string{"owner"}},
		{name: "owner renamed", before: []string{"old"}, after: []string{"new"}},
		{name: "owner winner changed", before: []string{"a", "b"}, after: []string{"b"}},
	} {
		t.Run(tc.name, func(t *testing.T) { e2ESelectedKustomizeRootDiffName2(t, &tc) })
	}
	for _, tc := range []struct {
		name   string
		owners [2]string
		nested bool
	}{
		{name: "owner added", owners: [2]string{"", "owner"}},
		{name: "owner removed", owners: [2]string{"owner", ""}},
		{name: "owner renamed", owners: [2]string{"old", "new"}},
		{name: "owner renamed nested", owners: [2]string{"old", "new"}, nested: true},
	} {
		t.Run("named missing child/"+tc.name, func(t *testing.T) { e2ESelectedKustomizeRootDiffCase(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_NamespaceDiff(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		want                map[string]string
	}{
		{name: "changed", before: "config", after: "config", want: map[string]string{"config": "changed"}},
		{name: "added", after: "config", want: map[string]string{"config": "added"}},
		{name: "removed", before: "config", want: map[string]string{"config": "removed"}},
		{name: "renamed", before: "before", after: "after", want: map[string]string{"before": "removed", "after": "added"}},
		{name: "empty", want: map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots := []string{t.TempDir(), t.TempDir()}
			for side, root := range roots {
				testutil.GitInit(t, root)
				testutil.WriteFile(
					t,
					root,
					"flux/peer.yaml",
					testutil.KustomizationYAML("peer", "monitoring", "  path: ./peer\n  targetNamespace: monitoring\n"),
				)
				testutil.WriteFile(t, root, "peer/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"peer/cm.yaml",
					fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: peer}\ndata: {value: '%d'}\n", side),
				)
				name := []string{tc.before, tc.after}[side]
				if name != "" {
					testutil.WriteFile(
						t,
						root,
						"flux/selected.yaml",
						testutil.KustomizationYAML("selected", "apps", "  path: ./payload\n  targetNamespace: apps\n"),
					)
					testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(
						t,
						root,
						"payload/cm.yaml",
						fmt.Sprintf(
							"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s}\ndata: {value: '%d'}\n",
							name,
							side,
						),
					)
				}
			}
			for _, selection := range [][]string{{"all"}, {"ks"}, {"ks", "selected"}} {
				if tc.before == "" && tc.after == "" && len(selection) > 1 {
					continue
				}
				args := append([]string{"diff"}, selection...)
				args = append(
					args,
					"--namespace",
					"apps",
					"--path",
					filepath.Join(roots[1], "flux"),
					"--path-orig",
					filepath.Join(roots[0], "flux"),
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
					"-o",
					"json",
				)
				first, _ := harness.RequireCLIOK(t, args...)
				second, _ := harness.RequireCLIOK(t, args...)
				assert.Equal(t, first, second)
				var result struct {
					Changes []struct {
						Name   string `json:"Name"`
						Status string `json:"Status"`
					} `json:"Changes"`
				}
				assert.NoError(t, json.Unmarshal([]byte(first), &result))
				got := map[string]string{}
				for _, change := range result.Changes {
					got[change.Name] = change.Status
				}
				assert.Diff(t, got, tc.want)
			}
		})
	}
}

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
	first, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	second, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	assert.Equal(t, first, second)
	docs, err := testutil.DecodeDocs(strings.NewReader(first))
	assert.NoError(t, err)
	counts := map[string]int{}
	for _, doc := range docs {
		counts[manifest.DocKind(doc)]++
	}
	assert.Diff(t, counts, map[string]int{"Kustomization": 2, "ServiceAccount": 2, "ConfigMap": 2})
	roster, _ := harness.RequireCLIOK(t, append([]string{"get", "ks", "-o", "json"}, flags...)...)
	var objects []map[string]any
	assert.NoError(t, json.Unmarshal([]byte(roster), &objects))
	assert.Equal(t, len(objects), 2)
	harness.MutateFile(t, filepath.Join(roots[1], "apps/first/ks.yaml"), "interval: 1m", "interval: 2m")
	output, _ := harness.RequireCLIOK(
		t,
		append([]string{"diff", "ks", "-o", "json", "--path-orig", filepath.Join(roots[0], "apps")}, flags...)...)
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
	selected, _ := harness.RequireCLIOK(
		t,
		append([]string{"build", "ks", "--path-orig", filepath.Join(roots[0], "apps")}, flags...)...)
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
	out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--no-progress")
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
	out, _ = harness.RequireCLIOK(
		t,
		"build",
		"all",
		"-p",
		filepath.Join(root, "production"),
		"--concurrency",
		"2",
		"--no-progress",
	)
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
				out, stderr, code := harness.RunCLIBuffers(
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
				listed, _, _ := harness.RunCLIBuffers(
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
				roster, _, _ := harness.RunCLIBuffers(
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

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_AuthoritativeManifestFailures(t *testing.T) {
	const release = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: invalid, namespace: flux-system}
spec:
  interval: []
  suspend: true
  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system}}}
`
	for _, tc := range []struct {
		name, document, patch, extra, producer, symptom string
		failed                                          bool
	}{
		{name: "typed release", document: release, producer: "Kustomization/flux-system/owner", symptom: "cannot unmarshal", failed: true},
		{name: "typed kustomization", document: testutil.KustomizationYAML("invalid", "flux-system", "  interval: []\n  path: ./child\n  sourceRef: {kind: GitRepository, name: flux-system}\n"), producer: "Kustomization/flux-system/owner", symptom: "cannot unmarshal", failed: true},
		{name: "generated release", document: "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: generator, namespace: flux-system}\nspec:\n  resourcesTemplate: |\n    " + strings.ReplaceAll(strings.TrimSpace(release), "\n", "\n    ") + "\n", producer: "ResourceSet/flux-system/generator", symptom: "cannot unmarshal", failed: true},
		{name: "patch-created release", document: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: invalid, namespace: flux-system}\n", patch: "patches:\n- target: {kind: ConfigMap, name: invalid}\n  patch: |-\n    - op: replace\n      path: /apiVersion\n      value: helm.toolkit.fluxcd.io/v2\n    - op: replace\n      path: /kind\n      value: HelmRelease\n    - op: add\n      path: /spec\n      value: {interval: [], suspend: true}\n", producer: "Kustomization/flux-system/owner", symptom: "cannot unmarshal", failed: true},
		{name: "required syntax", document: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: broken}\nbroken: [\n", producer: "Kustomization/flux-system/owner", symptom: "did not find expected", failed: true},
		{name: "repaired raw release", document: release, patch: "patches:\n- target: {kind: HelmRelease, name: invalid}\n  patch: |-\n    - op: replace\n      path: /spec/interval\n      value: 10m\n"},
		{name: "incidental malformed and scalar", document: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\n", extra: "configMapGenerator:\n- name: settings\n  files: [settings.yaml]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.GitInit(t, dir)
			testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [drivers.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"flux/drivers.yaml",
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
			testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources: [child.yaml]\n"+tc.patch+tc.extra)
			testutil.WriteFile(t, dir, "app/child.yaml", tc.document)
			testutil.WriteFile(t, dir, "app/unrelated.yaml", "broken: [\n")
			testutil.WriteFile(t, dir, "app/settings.yaml", "just scalar content\n")
			testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"healthy/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent-output}\n",
			)
			args := []string{
				"build",
				"all",
				"--path",
				filepath.Join(dir, "flux"),
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			want := 0
			if tc.failed {
				want = 1
			}
			assert.FatalIf(
				t,
				code != want || !strings.Contains(stdout, "independent-output"),
				"exit=%d want=%d\n%s\n%s",
				code,
				want,
				stdout,
				stderr,
			)
			producer := strings.SplitN(tc.producer, "/", 2)
			assert.FatalIf(t, tc.failed &&
				(!strings.Contains(stderr, producer[0]) || !strings.Contains(stderr, producer[1]) || strings.Count(stderr, tc.symptom) != 1), "unstable or unowned failure:\n%s", stderr)
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}
