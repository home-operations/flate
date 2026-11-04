package scope_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SelectedKustomizeRoot(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "app/kustomization.yaml", `resources: [release.yaml, repository.yaml, child.yaml]
nameSuffix: -dev
patches:
  - target: {kind: HelmRelease, name: podinfo}
    patch: |-
      - op: replace
        path: /spec/chart/spec/version
        value: 6.5.0
`)
	testutil.WriteFile(t, root, "app/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: podinfo, namespace: apps}
spec:
  suspend: true
  chart:
    spec:
      chart: podinfo
      version: 6.0.0
      sourceRef: {kind: HelmRepository, name: podinfo}
`)
	testutil.WriteFile(t, root, "app/repository.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: podinfo, namespace: apps}
spec: {suspend: true, url: https://fixture.invalid/charts}
`)
	testutil.WriteFile(t, root, "app/unlisted.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: unlisted, namespace: apps}
spec: {suspend: true}
`)
	testutil.WriteFile(
		t,
		root,
		"app/child.yaml",
		testutil.KustomizationYAML("child", "apps", "  path: ./unavailable\n  suspend: true\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"app/excluded.yaml",
		testutil.KustomizationYAML(
			"excluded-driver",
			"apps",
			"  path: ./must-not-run\n",
		)+"---\n"+`apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: excluded-source, namespace: apps}
spec: {url: https://must-not-run.invalid/charts}
`,
	)
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/owner.yaml",
		testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  path: ./app\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)

	out, stderr := harness.RequireCLIOK(t, "get", "hr", "-o", "json", "--path", filepath.Join(root, "app"),
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	var releases []struct {
		Name    string `json:"Name"`
		Version string `json:"Version"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &releases))
	assert.FatalIf(
		t,
		len(releases) != 1 || releases[0].Name != "podinfo-dev" || releases[0].Version != "6.5.0",
		"selected root lost transformations or admitted a sibling:\n%s\n%s",
		out,
		stderr,
	)

	flags := []string{
		"--path",
		filepath.Join(root, "app"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	build, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	second, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	assert.FatalIf(t, build != second, "%v", "direct build output changed between runs")
	docs, err := testutil.SplitDocs([]byte(build))
	assert.NoError(t, err)
	for _, doc := range docs {
		name, _ := manifest.DocMetadata(doc)
		assert.FatalIf(
			t,
			name == "flate-bootstrap" || strings.HasPrefix(name, "excluded-") || name == "unlisted" || name == "child",
			"unexpected authored or bootstrap document: %v",
			doc,
		)
	}
	kss, _ := harness.RequireCLIOK(t, append([]string{"get", "ks", "-o", "json"}, flags...)...)
	var owners []struct {
		Name string `json:"Name"`
	}
	assert.NoError(t, json.Unmarshal([]byte(kss), &owners))
	assert.FatalIf(t, len(owners) != 1 || owners[0].Name != "child-dev", "ordinary root/child collection = %s", kss)
	owned, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	ownedDocs, err := testutil.SplitDocs([]byte(owned))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(docs) != 3 || len(ownedDocs) != 4,
		"direct/owned document counts: %d/%d",
		len(docs),
		len(ownedDocs),
	)
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		assert.FatalIf(
			t,
			namespace != "apps" || name != "podinfo-dev" && name != "child-dev",
			"selected entry lost a transformed identity: %v",
			doc,
		)
	}
	for _, doc := range ownedDocs {
		name, namespace := manifest.DocMetadata(doc)
		if name == "owner" && namespace == "flux-system" {
			continue
		}
		assert.FatalIf(
			t,
			namespace != "apps" || name != "podinfo-dev" && name != "child-dev",
			"actual Flux owner lost a transformed identity: %v",
			doc,
		)
		md := doc["metadata"].(map[string]any)
		labels := md["labels"].(map[string]any)
		assert.FatalIf(t, labels[manifest.FluxKustomizeDomain+"/name"] != "owner" ||
			labels[manifest.FluxKustomizeDomain+"/namespace"] != "flux-system", "actual Flux owner labels missing: %v", labels)
	}

	tested, _ := harness.RequireCLIOK(t, append([]string{"test", "hr"}, flags...)...)
	assert.FatalIf(
		t,
		!strings.Contains(tested, "podinfo-dev") || strings.Contains(tested, "flate-bootstrap"),
		"test did not use real transformed identity: %s",
		tested,
	)

	t.Run("unsuspended child", func(t *testing.T) { e2ESelectedKustomizeRootUnsuspendedChild(t, &root, &flags) })

	t.Run(
		"excluded entry dependency",
		func(t *testing.T) { e2ESelectedKustomizeRootExcludedEntryDependency(t, &root, &flags) },
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SelectedKustomizeRootNestedOwner(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"app/kustomization.yaml",
		"nameSuffix: -dev\nresources: [child.yaml, cm.yaml, nested]\n",
	)
	testutil.WriteFile(t, root, "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./app/nested\n"))
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: outer, namespace: apps}\n",
	)
	testutil.WriteFile(t, root, "app/nested/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/nested/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: inner, namespace: apps}\n",
	)
	flags := []string{
		"build",
		"all",
		"--path",
		filepath.Join(root, "app"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	out, _ := harness.RequireCLIOK(t, flags...)
	second, _ := harness.RequireCLIOK(t, flags...)
	assert.FatalIf(t, out != second, "%v", "selected entry output changed between runs")
	t.Logf("full selected entry output:\n%s", out)
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	identities := map[string]bool{}
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		identities[manifest.DocKind(doc)+"/"+name] = true
		if name == "inner" {
			labels := doc["metadata"].(map[string]any)["labels"].(map[string]any)
			assert.FatalIf(t, namespace != "apps" || labels[manifest.FluxKustomizeDomain+"/name"] != "child-dev" ||
				labels[manifest.FluxKustomizeDomain+"/namespace"] != "apps", "nested child reconciliation lost ownership: %v", doc)
		}
	}
	want := map[string]bool{
		"ConfigMap/inner":         true,
		"ConfigMap/inner-dev":     true,
		"ConfigMap/outer-dev":     true,
		"Kustomization/child-dev": true,
	}
	{
		difference := cmp.Diff(want, identities)
		assert.FatalIf(t, difference != "", "selected entry documents (-want +got):\n%s", difference)
	}
	assert.FatalIf(t, len(docs) != len(want), "selected entry document count = %d, want %d", len(docs), len(want))
}

func e2ESelectedKustomizeRootUnsuspendedChild(t *testing.T, root *string, flags *[]string) {
	t.Helper()

	testutil.WriteFile(t, (*root), "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"))
	testutil.WriteFile(t, (*root), "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, (*root), "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf-output}\n")
	harness.MutateFile(
		t,
		filepath.Join((*root), "app/kustomization.yaml"),
		"resources: [release.yaml, repository.yaml, child.yaml]",
		"resources: [release.yaml, repository.yaml, child.yaml, root.yaml]",
	)
	testutil.WriteFile(
		t,
		(*root),
		"app/root.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root-output, namespace: apps}\ndata: {value: root-value}\n",
	)
	out, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, (*flags)...)...)
	again, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, (*flags)...)...)
	assert.Equal(t, again, out)
	docs, err := testutil.DecodeDocs(strings.NewReader(out))
	assert.NoError(t, err)
	assert.FatalIf(t, len(docs) != 5, "child and root inventory count=%d, want 5: %s", len(docs), out)
	assert.Diff(t, docs[0], map[string]any{
		"apiVersion": "v1",
		"kind":       manifest.KindConfigMap,
		"metadata": map[string]any{
			"name": "leaf-output",
			"labels": map[string]any{
				manifest.FluxKustomizeDomain + "/name":      "child-dev",
				manifest.FluxKustomizeDomain + "/namespace": "apps",
			},
		},
	})
	rootMaps := 0
	for _, doc := range docs[1:] {
		if manifest.DocKind(doc) != manifest.KindConfigMap {
			continue
		}
		rootMaps++
		assert.Diff(t, doc, map[string]any{
			"apiVersion": "v1",
			"kind":       manifest.KindConfigMap,
			"metadata":   map[string]any{"name": "root-output-dev", "namespace": "apps"},
			"data":       map[string]any{"value": "root-value"},
		})
	}
	assert.Equal(t, rootMaps, 1)
	named, _ := harness.RequireCLIOK(t, append([]string{"build", "ks", "child-dev"}, (*flags)...)...)
	assert.FatalIf(
		t,
		!strings.Contains(named, "name: leaf-output") || strings.Contains(named, "name: podinfo-dev"),
		"named child selection mixed root siblings with descendants: %s",
		named,
	)
}

func e2ESelectedKustomizeRootExcludedEntryDependency(t *testing.T, root *string, flags *[]string) {
	t.Helper()

	testutil.WriteFile(
		t,
		(*root),
		"app/kustomization.yaml",
		"resources: [child.yaml, settings.yaml]\nnameSuffix: -dev\n",
	)
	testutil.WriteFile(
		t,
		(*root),
		"app/settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: resolved}\n",
	)
	testutil.WriteFile(t, (*root), "app/child.yaml", testutil.KustomizationYAML("child", "apps", `  path: ./leaf
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: settings-dev}]
`))
	testutil.WriteFile(t, (*root), "leaf/kustomization.yaml", "resources: [account.yaml]\n")
	testutil.WriteFile(
		t,
		(*root),
		"leaf/account.yaml",
		"apiVersion: v1\nkind: ServiceAccount\nmetadata: {name: visible, annotations: {value: '${VALUE}'}}\n",
	)
	args := append([]string{"build", "all", "--skip-kinds", "ConfigMap"}, (*flags)...)
	out, _ := harness.RequireCLIOK(t, args...)
	warm, _ := harness.RequireCLIOK(t, args...)
	assert.FatalIf(t, out != warm, "%v", "excluded dependency output changed on a cache hit")
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	for _, doc := range docs {
		assert.FatalIf(
			t,
			manifest.DocKind(doc) == manifest.KindConfigMap,
			"excluded entry dependency leaked into output: %v",
			doc,
		)
	}
	assert.FatalIf(
		t,
		!strings.Contains(out, "kind: ServiceAccount\n") || !strings.Contains(out, "value: resolved") ||
			!strings.Contains(
				out,
				"kustomize.toolkit.fluxcd.io/name: child-dev",
			),
		"excluded entry values lost admission or visible descendant stamping: %s",
		out,
	)
}
