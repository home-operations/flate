package scope_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_OrphanParentSource(t *testing.T) {
	for _, tc := range []struct {
		name     string
		external bool
	}{
		{name: "local parent"},
		{name: "deeper external parent", external: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(
				t,
				dir,
				"kubernetes/flux/ks.yaml",
				testutil.KustomizationYAML(
					"local-apps",
					"flux-system",
					"  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			resources := "resources: [configmap.yaml]\n"
			if tc.external {
				resources = "resources: [configmap.yaml, external.yaml]\n"
				testutil.WriteFile(t, dir, "kubernetes/apps/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec:
  provider: github
  url: https://fixture.invalid/external
  secretRef: {name: missing-auth}
`+"---\n"+
					testutil.KustomizationYAML("external-parent", "flux-system", `  path: ./kubernetes/apps/orphan
  sourceRef: {kind: GitRepository, name: external}
`))
			}
			testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", resources)
			testutil.WriteFile(t, dir, "kubernetes/apps/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: healthy-output}
data: {value: rendered}
`)
			testutil.WriteFile(
				t,
				dir,
				"kubernetes/apps/orphan/ks.yaml",
				testutil.KustomizationYAML(
					"orphan",
					"flux-system",
					"  path: ./missing\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			stdout, stderr, code := harness.RunCLIBuffers(
				"build",
				"all",
				"--path",
				dir,
				"--allow-missing-kinds=Secret,ConfigMap",
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			)
			assert.FatalIf(t, code != 0 || !strings.Contains(stdout, "name: healthy-output") ||
				strings.Contains(
					stdout,
					"name: orphan\n",
				), "unreferenced child failed a healthy local build: exit=%d\n%s\n%s", code, stdout, stderr)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ComponentChangePropagatesToAllConsumers(t *testing.T) {
	src := harness.TestdataPath(t, "components")
	orig := t.TempDir()
	testutil.CopyTree(t, src, orig)
	testutil.GitInit(t, orig)
	current := t.TempDir()
	testutil.CopyTree(t, src, current)
	testutil.GitInit(t, current)

	// Mutate the shared component in only the "current" tree.
	harness.MutateFile(
		t,
		filepath.Join(current, "components", "shared", "shared-cm.yaml"),
		"value: original",
		"value: changed",
	)

	// -o github explicitly: this test asserts github diff-syntax markers,
	// independent of the default style.
	out := harness.RunCLI(
		t,
		"diff",
		"ks",
		"--path",
		filepath.Join(current, "cluster"),
		"--path-orig",
		filepath.Join(orig, "cluster"),
		"-o",
		"github",
	)

	// Coverage must propagate to BOTH consumers, so the
	// original→changed transition should surface at least twice (once
	// per consumer Kustomization). dyff prints value changes as bare
	// `- original` / `+ changed` lines under a `@@ data.value @@`
	// path header.
	{
		got := strings.Count(out, "+ changed")
		assert.ErrorIf(
			t,
			got < 2,
			"coverage did not propagate to both consumers (got %d hits, want >= 2):\n%s",
			got,
			out,
		)
	}
	assert.ErrorIf(t, !strings.Contains(out, "- original"), "removal of baseline value missing from diff:\n%s", out)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_NonSharedChangeDoesNotPropagate(t *testing.T) {
	src := harness.TestdataPath(t, "components")
	orig := t.TempDir()
	testutil.CopyTree(t, src, orig)
	testutil.GitInit(t, orig)
	current := t.TempDir()
	testutil.CopyTree(t, src, current)
	testutil.GitInit(t, current)

	harness.MutateFile(t, filepath.Join(current, "apps", "app-a", "cm.yaml"), "app: app-a", "app: app-a-edited")

	out := harness.RunCLI(
		t,
		"diff",
		"ks",
		"--path",
		filepath.Join(current, "cluster"),
		"--path-orig",
		filepath.Join(orig, "cluster"),
	)

	assert.ErrorIf(t, !strings.Contains(out, "app-a-edited"), "expected app-a change to surface in diff:\n%s", out)
	// app-b must be UNTOUCHED — no app-b-cm body should appear in
	// either diff direction.
	assert.ErrorIf(
		t,
		strings.Contains(out, "name: app-b-cm"),
		"app-b leaked into diff for an app-a-only change (over-broad coverage):\n%s",
		out,
	)
}

func TestE2E_SkipKindsAppliesToKustomizationOutput(t *testing.T) {
	root := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "skip-kinds"), root)
	for _, tc := range []struct {
		name                        string
		args                        []string
		env                         map[string]string
		wantCM, wantSecret, wantCRD bool
	}{
		{name: "defaults", wantCM: true},
		{name: "empty skip", args: []string{"--skip-kinds", ""}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "replacement skip", args: []string{"--skip-kinds", "ConfigMap"}, wantSecret: true, wantCRD: true},
		{name: "only CRDs", args: []string{"--only-kinds", "CustomResourceDefinition"}, wantCRD: true},
		{name: "only Secret", args: []string{"--only-kinds", "Secret"}, wantSecret: true},
		{name: "only multiple", args: []string{"--only-kinds", "ConfigMap,Secret"}, wantCM: true, wantSecret: true},
		{name: "empty only", args: []string{"--only-kinds", ""}, wantCM: true},
		{name: "exact kind", args: []string{"--only-kinds", "secret"}},
		{name: "exact exclusion", args: []string{"--skip-kinds", "secret,customresourcedefinition"}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "env empty only", env: map[string]string{"FLATE_ONLY_KINDS": ""}, wantCM: true},
		{name: "argv overrides empty env skip", args: []string{"--skip-kinds", "Secret,CustomResourceDefinition"}, env: map[string]string{"FLATE_SKIP_KINDS": ""}, wantCM: true},
		{name: "explicit skip wins", args: []string{"--skip-kinds", "Secret,CustomResourceDefinition", "--only-kinds", "CustomResourceDefinition"}},
		{name: "env skip wins", args: []string{"--only-kinds", "CustomResourceDefinition"}, env: map[string]string{"FLATE_SKIP_KINDS": "CustomResourceDefinition"}},
		{name: "env only", env: map[string]string{"FLATE_ONLY_KINDS": "Secret"}, wantSecret: true},
		{name: "env empty skip", env: map[string]string{"FLATE_SKIP_KINDS": ""}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "argv overrides env skip", args: []string{"--skip-kinds", ""}, env: map[string]string{"FLATE_SKIP_KINDS": "ConfigMap"}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "argv empty only overrides env", args: []string{"--only-kinds", ""}, env: map[string]string{"FLATE_ONLY_KINDS": "Secret"}, wantCM: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			args := append(
				[]string{
					"build",
					"ks",
					"apps",
					"--path",
					root,
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
					"--concurrency",
					"2",
				},
				tc.args...)
			out := harness.RunCLIStdout(t, args...)
			for kind, want := range map[string]bool{"ConfigMap": tc.wantCM, "Secret": tc.wantSecret, "CustomResourceDefinition": tc.wantCRD} {
				{
					got := strings.Contains(out, "kind: "+kind+"\n")
					assert.ErrorIf(t, got != want, "kind %s present=%t, want %t: %s", kind, got, want, out)
				}
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_OrphanedKustomizationNotDiscovered(t *testing.T) {
	src := harness.TestdataPath(t, "orphans")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	// "wired" is referenced and should pass.
	wiredLine := harness.MustExtractLine(t, out, "flux-system/wired")
	assert.ErrorIf(t, !strings.Contains(wiredLine, "✓"), "wired should pass: %s", wiredLine)
	// "orphan" must not appear anywhere in the test output — it was
	// never loaded because the kustomize graph doesn't reach it.
	assert.ErrorIf(
		t,
		strings.Contains(out, "flux-system/orphan"),
		"orphan Kustomization should be invisible to graph-driven loader; output contained reference:\n%s",
		out,
	)
}

func initGitFixture(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	repo := testutil.GitInit(t, dir)
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("kubernetes/flux/cluster.yaml", "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/x.git"}
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`))
	writeFile("kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("kubernetes/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	return filepath.Join(dir, "kubernetes", "flux"), dir
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MalformedManifestScope(t *testing.T) {
	for _, tc := range []struct {
		name      string
		graph     bool
		owned     bool
		shared    bool
		wide      bool
		suspended bool
		changed   bool
		generated bool
		external  bool
	}{
		{name: "owned graph", graph: true, owned: true},
		{name: "shared graph", graph: true, owned: true, shared: true},
		{name: "owned ad-hoc", owned: true},
		{name: "owned ad-hoc, wide scan", owned: true, wide: true},
		{name: "suspended owner", owned: true, wide: true, suspended: true},
		{name: "changed-only, unselected owner", owned: true, wide: true, changed: true},
		{name: "ResourceSet-generated owner", owned: true, wide: true, generated: true},
		{name: "suspended generated owner", owned: true, wide: true, generated: true, suspended: true},
		{name: "external-source owner", owned: true, wide: true, external: true},
		{name: "unowned graph", graph: true},
		{name: "unowned ad-hoc"},
	} {
		t.Run(tc.name, func(t *testing.T) { e2EMalformedManifestScopeName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_BootstrapSiblingNamespaceProjection(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [sibling.yaml]\ntransformers: [transformers]\n")
	testutil.WriteFile(
		t,
		root,
		"app/transformers/kustomization.yaml",
		"namespace: apps\nresources: [../../transformers]\n",
	)
	testutil.WriteFile(t, root, "transformers/kustomization.yaml", "resources: [transformer.yaml]\n")
	testutil.WriteFile(t, root, "transformers/transformer.yaml", `apiVersion: builtin
kind: NamespaceTransformer
metadata: {name: apply-ns, namespace: .invalid}
fieldSpecs:
  - path: metadata/name
    kind: Namespace
    create: true
  - path: spec/targetNamespace
    group: kustomize.toolkit.fluxcd.io
    kind: Kustomization
    create: true
`)
	testutil.WriteFile(
		t,
		root,
		"app/sibling.yaml",
		testutil.KustomizationYAML("sibling", "flux-system", "  path: ./leaf\n"),
	)
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")

	args := []string{
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
	first, _ := harness.RequireCLIOK(t, args...)
	second, _ := harness.RequireCLIOK(t, args...)
	assert.FatalIf(t, first != second, "%v", "projected bootstrap output changed between runs")
	docs, err := testutil.SplitDocs([]byte(first))
	assert.NoError(t, err)
	found := false
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		if manifest.DocKind(doc) == "ConfigMap" && name == "leaf" {
			assert.FatalIf(t, namespace != "apps", "leaf namespace = %q, want apps:\n%s", namespace, first)
			found = true
		}
	}
	assert.FatalIf(t, !found, "projected bootstrap root did not render its leaf:\n%s", first)
}

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
