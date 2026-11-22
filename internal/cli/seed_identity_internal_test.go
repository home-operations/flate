package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/cli/output"
	"github.com/home-operations/flate/internal/engine/diff"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // CLI invocations replace the default logger.
func TestRun_SeedIdentityChangedDiff(t *testing.T) {
	for _, variant := range []string{"transformer", "namespace-field", "metadata-namespace"} {
		for _, edit := range []string{"chart", "values"} {
			t.Run(variant+"/"+edit, func(t *testing.T) {
				testSeedIdentityChangedDiff(t, variant, edit)
			})
		}
	}
	t.Run("transformer/namespace_move", func(t *testing.T) {
		testSeedIdentityChangedDiff(t, "transformer", "namespace")
	})
}

func testSeedIdentityChangedDiff(t *testing.T, variant, edit string) {
	t.Helper()
	root := t.TempDir()
	testutil.CopyTree(t, "../../testdata/seed-identity/repo", root)
	repo := testutil.GitInit(t, root)
	_, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{"https://github.com/example/example.git"},
	})
	assert.NoError(t, err)
	for _, cluster := range []string{"cluster-a", "cluster-b"} {
		dir := filepath.Join("clusters", cluster, "definition")
		switch variant {
		case "namespace-field":
			testutil.WriteFile(t, root, filepath.Join(dir, "kustomization.yaml"),
				"namespace: "+cluster+"\nresources: [helm-release.yaml]\n")
		case "metadata-namespace":
			name := filepath.Join(dir, "helm-release.yaml")
			body, readErr := os.ReadFile(filepath.Join(root, name))
			assert.NoError(t, readErr)
			testutil.WriteFile(t, root, name, strings.Replace(
				string(body), "  name: demo\n", "  name: demo\n  namespace: "+cluster+"\n", 1,
			))
		}
	}
	commit := &git.CommitOptions{Author: &object.Signature{Name: "Fixture", Email: "fixture@example.com"}}
	base := testutil.GitCommitAll(t, repo, "base", commit)
	fullCache, entry := t.TempDir(), filepath.Join(root, "clusters", "management")
	flags := []string{"--path", entry, "--concurrency", "2", "--no-progress", "--log-level", "debug"}
	fullArgs := append([]string{"build", "all", "--cache-dir", fullCache}, flags...)
	before := runCLI(t, fullArgs...)
	assert.FatalIf(t, before.code != 0, "base full render exited %d: %s", before.code, before.stderr)

	file, old, replacement := "charts/demo/templates/configmap.yaml", "\ndata:", "\n  labels: {changed: \"yes\"}\ndata:"
	switch edit {
	case "values":
		file = "clusters/cluster-b/definition/helm-release.yaml"
		old, replacement = "hello from cluster-b", "bonjour from cluster-b"
	case "namespace":
		file = "clusters/cluster-b/definition/kustomization.yaml"
		old, replacement = "cluster-b", "moved-b"
	}
	body, err := os.ReadFile(filepath.Join(root, file))
	assert.NoError(t, err)
	testutil.WriteFile(t, root, file, strings.Replace(string(body), old, replacement, 1))
	testutil.GitCommitAll(t, repo, "head", commit)
	after := runCLI(t, fullArgs...)
	assert.FatalIf(t, after.code != 0, "head full render exited %d: %s", after.code, after.stderr)

	full, err := diff.Compare([]diff.Group{{
		Orig: seedIdentitySnapshot(t, before.stdout), Current: seedIdentitySnapshot(t, after.stdout),
	}}, diff.Options{StripAttrs: diff.DefaultStripAttrs, StripFields: diff.DefaultStripFields})
	assert.NoError(t, err)
	var expected bytes.Buffer
	assert.NoError(t, full.Write(&expected, nil))
	logSeedRenders(t, "full base", &before)
	logSeedRenders(t, "full head", &after)
	args := append([]string{"diff", "all", "--base", base.String(), "--cache-dir", t.TempDir()}, flags...)
	for _, cache := range []string{"fresh", "reused"} {
		actual := runCLI(t, args...)
		logSeedRenders(t, cache, &actual)
		assert.FatalIf(t, actual.code != 0, "changed-only diff exited %d: %s", actual.code, actual.stderr)
		assert.Diff(t, actual.stdout, expected.String())
		assert.FatalIf(t, !strings.Contains(actual.stdout, "ConfigMap") ||
			!strings.Contains(actual.stdout, "cluster-b"), "cluster B rendered change missing: %s", actual.stdout)
		switch edit {
		case "chart":
			assert.FatalIf(
				t,
				!strings.Contains(actual.stdout, "cluster-a"),
				"cluster A chart change missing: %s",
				actual.stdout,
			)
		case "values":
			assert.FatalIf(t, !strings.Contains(actual.stdout, "HelmRelease") ||
				strings.Contains(actual.stdout, "cluster-a"), "unexpected values diff: %s", actual.stdout)
		case "namespace":
			assert.FatalIf(t, !strings.Contains(actual.stdout, "moved-b") ||
				strings.Contains(actual.stdout, "cluster-a"), "unexpected namespace diff: %s", actual.stdout)
		}
	}
}

func seedIdentitySnapshot(t *testing.T, yaml string) diff.Snapshot {
	t.Helper()
	docs, err := manifest.DecodeDocs(strings.NewReader(yaml), manifest.RequiredDocuments)
	assert.NoError(t, err)
	documents := make([]map[string]any, 0, len(docs))
	for i := range docs {
		documents = append(documents, docs[i].Document)
	}
	return diff.Snapshot{Manifests: []manifest.DocumentBatch{{Documents: documents}}}
}

//nolint:paralleltest // CLI invocations replace the default logger.
func TestRun_SeedIdentityNamedBuild(t *testing.T) {
	for _, scenario := range []string{
		"copied artifact", "copied artifact narrow", "target namespace", "parent namespace", "patched parent namespace", "rename",
		"disjoint owner", "patched owner", "patched path", "patched suspension", "namespace override",
		"unlisted namespace", "unlisted patch", "unrelated patch", "unrelated test patch", "unrelated test patch file",
		"unrelated component", "component resource configuration", "component namespace", "patched owner file",
		"patched application fields", "patched component", "package root", "patched package root",
		"component cycle", "local config owner", "generated owner", "name suffix", "different source",
	} {
		t.Run(scenario, func(t *testing.T) {
			testSeedIdentityNamedScenario(t, scenario)
		})
	}
	t.Run("transformed kustomization and namespace selection", testSeedIdentityTransformedSelection)
}

func testSeedIdentityNamedScenario(t *testing.T, scenario string) {
	t.Helper()
	fixture := seedIdentityNamedFixture(t, scenario)
	root, entry := fixture.root, fixture.entry
	namespace, name := fixture.namespace, fixture.name
	const management = "clusters/management"
	const definition = "clusters/cluster-b/definition"

	flags := []string{"--namespace", namespace, "--concurrency", "2", "--no-progress"}
	fullCache := t.TempDir()
	full := seedIdentityFullBuild(t, &fixture, scenario, fullCache)
	logSeedRenders(t, "full", &full)
	cache := t.TempDir()
	args := append(
		[]string{"build", "hr", name, "--path", entry, "--cache-dir", cache, "--log-level", "debug"},
		flags...)
	cold, warm := runCLI(t, args...), runCLI(t, args...)
	logSeedRenders(t, "fresh", &cold)
	logSeedRenders(t, "reused", &warm)
	var diagnostic string
	switch scenario {
	case "component cycle":
		diagnostic = "cycle detected"
	case "unrelated patch", "unrelated component":
		diagnostic = "/data/missing/value"
	case "unrelated test patch", "unrelated test patch file":
		diagnostic = "testing value /metadata/name failed"
	}
	if diagnostic != "" {
		assert.Diff(t, full.code, 1)
		assert.Diff(t, full.stdout, "")
		assert.FatalIf(t, !strings.Contains(full.stderr, diagnostic), "full build lost %q: %s", diagnostic, full.stderr)
		for _, result := range []*cliResult{&cold, &warm} {
			assert.Diff(t, result.code, full.code)
			assert.Diff(t, result.stdout, full.stdout)
			assert.FatalIf(
				t,
				!strings.Contains(result.stderr, diagnostic),
				"lookup lost %q: %s",
				diagnostic,
				result.stderr,
			)
		}
		return
	}
	want := seedIdentityChartOutput(t, &fixture, full.stdout)
	assert.FatalIf(t, full.code != 0 || want == "", "full build: %+v", full)
	assert.FatalIf(t, cold.code != 0, "cold named render exited %d: %s", cold.code, cold.stderr)
	assert.Diff(t, cold.stdout, want)
	assert.FatalIf(t, warm.code != 0, "warm named render exited %d: %s", warm.code, warm.stderr)
	assert.Diff(t, warm.stdout, want)
	switch scenario {
	case "parent namespace", "patched parent namespace":
		argsWithFullCache := append([]string{"build", "hr", name, "--path", entry, "--cache-dir", fullCache}, flags...)
		reused := runCLI(t, argsWithFullCache...)
		assert.FatalIf(t, reused.code != 0, "named render with full cache exited %d: %s", reused.code, reused.stderr)
		assert.Diff(t, reused.stdout, want)
	case "copied artifact", "copied artifact narrow":
		assertCopiedSeedIdentity(t, &fixture, want, cold.stdout)
	case "component namespace":
		const producer = `msg="orchestrator: entry accepted" ` +
			`producer=Kustomization.kustomize.toolkit.fluxcd.io/flux-system/cluster-b`
		for _, stderr := range []string{cold.stderr, warm.stderr} {
			assert.FatalIf(t, !strings.Contains(stderr, producer), "rendered producer not accepted: %s", stderr)
		}
	case "patched owner":
		for _, command := range [][]string{{"build", "hr", name}, {"build", "hr"}} {
			result := runCLI(t, append(command, "--path", filepath.Join(root, management),
				"--cache-dir", cache, "--namespace", namespace, "--concurrency", "2", "--no-progress",
				"--log-level", "debug")...)
			assert.FatalIf(t, result.code != 0 || result.stdout == "", "patched owner lookup %v: %+v", command, result)
			assert.Diff(t, result.stdout, want)
		}
	case "namespace override", "patched path":
		miss := runCLI(t, "build", "hr", name, "--path", entry, "--cache-dir", t.TempDir(),
			"--namespace", "native", "--concurrency", "2", "--no-progress")
		assert.Diff(t, miss.code, 1)
		assert.Diff(t, miss.stdout, "")
		assert.FatalIf(t, !strings.Contains(miss.stderr, `no HelmRelease named "demo" in --path`),
			"overridden namespace result: %+v", miss)
	}
	parentNamespace := scenario == "parent namespace" || scenario == "patched parent namespace"
	if parentNamespace || scenario == "target namespace" {
		scopeArgs := append(
			[]string{"build", "hr", "--path", entry, "--cache-dir", t.TempDir(), "--log-level", "debug"}, flags...)
		testSeedIdentityNamespaceSelection(t, scopeArgs, want)
	}
	if parentNamespace || strings.HasPrefix(scenario, "unlisted ") {
		assert.FatalIf(t, !strings.Contains(full.stdout, "    helm.toolkit.fluxcd.io/name: sibling\n"),
			"full build did not include the outside sibling: %s", full.stdout)
		for _, stderr := range []string{cold.stderr, warm.stderr} {
			assert.FatalIf(
				t,
				strings.Contains(stderr, `msg="kustomize: build" path=./`+definition),
				"unlisted owner: %s",
				stderr,
			)
		}
		missArgs := append(
			[]string{"build", "hr", "sibling", "--path", entry, "--cache-dir", t.TempDir()},
			flags...)
		for _, state := range []string{"fresh", "reused"} {
			miss := runCLI(t, missArgs...)
			logSeedRenders(t, "sibling "+state, &miss)
			assert.Diff(t, miss.code, 1)
			assert.Diff(t, miss.stdout, "")
			assert.FatalIf(t, !strings.Contains(miss.stderr, `no HelmRelease named "sibling" in --path`),
				"outside sibling result: %+v", miss)
		}
	}
}

func testSeedIdentityNamespaceSelection(t *testing.T, args []string, want string) {
	t.Helper()
	for _, state := range []string{"fresh", "reused"} {
		scope := runCLI(t, args...)
		logSeedRenders(t, "namespace "+state, &scope)
		assert.FatalIf(t, scope.code != 0, "namespace selection: %+v", scope)
		assert.Diff(t, scope.stdout, want)
	}
}

func seedIdentityFullBuild(t *testing.T, fixture *seedIdentityFixture, scenario, cache string) cliResult {
	t.Helper()
	fullPath := "clusters/management"
	switch scenario {
	case "parent namespace", "patched parent namespace", "local config owner", "different source",
		"unlisted namespace", "unlisted patch":
		fullPath = "clusters/cluster-b/definition"
	}
	return runCLI(t, "build", "all", "--only-kinds", "ConfigMap", "--path", filepath.Join(fixture.root, fullPath),
		"--cache-dir", cache, "--concurrency", "2", "--no-progress", "--log-level", "debug")
}

func seedIdentityChartOutput(t *testing.T, fixture *seedIdentityFixture, stdout string) string {
	t.Helper()
	var matched strings.Builder
	for section := range strings.SplitSeq(stdout, "---\n") {
		if strings.Contains(section, "    helm.toolkit.fluxcd.io/name: "+fixture.name+"\n") &&
			strings.Contains(section, "    helm.toolkit.fluxcd.io/namespace: "+fixture.namespace+"\n") {
			matched.WriteString("---\n")
			matched.WriteString(section)
		}
	}
	return matched.String()
}

func logSeedRenders(t *testing.T, cache string, result *cliResult) {
	t.Helper()
	var paths []string
	for line := range strings.SplitSeq(result.stderr, "\n") {
		if _, path, ok := strings.Cut(line, `msg="kustomize: build" path=`); ok {
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	t.Logf("Kustomize builds %s (%d): %v", cache, len(paths), paths)
}

type seedIdentityFixture struct {
	root, entry, namespace, name string
}

func seedIdentityNamedFixture(t *testing.T, scenario string) seedIdentityFixture {
	t.Helper()
	root := t.TempDir()
	testutil.CopyTree(t, "../../testdata/seed-identity/repo", root)
	repo := testutil.GitInit(t, root)
	_, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{"https://github.com/example/example.git"},
	})
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "clusters/cluster-a/definition/kustomization.yaml",
		"namespace: cluster-a\nresources: [helm-release.yaml]\n")

	const management = "clusters/management"
	const definition = "clusters/cluster-b/definition"
	const ownerFile = management + "/apps/cluster-b.yaml"
	const component = "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n"
	release, err := os.ReadFile(filepath.Join(root, definition, "helm-release.yaml"))
	assert.NoError(t, err)
	namespace, name := "cluster-b", "demo"
	entry := filepath.Join(root, definition, "leaf")
	recipe := "resources: [leaf/release.yaml]\n"
	switch scenario {
	case "copied artifact", "copied artifact narrow":
		namespace, name = "default", "plex"
		entry = filepath.Join(root, management)
		copiedOwner := ownerFile
		generator := management + "/apps/generator.yaml"
		if scenario == "copied artifact narrow" {
			entry = filepath.Join(entry, "apps")
			copiedOwner, generator = management+"/copied-owner.yaml", management+"/copied-generator.yaml"
			testutil.WriteFile(t, root, management+"/kustomization.yaml",
				"resources: [flux, apps, copied-owner.yaml, copied-generator.yaml]\n")
			testutil.WriteFile(t, root, management+"/apps/kustomization.yaml",
				"resources: [marker.yaml]\n")
			testutil.WriteFile(t, root, management+"/apps/marker.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker}\n")
		} else {
			testutil.WriteFile(t, root, management+"/apps/kustomization.yaml",
				"resources: [cluster-a.yaml, cluster-b.yaml, generator.yaml]\n")
		}
		release = bytes.Replace(release, []byte("  name: demo\n"), []byte("  name: plex\n"), 1)
		testutil.WriteFile(
			t,
			root,
			copiedOwner,
			testutil.KustomizationYAML("plex", "default", `  path: ./copied
  targetNamespace: default
  sourceRef: {kind: ExternalArtifact, name: media, namespace: flux-system}
`),
		)
		testutil.WriteFile(
			t,
			root,
			generator,
			`apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: media, namespace: flux-system}
spec:
  sources: [{alias: repo, kind: GitRepository, name: example}]
  artifacts:
  - name: media
    copy:
    - from: "@repo/clusters/cluster-b/definition/**"
      to: "@artifact/copied/"
`,
		)
	case "target namespace", "rename", "namespace override":
		owner, err := os.ReadFile(filepath.Join(root, ownerFile))
		assert.NoError(t, err)
		testutil.WriteFile(t, root, definition+"/ks.yaml", string(owner)+"  targetNamespace: cluster-b\n")
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml",
			"resources: [cluster-a.yaml, ../../cluster-b/definition/ks.yaml]\n")
		switch scenario {
		case "namespace override":
			recipe = "namespace: native\n" + recipe
		case "rename":
			name = "renamed"
			recipe += `patches:
- target:
    kind: HelmRelease
    name: demo
    annotationSelector: '!config.kubernetes.io/origin'
  patch: |-
    - op: replace
      path: /metadata/name
      value: renamed
`
		}
	case "disjoint owner", "patched owner", "patched owner file", "patched path", "patched suspension",
		"package root", "patched package root":
		body, err := os.ReadFile(filepath.Join(root, ownerFile))
		assert.NoError(t, err)
		owner := string(body)
		target := namespace
		var patchPath, patchValue string
		switch scenario {
		case "patched owner", "patched owner file", "patched package root":
			target = "other"
			patchPath, patchValue = "/spec/targetNamespace", namespace
		case "patched path":
			owner = strings.Replace(owner, "path: ./"+definition, "path: ./unrelated", 1)
			patchPath, patchValue = "/spec/path", "./"+definition
		case "patched suspension":
			owner += "  suspend: true\n"
			patchPath, patchValue = "/spec/suspend", "false"
		}
		switch scenario {
		case "package root", "patched package root":
			entry = filepath.Join(root, definition)
			fallthrough
		case "patched path", "patched suspension":
			recipe = "namespace: native\n" + recipe
		}
		if patchPath != "" {
			patch := "- op: replace\n  path: " + patchPath + "\n  value: " + patchValue + "\n"
			testutil.WriteFile(t, root, management+"/apps/kustomization.yaml", `resources: [cluster-b.yaml]
patches:
- target: {kind: Kustomization, name: cluster-b}
`+seedIdentityPatch(t, root, scenario == "patched owner file", patch))
		}
		testutil.WriteFile(t, root, ownerFile, owner+"  targetNamespace: "+target+"\n")
	case "unrelated patch", "unrelated test patch", "unrelated test patch file", "unrelated component":
		recipe = "namespace: cluster-b\nresources: [leaf/release.yaml, ../../management/apps]\n"
		resources := "resources: [cluster-a.yaml, unrelated.yaml]\n"
		testutil.WriteFile(t, root, management+"/apps/unrelated.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\ndata: {present: old}\n")
		patch := `patches:
- target: {kind: ConfigMap, name: unrelated}
  patch: |-
    - op: replace
      path: /data/missing/value
      value: new
`
		switch scenario {
		case "unrelated test patch", "unrelated test patch file":
			patch = `patches:
- target: {group: kustomize.toolkit.fluxcd.io, kind: Kustomization, name: cluster-a}
` + seedIdentityPatch(t, root, scenario == "unrelated test patch file",
				"- op: test\n  path: /metadata/name\n  value: expected-elsewhere\n")
		case "unrelated component":
			testutil.WriteFile(t, root, management+"/apps/component/kustomization.yaml", component+patch)
			patch = "components: [component]\n"
		}
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml", resources+patch)
	case "component resource configuration":
		entry = filepath.Join(root, definition)
		recipe = "namespace: native\n" + recipe
		testutil.WriteFile(
			t, root, ownerFile,
			testutil.KustomizationYAML("cluster-b", "flux-system", `  path: ./unrelated
  targetNamespace: cluster-b
  sourceRef: {kind: GitRepository, name: example}
`),
		)
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml",
			"resources: [cluster-b.yaml]\ncomponents: [component]\ncommonLabels: {path: ./"+definition+"}\n")
		testutil.WriteFile(t, root, management+"/apps/component/kustomization.yaml", component+"resources: [base]\n")
		const base = management + "/apps/component/base"
		testutil.WriteFile(t, root, base+"/kustomization.yaml", "configurations: [routing.yaml]\n")
		testutil.WriteFile(t, root, base+"/routing.yaml", `commonLabels:
- group: kustomize.toolkit.fluxcd.io
  kind: Kustomization
  path: spec
`)
	case "component namespace":
		entry = filepath.Join(root, definition)
		recipe = "namespace: native\n" + recipe
		testutil.WriteFile(
			t, root, ownerFile,
			testutil.KustomizationYAML(
				"cluster-b",
				"unrelated",
				"  path: ./"+definition+"\n  targetNamespace: cluster-b\n  sourceRef: {kind: GitRepository, name: example}\n",
			),
		)
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml",
			"resources: [cluster-b.yaml]\ncomponents: [component]\n")
		testutil.WriteFile(
			t,
			root,
			management+"/apps/component/kustomization.yaml",
			component+"namespace: flux-system\n",
		)
	case "component cycle":
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml",
			"resources: [cluster-b.yaml]\ncomponents: [a]\n")
		testutil.WriteFile(t, root, management+"/apps/a/kustomization.yaml", component+"components: [../b]\n")
		testutil.WriteFile(t, root, management+"/apps/b/kustomization.yaml", component+"components: [../a]\n")
	case "patched application fields", "patched component":
		recipe = "namespace: native\n" + recipe
		testutil.WriteFile(t, root, ownerFile,
			testutil.KustomizationYAML("cluster-b", "flux-system", "  targetNamespace: cluster-b\n"))
		patch := `patches:
- target: {kind: Kustomization, name: cluster-b}
  patch: |-
    - op: add
      path: /spec/path
      value: ./clusters/cluster-b/definition
    - op: add
      path: /spec/sourceRef
      value: {kind: GitRepository, name: example}
`
		if scenario == "patched component" {
			testutil.WriteFile(t, root, management+"/apps/component/kustomization.yaml", component+patch)
			patch = "components: [component]\n"
		}
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml", "resources: [cluster-b.yaml]\n"+patch)
	case "generated owner":
		recipe = "namespace: native\n" + recipe
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml", `generatorOptions:
  disableNameSuffixHash: true
configMapGenerator:
- name: generated
patches:
- target: {kind: ConfigMap, name: generated}
  patch: |-
    - op: replace
      path: /apiVersion
      value: kustomize.toolkit.fluxcd.io/v1
    - op: replace
      path: /kind
      value: Kustomization
    - op: add
      path: /metadata/namespace
      value: flux-system
    - op: add
      path: /spec
      value:
        interval: 1m
        path: ./clusters/cluster-b/definition
        targetNamespace: cluster-b
        sourceRef: {kind: GitRepository, name: example}
`)
	case "name suffix":
		name = "demo-rendered"
		recipe = "namespace: cluster-b\nnameSuffix: -rendered\n" + recipe
	case "different source":
		recipe = "namespace: cluster-b\n" + recipe
		testutil.WriteFile(t, root, ownerFile, testutil.KustomizationYAML(
			"cluster-b",
			"flux-system",
			"  path: ./"+definition+"\n  targetNamespace: blocked\n  sourceRef: {kind: GitRepository, name: foreign}\n",
		))
		testutil.WriteFile(t, root, management+"/apps/kustomization.yaml",
			"resources: [cluster-b.yaml, foreign.yaml]\n")
		testutil.WriteFile(t, root, management+"/apps/foreign.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {namespace: flux-system, name: foreign}
spec: {interval: 1m, url: 'file:///unavailable'}
`)
	case "local config owner":
		testutil.WriteFile(t, root, ownerFile, strings.Replace(
			testutil.KustomizationYAML("cluster-b", "flux-system", "  path: ./"+definition+"\n"+
				"  targetNamespace: blocked\n  sourceRef: {kind: GitRepository, name: example}\n"),
			"spec:", "  annotations: {config.kubernetes.io/local-config: 'true'}\nspec:", 1,
		))
		recipe = "namespace: cluster-b\n" + recipe
	case "parent namespace", "patched parent namespace", "unlisted namespace", "unlisted patch":
		recipe = seedIdentityParentRecipe(t, root, scenario, release)
	}
	testutil.WriteFile(t, root, definition+"/leaf/release.yaml", string(release))
	testutil.WriteFile(t, root, definition+"/kustomization.yaml", recipe)
	testutil.GitCommitAll(t, repo, "fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "Fixture", Email: "fixture@example.com"}})

	return seedIdentityFixture{root: root, entry: entry, namespace: namespace, name: name}
}

func seedIdentityParentRecipe(t *testing.T, root, scenario string, release []byte) string {
	t.Helper()
	const definition = "clusters/cluster-b/definition"
	const recipe = "namespace: cluster-b\nresources: [leaf/release.yaml, outside/sibling.yaml]\n"
	testutil.WriteFile(t, root, "clusters/management/apps/kustomization.yaml", "resources: [cluster-a.yaml]\n")
	sibling := bytes.Replace(release, []byte("  name: demo\n"), []byte("  name: sibling\n"), 1)
	testutil.WriteFile(t, root, definition+"/outside/sibling.yaml", string(sibling))
	switch scenario {
	case "parent namespace":
		return recipe
	case "patched parent namespace":
		return recipe + `patches:
- target: {kind: HelmRelease, name: demo}
  patch: '[{"op":"add","path":"/metadata/labels","value":{"scope":"leaf"}}]'
`
	}
	target := "unrelated"
	var patch string
	if scenario == "unlisted patch" {
		target = "cluster-b"
		patch = `  patches:
  - target: {kind: HelmRelease, name: demo}
    patch: '[{"op":"replace","path":"/metadata/missing/name","value":"broken"}]'
`
	}
	testutil.WriteFile(t, root, definition+"/unlisted.yaml", testutil.KustomizationYAML(
		"unlisted", "flux-system", "  path: ./"+definition+"\n"+
			"  targetNamespace: "+target+"\n  sourceRef: {kind: GitRepository, name: example}\n"+patch))
	return recipe
}

func seedIdentityPatch(t *testing.T, root string, file bool, operations string) string {
	t.Helper()
	if file {
		testutil.WriteFile(t, root, "clusters/management/apps/patch.yaml", operations)
		return "  path: patch.yaml\n"
	}
	return "  patch: |-\n    " + strings.ReplaceAll(strings.TrimSuffix(operations, "\n"), "\n", "\n    ") + "\n"
}

func assertCopiedSeedIdentity(t *testing.T, fixture *seedIdentityFixture, full, cold string) {
	t.Helper()
	entry, name, namespace := fixture.entry, fixture.name, fixture.namespace
	want := seedIdentitySnapshot(t, full).Manifests[0].Documents
	cache := t.TempDir()
	for _, state := range []string{"fresh", "reused"} {
		owner := runCLI(t, "build", "ks", "plex", "-n", "default", "--only-kinds", "HelmRelease",
			"--path", entry, "--cache-dir", cache, "--concurrency", "2", "--no-progress", "--log-level", "debug")
		logSeedRenders(t, "copied owner "+state, &owner)
		assert.FatalIf(t, owner.code != 0, "copied package render: %+v", owner)
		declarations := seedIdentitySnapshot(t, owner.stdout).Manifests[0].Documents
		assert.FatalIf(t, len(declarations) != 1, "copied package declarations: %s", owner.stdout)
		id := manifest.DocIdentity(declarations[0])
		assert.Diff(t, id.Name, name)
		assert.Diff(t, id.Namespace, namespace)
	}
	want = slices.DeleteFunc(want, func(document map[string]any) bool {
		return manifest.DocIdentity(document).Namespace != namespace
	})
	assert.FatalIf(t, len(want) != 1, "copied chart output missing: %s", full)
	assert.Diff(t, seedIdentitySnapshot(t, cold).Manifests[0].Documents, want)
}

func testSeedIdentityTransformedSelection(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	testutil.CopyTree(t, "../../testdata/seed-identity/repo", root)
	repo := testutil.GitInit(t, root)
	_, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{"https://github.com/example/example.git"},
	})
	assert.NoError(t, err)
	const definition = "clusters/cluster-b/definition"
	const recipe = "namespace: cluster-b\nnamePrefix: rendered-\nresources: [leaf/ks.yaml, leaf/release.yaml]\n"
	testutil.WriteFile(t, root, definition+"/kustomization.yaml", recipe)
	release, err := os.ReadFile(filepath.Join(root, definition, "helm-release.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, root, definition+"/leaf/release.yaml", string(release))
	testutil.WriteFile(t, root, definition+"/leaf/ks.yaml", testutil.KustomizationYAML(
		"workload", "", "  path: ./"+definition+"/workload\n"+
			"  sourceRef: {kind: GitRepository, name: example, namespace: flux-system}\n"))
	testutil.WriteFile(t, root, definition+"/workload/kustomization.yaml", "resources: [marker.yaml]\n")
	testutil.WriteFile(t, root, definition+"/workload/marker.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: cluster-b}\n")
	testutil.GitCommitAll(t, repo, "fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "Fixture", Email: "fixture@example.com"}})
	full := runCLI(t, "build", "all", "--only-kinds", "ConfigMap", "--path", filepath.Join(root, definition),
		"--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress", "--log-level", "debug")
	logSeedRenders(t, "full", &full)
	assert.FatalIf(t, full.code != 0, "full render exited %d: %s", full.code, full.stderr)
	fullDocs := seedIdentitySnapshot(t, full.stdout).Manifests[0].Documents
	for _, tc := range []struct {
		args       []string
		kind, name string
	}{
		{args: []string{"build", "ks", "rendered-workload"}, kind: "ConfigMap", name: "workload"},
		{args: []string{"build", "hr"}, kind: "ConfigMap", name: "demo"},
	} {
		expected := slices.DeleteFunc(slices.Clone(fullDocs), func(doc map[string]any) bool {
			id := manifest.DocIdentity(doc)
			return id.Kind != tc.kind || id.Name != tc.name || id.Namespace != "cluster-b"
		})
		assert.Equal(t, len(expected), 1)
		var want bytes.Buffer
		encoder := output.Encoder{Writer: &want, Format: output.FormatYAML}
		assert.NoError(t, encoder.DocumentGroups([]output.DocumentGroup{{Documents: expected}}))
		args := append(slices.Clone(tc.args), "-n", "cluster-b", "--path", filepath.Join(root, definition, "leaf"),
			"--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress", "--log-level", "debug")
		for _, state := range []string{"fresh", "reused"} {
			narrow := runCLI(t, args...)
			logSeedRenders(t, tc.name+" "+state, &narrow)
			assert.FatalIf(t, narrow.code != 0, "narrow render exited %d: %s", narrow.code, narrow.stderr)
			assert.FatalIf(t, narrow.stdout == "", "narrow render emitted no documents: %s", narrow.stderr)
			assert.Diff(t, narrow.stdout, want.String())
			docs := seedIdentitySnapshot(t, narrow.stdout).Manifests[0].Documents
			assert.Equal(t, len(docs), 1)
			id := manifest.DocIdentity(docs[0])
			assert.Equal(t, id.Kind, tc.kind)
			assert.Equal(t, id.Name, tc.name)
			assert.Equal(t, id.Namespace, "cluster-b")
		}
	}
}

//nolint:paralleltest // CLI invocations replace the default logger.
func TestRun_SeedIdentityLookupOutcome(t *testing.T) {
	for _, tc := range []struct {
		name, patch, diagnostic string
		fullCode, builds        int
	}{
		{
			name: "empty",
			patch: `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: authored, namespace: apps}
$patch: delete`,
			diagnostic: `no HelmRelease named "rendered" in --path`,
			builds:     1,
		},
		{
			name:       "failed",
			patch:      `[{"op":"replace","path":"/metadata/missing/name","value":"rendered"}]`,
			diagnostic: "/metadata/missing/name",
			fullCode:   1,
			builds:     1,
		},
		{name: "malformed recipe", diagnostic: "error converting YAML to JSON", fullCode: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "package/sibling/kustomization.yaml", "resources: [absent.yaml]\n")
			testutil.WriteFile(t, root, "package/leaf/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: authored, namespace: apps}
spec:
  interval: 1m
  chartRef: {kind: OCIRepository, name: absent}
`)
			recipe := `resources: [leaf/release.yaml]
patches:
- target: {kind: HelmRelease, name: authored}
  patch: |-
    ` + strings.ReplaceAll(tc.patch, "\n", "\n    ") + "\n"
			if tc.patch == "" {
				recipe = "resources: [leaf/release.yaml\n"
			}
			testutil.WriteFile(t, root, "package/kustomization.yaml", recipe)
			full := runCLI(t, "build", "all", "--only-kinds", "HelmRelease", "--concurrency", "2",
				"--path", filepath.Join(root, "package"), "--cache-dir", t.TempDir(), "--no-progress",
				"--log-level", "debug")
			logSeedRenders(t, "full", &full)
			assert.Diff(t, full.stdout, "")
			assert.Diff(t, full.code, tc.fullCode)
			assert.FatalIf(t, tc.fullCode != 0 && !strings.Contains(full.stderr, tc.diagnostic),
				"full result: %+v", full)
			cache := t.TempDir()
			for _, state := range []string{"fresh", "reused"} {
				result := runCLI(t, "build", "hr", "rendered", "-n", "apps", "--concurrency", "2",
					"--path", filepath.Join(root, "package", "leaf"), "--cache-dir", cache, "--no-progress",
					"--log-level", "debug")
				logSeedRenders(t, state, &result)
				if state == "fresh" || tc.fullCode != 0 {
					assert.Diff(t, strings.Count(result.stderr, `msg="kustomize: build" path=package`+"\n"), tc.builds)
				}
				assert.FatalIf(t, strings.Contains(result.stderr, `msg="kustomize: build" path=package/sibling`),
					"unrelated package was rendered: %s", result.stderr)
				assert.Diff(t, result.code, 1)
				assert.Diff(t, result.stdout, full.stdout)
				assert.FatalIf(t, !strings.Contains(result.stderr, tc.diagnostic), "lookup result: %+v", result)
				assert.FatalIf(t, tc.fullCode != 0 && strings.Contains(result.stderr, "no HelmRelease named"),
					"failure reported as absent: %s", result.stderr)
			}
		})
	}
	t.Run("different target namespace", testSeedIdentityDifferentTargetNamespace)
	testSeedIdentityOutsideLookups(t)
}

func testSeedIdentityDifferentTargetNamespace(t *testing.T) {
	root := t.TempDir()
	testutil.CopyTree(t, "../../testdata/seed-identity/repo", root)
	repo := testutil.GitInit(t, root)
	_, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{"https://github.com/example/example.git"},
	})
	assert.NoError(t, err)
	for _, namespace := range []string{"cluster-a", "cluster-b"} {
		extra := "  targetNamespace: " + namespace + "\n"
		if namespace == "cluster-b" {
			extra += `  patches:
  - target: {kind: HelmRelease, name: demo}
    patch: '[{"op":"replace","path":"/metadata/missing/name","value":"broken"}]'
`
		}
		file := "clusters/management/apps/" + namespace + ".yaml"
		data, err := os.ReadFile(filepath.Join(root, file))
		assert.NoError(t, err)
		testutil.WriteFile(t, root, file, string(data)+extra)
	}
	args := []string{
		"build", "hr", "-n", "cluster-a", "demo",
		"--path", filepath.Join(root, "clusters", "management"),
		"--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress", "--log-level", "debug",
	}
	for _, state := range []string{"fresh", "reused"} {
		r := runCLI(t, args...)
		logSeedRenders(t, state, &r)
		assert.FatalIf(t, r.code != 0, "lookup result: %+v", r)
		const built = `msg="kustomize: build" path=./clusters/`
		if state == "fresh" {
			assert.Diff(t, strings.Count(r.stderr, built+"cluster-a/definition\n"), 1)
		}
		assert.Diff(t, strings.Count(r.stderr, built+"cluster-b/definition\n"), 0)
		assert.Diff(t, r.stdout, `---
apiVersion: v1
data:
  greeting: hello from cluster-a
kind: ConfigMap
metadata:
  labels:
    helm.toolkit.fluxcd.io/name: demo
    helm.toolkit.fluxcd.io/namespace: cluster-a
  name: demo
  namespace: cluster-a
`)
	}
}
