package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/testfixture"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_EnclosingPackageScope(t *testing.T) {
	t.Parallel()
	const apps = "kubernetes/apps"
	const media = apps + "/media"
	const plain = media + "/plain"
	const syncOrigin = "https://github.com/example/fleet.git"
	const deploymentHeader = `apiVersion: apps/v1
kind: Deployment
metadata:
`
	const rootStamp = `  annotations:
    review: accepted-root
  labels:
    kustomize.toolkit.fluxcd.io/name: flux-system
    kustomize.toolkit.fluxcd.io/namespace: flux-system
`
	const deploymentBody = `  name: plain
  namespace: default
spec:
  selector:
    matchLabels:
      app: plain
  template:
    metadata:
      labels:
        app: plain
    spec:
      containers:
      - image: busybox
        name: c
`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: bad, namespace: media}
spec: invalid
`))
	}))
	t.Cleanup(server.Close)
	for _, tc := range []struct {
		name, requested, appsResources, mediaResources string
		instancePath, extraPatches                     string
		extra                                          map[string]string
		wantRoot                                       bool
		probes                                         uint64
	}{
		{
			name: "reached authored package", requested: media, probes: 1,
			appsResources: "flux-instance.yaml, media, media/unlisted.yaml", mediaResources: "plain/deploy.yaml",
			instancePath: apps + "/flux-instance.yaml", wantRoot: true,
		},
		{
			name: "reached bare directory", requested: plain, probes: 1,
			appsResources: "flux-instance.yaml, media", mediaResources: "plain/deploy.yaml",
			instancePath: apps + "/flux-instance.yaml", wantRoot: true,
		},
		{
			name: "repository root enclosing package", requested: plain, probes: 2,
			appsResources: "media/plain/deploy.yaml", instancePath: "clusters/main/flux-instance.yaml", wantRoot: true,
			extra: map[string]string{
				"kustomization.yaml": "resources: [clusters/main/flux-instance.yaml, kubernetes/apps]\n",
			},
		},
		{
			name: "enclosing package with decode failure", requested: media, probes: 2,
			appsResources: "media", mediaResources: "plain/deploy.yaml",
			instancePath: "kubernetes/flux-instance.yaml", wantRoot: true,
			extra: map[string]string{
				"kubernetes/kustomization.yaml": "resources: [flux-instance.yaml, apps, " + server.URL + "/bad.yaml]\n",
			},
		},
		{
			name: "nested packages with the same instance", requested: media, probes: 2,
			appsResources: "flux-instance.yaml, media", mediaResources: "plain/deploy.yaml",
			instancePath: apps + "/flux-instance.yaml", wantRoot: true,
			extra: map[string]string{"kubernetes/kustomization.yaml": "resources: [apps]\n"},
		},
		{
			name: "runnable sibling in the declaring package", requested: media, probes: 2,
			appsResources: "media", mediaResources: "plain/deploy.yaml",
			instancePath: "kubernetes/flux-instance.yaml", wantRoot: true,
			extra: map[string]string{
				"kubernetes/kustomization.yaml": "resources: [flux-instance.yaml, sibling.yaml, apps]\n",
				"kubernetes/sibling.yaml": testutil.KustomizationYAML("sibling", "apps",
					"  path: ./absent\n  sourceRef: {kind: GitRepository, name: absent}\n"),
			},
		},
		{
			name: "disjoint effective sync path", requested: plain, probes: 2,
			appsResources: "media/plain/deploy.yaml", instancePath: "kubernetes/flux-instance.yaml",
			extra: map[string]string{"kubernetes/kustomization.yaml": `resources: [apps, flux-instance.yaml]
patches:
- target: {kind: FluxInstance}
  patch: |-
    - op: replace
      path: /spec/sync/path
      value: kubernetes/other
`},
		},
		{
			name: "invalid private generation", requested: plain, probes: 1,
			appsResources: "flux-instance.yaml, media", mediaResources: "plain/deploy.yaml",
			instancePath: apps + "/flux-instance.yaml",
			extraPatches: `- target: {kind: FluxInstance}
  patch: |-
    - op: add
      path: /spec/kustomize
      value:
        patches:
        - target: {kind: Kustomization, name: flux-system}
          patch: |
            - op: add
              path: /missing/path
              value: invalid
`,
		},
		{
			name: "no instance", requested: plain, probes: 1, appsResources: "media/plain/deploy.yaml",
		},
		{
			name: "broken enclosing package", requested: plain, probes: 2,
			appsResources: "flux-instance.yaml, media", mediaResources: "plain/deploy.yaml, missing.yaml",
			instancePath: apps + "/flux-instance.yaml",
		},
		{
			name: "unreached bare directory", requested: plain,
			appsResources: "flux-instance.yaml, sibling", mediaResources: "removed.yaml",
			instancePath: apps + "/flux-instance.yaml",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "flux-instance-scoped-workloads", "workloads")
			instance, err := os.ReadFile(filepath.Join(fixture.Repo, media, "flux-instance.yaml"))
			assert.NoError(t, err)
			assert.NoError(t, os.Remove(filepath.Join(fixture.Repo, media, "flux-instance.yaml")))
			if tc.instancePath != "" {
				testutil.WriteFile(t, fixture.Repo, tc.instancePath, string(instance))
			}
			for path, data := range tc.extra {
				testutil.WriteFile(t, fixture.Repo, path, data)
			}
			testutil.WriteFile(t, fixture.Repo, media+"/unlisted.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unlisted}\n",
			)
			if tc.mediaResources == "" {
				assert.NoError(t, os.Remove(filepath.Join(fixture.Repo, media, "kustomization.yaml")))
			} else {
				testutil.WriteFile(t, fixture.Repo, media+"/kustomization.yaml", "resources: ["+tc.mediaResources+"]\n")
			}
			testutil.WriteFile(
				t,
				fixture.Repo,
				apps+"/kustomization.yaml",
				"resources: ["+tc.appsResources+"]\n"+`patches:
- target: {kind: Deployment, name: plain}
  patch: |-
    - op: add
      path: /metadata/annotations
      value: {review: accepted-root}
`+tc.extraPatches,
			)
			runs, err := renderTreesForTest(t.Context(), t, []Tree{{
				ID:       manifest.TreeID{Cluster: "."},
				RepoRoot: fixture.Repo,
				Path:     filepath.Join(fixture.Repo, tc.requested),
				Input:    testDirectoryInput(t, fixture.Repo),
				SelfURLs: []string{syncOrigin},
			}}, Config{Concurrency: 2, RecordWork: true})
			assert.NoError(t, err)
			assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing result: %v", runs)
			wantProducer := manifest.NewEntryKustomization(manifest.BootstrapSourceID, tc.requested, manifest.NamedResource{}).
				Named()
			var wantAnnotation any
			var warnings []manifest.Warning
			want := deploymentHeader + deploymentBody
			builds := map[manifest.NamedResource]uint64{{}: tc.probes + 1}
			if tc.wantRoot {
				wantProducer = testutil.KustomizationID("flux-system", "flux-system")
				builds[wantProducer] = 1
				wantAnnotation = "accepted-root"
				want = deploymentHeader + rootStamp + deploymentBody
			} else {
				warnings = []manifest.Warning{{
					Category: manifest.WarnEmptyScan, Count: 1,
					Message: "no Flux Kustomization or HelmRelease objects found under the input path",
				}}
			}
			result := runs[0].Result
			assert.Diff(t, witnessWork(result.Work, manifest.WorkKustomize), builds)
			assert.Equal(t, runs[0].entryBinding.Descendant != "", tc.wantRoot)
			if !tc.wantRoot {
				public := testutil.KustomizationID("flux-system", "flux-system")
				assert.Equal(t, workCounts(result.Work, public, manifest.WorkKustomize).Attempts, uint64(0))
			}
			assert.Equal(t, runs[0].entryBinding.Producer, wantProducer)
			doc := enclosingPackageDeployment(t, result, wantProducer)
			metadata := doc["metadata"].(map[string]any)
			annotations, _ := metadata["annotations"].(map[string]any)
			assert.Equal(t, annotations["review"], wantAnnotation)
			assert.Diff(t, result.Warnings, warnings)
			assert.Diff(t, serializeManifestBatches(t, result.Manifests, ""), "---\n"+want)
		})
	}
	t.Run("component reach", testEnclosingPackageComponentReach)
	t.Run("different nested recipes", testEnclosingPackageNestedRecipes)
	t.Run("ordinary instance shares the rejected root identity", testEnclosingPackageSharedIdentity)
}

func testEnclosingPackageSharedIdentity(t *testing.T) {
	t.Parallel()
	const origin = "https://github.com/example/fleet.git"
	root := t.TempDir()
	for path, data := range map[string]string{
		"kustomization.yaml": `resources: [apps]
patches:
- target: {kind: FluxInstance, name: flux}
  patch: |
    - op: replace
      path: /spec/sync/path
      value: disjoint
`,
		"apps/kustomization.yaml": `resources: [configmap.yaml, instance.yaml]`,
		"apps/configmap.yaml": `apiVersion: v1
kind: ConfigMap
metadata: {name: ordinary}
`,
		"apps/instance.yaml": `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec:
  distribution: {version: "2.x", registry: ghcr.io/fluxcd}
  sync: {kind: GitRepository, url: https://github.com/example/fleet.git, ref: refs/heads/main, path: apps}
`,
		"disjoint/kustomization.yaml": "resources: []\n",
	} {
		testutil.WriteFile(t, root, path, data)
	}
	runs, err := renderTreesForTest(t.Context(), t, []Tree{{
		ID: manifest.TreeID{Cluster: "."}, RepoRoot: root, Path: filepath.Join(root, "apps"),
		Input: testDirectoryInput(t, root), SelfURLs: []string{origin},
	}}, Config{Concurrency: 2, RecordWork: true})
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing result: %v", runs)
	run := runs[0]
	producer := testutil.KustomizationID("flux-system", "flux-system")
	assert.Equal(t, run.entryBinding.Producer, producer)
	ks, ok := run.Store().GetDeclaration(producer).(*manifest.Kustomization)
	assert.FatalIf(t, !ok, "missing selected recipe: %v", ks)
	assert.Equal(t, ks.Path, "apps")
	assert.Diff(t, run.Result.Warnings, []manifest.Warning(nil))
	assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkKustomize), map[manifest.NamedResource]uint64{producer: 1})
	assert.Diff(t, serializeManifestBatches(t, run.Result.Manifests, ""), `---
apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: flux-system
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: ordinary
---
apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: flux-system
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: flux
  namespace: flux-system
spec:
  distribution:
    registry: ghcr.io/fluxcd
    version: 2.x
  sync:
    kind: GitRepository
    path: apps
    ref: refs/heads/main
    url: https://github.com/example/fleet.git
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 1m0s
  ref:
    name: refs/heads/main
  url: https://github.com/example/fleet.git
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 10m0s
  path: apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
`)
}

func testEnclosingPackageNestedRecipes(t *testing.T) {
	t.Parallel()
	const entry = "apps/sync/media"
	const origin = "https://github.com/example/fleet.git"
	const recipe = `        patches:
        - target: {kind: Kustomization, name: flux-system}
          patch: |
            - op: add
              path: /spec/commonMetadata
              value:
                annotations: {recipe: `
	const configMap = `apiVersion: v1
kind: ConfigMap
metadata: {name: media, namespace: default}
`
	root := t.TempDir()
	for path, source := range map[string]string{
		"apps/instance.yaml": `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec:
  distribution: {version: "2.x", registry: ghcr.io/fluxcd}
  sync: {kind: GitRepository, url: https://github.com/example/fleet.git, ref: refs/heads/main, path: apps/sync}
  kustomize:
` + "    " + strings.TrimPrefix(strings.ReplaceAll(recipe, "\n    ", "\n"), "        ") + "nested}\n",
		"apps/kustomization.yaml":      "resources: [instance.yaml, sync]\n",
		"apps/sync/kustomization.yaml": "resources: [media]\n",
		entry + "/kustomization.yaml":  "resources: [configmap.yaml]\n",
		entry + "/configmap.yaml":      configMap,
		"kustomization.yaml": `resources: [apps]
patches:
- target: {kind: FluxInstance, name: flux}
  patch: |
    - op: replace
      path: /spec/kustomize
      value:
` + recipe + "outer}\n",
	} {
		testutil.WriteFile(t, root, path, source)
	}
	for _, request := range []string{entry, "."} {
		t.Run(request, func(t *testing.T) {
			t.Parallel()
			runs, err := renderTreesForTest(t.Context(), t, []Tree{{
				ID: manifest.TreeID{Cluster: "."}, RepoRoot: root,
				Path: filepath.Join(root, request), Input: testDirectoryInput(t, root),
				SelfURLs: []string{origin},
			}}, Config{Concurrency: 2})
			assert.NoError(t, err)
			assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing result: %v", runs)
			run := runs[0]
			producer := testutil.KustomizationID("flux-system", "flux-system")
			assert.Equal(t, run.entryBinding.Producer, producer)
			ks, ok := run.Store().GetDeclaration(producer).(*manifest.Kustomization)
			assert.FatalIf(t, !ok || ks.CommonMetadata == nil, "missing selected recipe: %v", ks)
			assert.Diff(t, ks.CommonMetadata.Annotations, map[string]string{"recipe": "outer"})
			assert.Diff(t, run.Result.Warnings, []manifest.Warning(nil))
			assert.Diff(t, serializeManifestBatches(t, run.Result.Manifests, ""), `---
apiVersion: v1
kind: ConfigMap
metadata:
  annotations:
    recipe: outer
  labels:
    kustomize.toolkit.fluxcd.io/name: flux-system
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: media
  namespace: default
`)
		})
	}
}

func testEnclosingPackageComponentReach(t *testing.T) {
	t.Parallel()
	const entry = "apps/media"
	const origin = "https://github.com/example/fleet.git"
	const instance = `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec:
  distribution: {version: "2.x", registry: ghcr.io/fluxcd}
  sync: {kind: GitRepository, url: https://github.com/example/fleet.git, ref: refs/heads/main, path: apps}
`
	const recipe = `  kustomize:
    patches:
    - target: {kind: Kustomization, name: flux-system, namespace: flux-system}
      patch: |-
        - op: add
          path: /spec/components
          value: ["./parts"]
`
	const component = "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n"
	const configMap = `apiVersion: v1
data:
  mode: test
kind: ConfigMap
metadata:
`
	const name = "  name: media\n  namespace: default\n"
	const labels = `  labels:
    kustomize.toolkit.fluxcd.io/name: flux-system
    kustomize.toolkit.fluxcd.io/namespace: flux-system
`
	for _, tc := range []struct {
		name, annotation string
		files            map[string]string
		wantRoot, full   bool
	}{
		{
			name: "authored component resource", wantRoot: true, full: true,
			files: map[string]string{
				"apps/instance.yaml":            instance,
				"apps/kustomization.yaml":       "resources: [instance.yaml]\ncomponents: [parts]\n",
				"apps/parts/kustomization.yaml": component + "resources: [../media]\n",
			},
		},
		{
			name: "effective recipe component resource", wantRoot: true,
			annotation: "  annotations:\n    config.kubernetes.io/origin: |\n      path: media/configmap.yaml\n",
			files: map[string]string{
				"instance.yaml":                 instance + recipe,
				"kustomization.yaml":            "resources: [instance.yaml, apps/media]\n",
				"apps/kustomization.yaml":       "resources: []\n",
				"apps/parts/kustomization.yaml": component + "resources: [../media]\n",
			},
		},
		{
			name: "component declaration alone",
			files: map[string]string{
				"apps/instance.yaml":                  instance,
				"apps/kustomization.yaml":             "resources: [instance.yaml]\ncomponents: [media/parts]\n",
				"apps/media/parts/kustomization.yaml": component,
			},
		},
		{
			name: "patch input alone",
			files: map[string]string{
				"apps/instance.yaml":                  instance,
				"apps/kustomization.yaml":             "resources: [instance.yaml]\ncomponents: [media/parts]\n",
				"apps/media/parts/kustomization.yaml": component + "patches: [{path: labels.yaml, target: {kind: ConfigMap}}]\n",
				"apps/media/parts/labels.yaml":        "- {op: add, path: /metadata/labels, value: {patched: test}}\n",
			},
		},
		{
			name: "ignored component resource",
			files: map[string]string{
				".sourceignore":                       "/apps/media/parts/ignored.yaml\n",
				"apps/instance.yaml":                  instance,
				"apps/kustomization.yaml":             "resources: [instance.yaml]\ncomponents: [media/parts]\n",
				"apps/media/parts/kustomization.yaml": component + "resources: [ignored.yaml]\n",
				"apps/media/parts/ignored.yaml":       configMap + name,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for file, data := range tc.files {
				testutil.WriteFile(t, root, file, data)
			}
			testutil.WriteFile(t, root, entry+"/kustomization.yaml", "resources: [configmap.yaml]\n")
			testutil.WriteFile(t, root, entry+"/configmap.yaml", configMap+name)
			render := func(request string) Rendered {
				runs, err := renderTreesForTest(t.Context(), t, []Tree{{
					ID: manifest.TreeID{Cluster: "."}, RepoRoot: root,
					Path: filepath.Join(root, request), Input: testDirectoryInput(t, root),
					SelfURLs: []string{origin},
				}}, Config{Concurrency: 2})
				assert.NoError(t, err)
				assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing result: %v", runs)
				return runs[0]
			}
			run := render(entry)
			producer := manifest.NewEntryKustomization(manifest.BootstrapSourceID, entry, manifest.NamedResource{}).
				Named()
			want := "---\n" + configMap + name
			warnings := []manifest.Warning{{
				Category: manifest.WarnEmptyScan, Count: 1,
				Message: "no Flux Kustomization or HelmRelease objects found under the input path",
			}}
			if tc.wantRoot {
				producer = testutil.KustomizationID("flux-system", "flux-system")
				want = "---\n" + configMap + tc.annotation + labels + name
				warnings = nil
			}
			result := run.Result
			assert.Equal(t, run.entryBinding.Producer, producer)
			assert.Equal(t, len(outputByParent(result.Manifests)[producer]), 1)
			assert.Diff(t, serializeManifestBatches(t, result.Manifests, ""), want)
			assert.Diff(t, result.Warnings, warnings)
			if tc.full {
				full := render("apps")
				assert.Equal(t, full.entryBinding.Producer, producer)
				assert.Diff(t, serializeManifestBatches(t, full.Result.Manifests, "ConfigMap"), want)
				assert.Diff(t, full.Result.Warnings, warnings)
			}
		})
	}
}

func enclosingPackageDeployment(t *testing.T, result *Result, producer manifest.NamedResource) map[string]any {
	t.Helper()
	var deployments []map[string]any
	for _, doc := range outputByParent(result.Manifests)[producer] {
		id := manifest.DocIdentity(doc)
		assert.FatalIf(t, id.Name == "unlisted", "document outside the requested inventory: %v", id)
		if id.Kind == "Deployment" && id.Name == "plain" {
			deployments = append(deployments, doc)
		}
	}
	assert.FatalIf(t, len(deployments) != 1, "expected one plain Deployment: %v", deployments)
	return deployments[0]
}

func TestRenderTrees_SeparateInstancePackage(t *testing.T) {
	t.Parallel()
	fixture := testfixture.Materialize(t, "flux-instance-ancestor", "separate")
	for _, tc := range []struct{ name, path, output, kind string }{
		{"ancestor", ".", "expect/patched.stdout", "Deployment"},
		{"equal", "kubernetes/apps", "expect/standalone.stdout", ""},
		{"descendant", "kubernetes/apps/media", "expect/standalone.stdout", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runs, err := renderTreesForTest(t.Context(), t, []Tree{{
				ID: manifest.TreeID{Cluster: "."}, RepoRoot: fixture.Repo,
				Path: filepath.Join(fixture.Repo, tc.path), Input: testDirectoryInput(t, fixture.Repo),
				SelfURLs: []string{"https://github.com/example/fleet.git"},
			}}, Config{Concurrency: 2})
			assert.NoError(t, err)
			assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing result: %v", runs)
			producer := manifest.NewEntryKustomization(manifest.BootstrapSourceID, tc.path, manifest.NamedResource{}).
				Named()
			warnings := []manifest.Warning{{
				Category: manifest.WarnEmptyScan, Count: 1,
				Message: "no Flux Kustomization or HelmRelease objects found under the input path",
			}}
			if tc.path == "." {
				producer, warnings = testutil.KustomizationID("flux-system", "flux-system"), nil
			}
			assert.Equal(t, runs[0].entryBinding.Producer, producer)
			assert.Diff(t, runs[0].Result.Warnings, warnings)
			assert.Diff(
				t,
				serializeManifestBatches(t, runs[0].Result.Manifests, tc.kind),
				fixture.Read(t, tc.output, ""),
			)
		})
	}
}

func serializeManifestBatches(t *testing.T, batches []manifest.DocumentBatch, kind string) string {
	t.Helper()
	var output strings.Builder
	for _, batch := range batches {
		for _, document := range batch.Documents {
			if kind != "" && manifest.DocKind(document) != kind {
				continue
			}
			data, err := yaml.Marshal(document)
			assert.NoError(t, err)
			output.WriteString("---\n")
			output.Write(data)
		}
	}
	return output.String()
}
