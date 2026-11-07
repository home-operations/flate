package orchestrator

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_OCIMachineCredentialBothSides(t *testing.T) {
	t.Parallel()
	registry := testutil.NewChartRegistry(t, []map[string]string{
		{
			"demo/Chart.yaml":        "apiVersion: v2\nname: demo\nversion: 0.1.0\n",
			"demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '{{ .Values.value }}'}\n",
		},
	}, true)
	parsed, err := url.Parse(registry.URL)
	assert.NoError(t, err)
	registryConfig := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(
		t,
		registryConfig,
		fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture-password"}}}`, parsed.Host),
	)
	trees := make([]Tree, 0, 2)
	for side := range 2 {
		root := t.TempDir()
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [oci.yaml, hr.yaml]\n")
		testutil.WriteFile(t, root, "oci.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: chart, namespace: apps}
spec:
  interval: 1m
  url: %s
  ref: {tag: 0.1.0}
  insecure: true
  layerSelector: {mediaType: application/vnd.cncf.helm.chart.content.v1.tar+gzip, operation: copy}
`, registry.URL))
		testutil.WriteFile(t, root, "hr.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: consumer, namespace: apps}
spec:
  interval: 1m
  chartRef: {kind: OCIRepository, name: chart}
  values: {value: side-%d}
`, side))
		trees = append(
			trees,
			Tree{
				RepoRoot:        root,
				Path:            root,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
				ExternalChanges: change.NewSet([]string{"hr.yaml"}),
			},
		)
	}
	results, err := renderTreesForTest(t.Context(), t,
		trees,
		Config{
			CacheDir:       t.TempDir(),
			RegistryConfig: registryConfig,
			Concurrency:    2,
			Selection:      selection.Request{Changed: true},
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, len(results), 2)
	source := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "chart",
	}
	for _, result := range results {
		assert.NoError(t, result.Err)
		info, exists := result.Store().GetStatus(source)
		assert.Equal(t, exists, true)
		assert.Equal(t, info.Status, store.StatusReady)
		assert.Equal(t, result.Store().GetArtifact(source, store.ArtifactContent) != nil, true)
		assert.Equal(t, len(result.Result.Warnings), 0)
	}
	assert.Equal(t, registry.Authenticated.Load() > 0, true)
}

func TestRenderTrees_OCIMachineCredentialSnapshotIsolation(t *testing.T) {
	t.Parallel()
	registry := testutil.NewChartRegistry(t, []map[string]string{
		{
			"demo/Chart.yaml":        "apiVersion: v2\nname: demo\nversion: 0.1.0\n",
			"demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '{{ .Values.value }}'}\n",
		},
	}, true)
	var unrelatedRequests atomic.Int64
	unrelated := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		unrelatedRequests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unrelated.Close()
	parsed, err := url.Parse(registry.URL)
	assert.NoError(t, err)
	registryConfig := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(
		t,
		registryConfig,
		fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture-password"}}}`, parsed.Host),
	)
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster-apps, namespace: flux-system}
spec:
  interval: 1m
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
  patches:
  - target: {kind: Kustomization}
    patch: |
      apiVersion: kustomize.toolkit.fluxcd.io/v1
      kind: Kustomization
      metadata: {name: unused}
      spec:
        patches:
        - target: {kind: HelmRelease}
          patch: |
            apiVersion: helm.toolkit.fluxcd.io/v2
            kind: HelmRelease
            metadata: {name: unused}
            spec:
              driftDetection: {mode: enabled}
`)

	for _, name := range []string{"selected", "unrelated"} {
		testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", `resources: [ks.yaml]
transformers:
- |-
  apiVersion: builtin
  kind: NamespaceTransformer
  metadata: {name: apply-ns, namespace: apps}
  unsetOnly: false
  fieldSpecs:
  - {path: spec/targetNamespace, group: kustomize.toolkit.fluxcd.io, kind: Kustomization, create: true}
`)
		testutil.WriteFile(t, root, "apps/"+name+"/ks.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s}
spec:
  interval: 1m
  path: ./apps/%s/app
  targetNamespace: apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`, name, name))
		testutil.WriteFile(t, root, "apps/"+name+"/app/kustomization.yaml", "resources: [oci.yaml, hr.yaml]\n")
		target := registry.URL
		if name == "unrelated" {
			target = "oci://" + strings.TrimPrefix(unrelated.URL, "https://") + "/unrelated"
		}
		testutil.WriteFile(t, root, "apps/"+name+"/app/oci.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: %s}
spec:
  interval: 1m
  url: %s
  ref: {tag: 0.1.0}
  insecure: true
  layerSelector: {mediaType: application/vnd.cncf.helm.chart.content.v1.tar+gzip, operation: copy}
`, name, target))
		testutil.WriteFile(t, root, "apps/"+name+"/app/hr.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s}
spec:
  interval: 1m
  chartRef: {kind: OCIRepository, name: %s}
  values: {value: old}
`, name, name))
	}
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "Fixture", Email: "fixture@example.test"}},
	)
	testutil.WriteFile(t, root, "apps/selected/app/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected}
spec:
  interval: 1m
  chartRef: {kind: OCIRepository, name: selected}
  values: {value: new}
`)
	cache, err := sourcecache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer cache.Close()
	inputs, err := baseline.AutoResolve(t.Context(), filepath.Join(root, "entry"), "HEAD", cache)
	assert.NoError(t, err)
	trees := []Tree{
		{
			ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
			Path:            inputs.BaseScanPath,
			SelfURLs:        inputs.SelfURLs,
			Input:           inputs.BaseInput,
			BaseInput:       inputs.Source,
			ExternalChanges: inputs.Changes,
		},
		{
			ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			Path:            inputs.ScanPath,
			SelfURLs:        inputs.SelfURLs,
			Input:           inputs.Source,
			BaseInput:       inputs.BaseInput,
			ExternalChanges: inputs.Changes,
		},
	}
	results, err := renderTreesForTest(t.Context(), t,
		trees,
		Config{
			Cache:          cache,
			RegistryConfig: registryConfig,
			Concurrency:    2,
			Selection:      selection.Request{Changed: true},
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, len(results), 2)
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "selected",
	}
	for _, result := range results {
		assert.NoError(t, result.Err)
		info, exists := result.Store().GetStatus(source)
		assert.Equal(t, exists, true)
		assert.Equal(t, info.Status, store.StatusReady)
		assert.Equal(t, result.Store().GetArtifact(source, store.ArtifactContent) != nil, true)
		assert.Equal(t, len(result.Result.Warnings), 0)
	}
	assert.Equal(t, registry.Authenticated.Load() > 0, true)
	assert.Equal(t, unrelatedRequests.Load(), int64(0))
}
