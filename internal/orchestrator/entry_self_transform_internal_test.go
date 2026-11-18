package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_EntrySelfTransformation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const url = "https://example.test/self-transform"
	testutil.WriteFile(
		t,
		root,
		"entry/kustomization.yaml",
		"resources: [source.yaml, application.yaml, value.yaml, vars.yaml]\n",
	)
	testutil.WriteFile(t, root, "entry/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: self, namespace: apps}
spec:
  url: `+url+`
  ignore: |
    /*
    !/entry
`)
	testutil.WriteFile(t, root, "entry/application.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: apps}
spec:
  interval: 1h
  path: ./entry
  sourceRef: {kind: GitRepository, name: self}
  postBuild:
    substitute: {NAME: accepted}
    substituteFrom: [{kind: Secret, name: vars, optional: false}]
  patches:
    - target: {kind: Kustomization}
      patch: |-
        - op: replace
          path: /spec/interval
          value: 2h
`)
	testutil.WriteFile(t, root, "entry/value.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '${NAME:=raw}', namespace: apps}
data: {value: accepted}
`)
	testutil.WriteFile(t, root, "entry/vars.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: vars, namespace: apps}
spec:
  target: {name: vars}
  dataFrom: [{extract: {key: vars}}]
`)
	o, err := newTestOrchestrator(t, Tree{
		Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url},
	}, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	assert.NoError(t, o.bootstrap(t.Context()))
	result, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(result.Close)
	assert.NoError(t, err)
	owner := testutil.KustomizationID("apps", "sync")
	assert.Equal(t, o.entryBinding.Producer, owner)
	output := outputByParent(result.Manifests)
	assert.Equal(t, len(output), 1)
	var names []string
	for _, doc := range output[owner] {
		if manifest.DocKind(doc) == manifest.KindConfigMap {
			names = append(names, manifest.DocMetadata(doc).Name)
		}
	}
	assert.Diff(t, names, []string{"accepted"})
}
