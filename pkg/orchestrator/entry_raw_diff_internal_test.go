package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_RawEntryDescendantDiff(t *testing.T) {
	t.Parallel()
	const url = "https://example.test/raw-entry"
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		testutil.WriteFile(t, root, "entry/config/kustomization.yaml", "resources: [cluster.yaml]\n")
		testutil.WriteFile(t, root, "entry/config/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: self, namespace: apps}
spec:
  url: `+url+`
  ignore: |
    /*
    !/entry
    !/applications
    !/workload
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster, namespace: apps}
spec:
  path: ./entry
  sourceRef: {kind: GitRepository, name: self}
  postBuild:
    substituteFrom: [{kind: Secret, name: vars}]
`)
		testutil.WriteFile(
			t,
			root,
			"entry/apps.yaml",
			testutil.KustomizationYAML("applications", "apps", `  path: ./applications
  sourceRef: {kind: GitRepository, name: self}
  postBuild:
    substituteFrom: [{kind: Secret, name: vars}]
`),
		)
		testutil.WriteFile(
			t,
			root,
			"entry/vars/secret.yaml",
			"apiVersion: v1\nkind: Secret\nmetadata: {name: vars, namespace: apps}\n"+
				"stringData: {TOKEN: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
		)
		testutil.WriteFile(t, root, "applications/kustomization.yaml", "resources: [child.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"applications/child.yaml",
			testutil.KustomizationYAML("child", "apps", `  path: ./workload
  sourceRef: {kind: GitRepository, name: self}
  postBuild:
    substituteFrom: [{kind: Secret, name: vars}]
`),
		)
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: [value.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"workload/value.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: value, namespace: apps}\ndata: {value: "+[]string{"before", "after"}[side]+"}\n",
		)
	}
	inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
	inputs[0].LocalPath, inputs[0].LocalRoot = roots[1], roots[1]
	trees := make([]Tree, 2)
	for side := range trees {
		trees[side] = Tree{
			ID: manifest.TreeID{
				Cluster: ".",
				Side:    []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead}[side],
			},
			Path:            filepath.Join(roots[1], "entry"),
			RepoRoot:        roots[1],
			SelfURLs:        []string{url},
			Input:           inputs[side],
			BaseInput:       inputs[1-side],
			ExternalChanges: change.NewSet([]string{"workload/value.yaml"}),
		}
	}
	runs, err := renderTreesForTest(t.Context(), t, trees, Config{
		CacheDir: t.TempDir(), Concurrency: 2, Selection: selection.Request{Changed: true},
	})
	assert.NoError(t, err)
	assert.Equal(t, len(runs), 2)
	for side, run := range runs {
		output := outputByParent(run.Result.Manifests)[testutil.KustomizationID("apps", "child")]
		assert.FatalIf(t, len(output) != 1, "changed descendant output: %+v", run.Result.Manifests)
		assert.Diff(t, output[0]["data"], map[string]any{"value": []string{"before", "after"}[side]})
		warnings := map[string]bool{}
		for _, warning := range run.Result.Warnings {
			warnings[warning.Category] = true
		}
		assert.Equal(t, warnings[manifest.WarnSOPSUnreadable], true)
		assert.Equal(t, warnings[manifest.WarnUnresolvedSubstitution], true)
	}
}
