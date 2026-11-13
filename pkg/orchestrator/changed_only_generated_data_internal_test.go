package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_ChangedOnlyUnavailableGeneratedData(t *testing.T) {
	t.Parallel()
	files := map[string]string{
		"entry/kustomization.yaml": "resources: [parent.yaml]\n",
		"entry/parent.yaml": testutil.KustomizationYAML("parent", "apps", `  interval: 1h
  path: ./apps
  postBuild: {substitute: {CLUSTER: shared}}
  patches:
    - target: {kind: Kustomization}
      patch: |-
        apiVersion: kustomize.toolkit.fluxcd.io/v1
        kind: Kustomization
        metadata: {name: unused}
        spec:
          postBuild: {substituteFrom: [{kind: Secret, name: shared-vars}]}
`),
		"apps/kustomization.yaml": "resources: [changed.yaml, unrelated.yaml, vars.yaml]\n",
		"apps/changed.yaml": testutil.KustomizationYAML("changed", "apps",
			"  interval: 1h\n  path: ./changed\n"),
		"apps/unrelated.yaml": testutil.KustomizationYAML("unrelated", "apps",
			"  interval: 1h\n  path: ./unrelated\n"),
		"apps/vars.yaml": `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: '${CLUSTER}-vars', namespace: apps}
spec: {target: {name: '${CLUSTER}-vars'}, data: [{secretKey: VALUE, remoteRef: {key: value}}]}
`,
		"changed/kustomization.yaml":   "resources: [output.yaml]\n",
		"changed/output.yaml":          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed}\ndata: {value: '${VALUE}'}\n",
		"unrelated/kustomization.yaml": "resources: [output.yaml]\n",
		"unrelated/output.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\ndata: {value: '${VALUE}'}\n",
	}
	roots := [2]string{writeWitness(t, files), ""}
	files["apps/changed.yaml"] += "  timeout: 2m\n"
	roots[1] = writeWitness(t, files)
	inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
	trees := make([]Tree, 2)
	for side := range trees {
		trees[side] = Tree{
			ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			Path: filepath.Join(roots[side], "entry"), RepoRoot: roots[side],
			Input: inputs[side], BaseInput: inputs[1-side],
			ExternalChanges: change.NewSet([]string{"apps/changed.yaml"}),
		}
	}
	runs, err := renderTreesForTest(t.Context(), t, trees, Config{
		CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true,
		Selection: selection.Request{Changed: true},
	})
	assert.NoError(t, err)
	assert.Equal(t, len(runs), 2)
	for _, run := range runs {
		assert.Equal(t, workCounts(run.Result.Work, testutil.KustomizationID("apps", "unrelated"),
			manifest.WorkKustomize).Attempts, uint64(0))
		assert.Equal(t, workCounts(run.Result.Work, testutil.KustomizationID("apps", "changed"),
			manifest.WorkKustomize).Attempts, uint64(1))
	}
}
