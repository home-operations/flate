package orchestrator

import (
	"encoding/json"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testEntrySelfApplier(t *testing.T) {
	t.Helper()
	var previous [2][]byte
	for run := range 2 {
		for side := range 2 {
			trees := entrySelfApplierTrees(t)
			selected, err := renderTreesForTest(t.Context(), t, []Tree{trees[side]}, Config{
				CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true,
				Selection: selection.Request{Changed: true},
			})
			assert.NoError(t, err)
			assert.Equal(t, len(selected), 1)
			oracleTrees := entrySelfApplierTrees(t)
			oracle := oracleTrees[side]
			oracle.BaseInput, oracle.ExternalChanges = nil, nil
			full, err := renderTreesForTest(
				t.Context(),
				t,
				[]Tree{oracle},
				Config{CacheDir: t.TempDir(), Concurrency: 2},
			)
			assert.NoError(t, err)
			assert.Equal(t, len(full), 1)
			result := selected[0].Result
			assert.Diff(t, result.Failed, full[0].Result.Failed)
			assert.Diff(t, result.Blocked, full[0].Result.Blocked)
			actual, err := json.Marshal(result.Manifests)
			assert.NoError(t, err)
			want, err := json.Marshal(full[0].Result.Manifests)
			assert.NoError(t, err)
			assert.Equal(t, string(actual), string(want))
			if run == 0 {
				previous[side] = actual
			} else {
				assert.Equal(t, string(actual), string(previous[side]))
			}
			assertEntrySelfApplierOutput(t, result, side)
			assertEntrySelfApplierWork(t, result, trees[side].ID)
		}
	}
}

func assertEntrySelfApplierOutput(t *testing.T, result *Result, side int) {
	t.Helper()
	sync := testutil.KustomizationID("apps", "sync")
	typed := testutil.HelmReleaseID("apps", "typed")
	output := outputByParent(result.Manifests)
	assert.Equal(t, len(output[sync]), 6)
	assert.Equal(t, len(output[typed]), 1)
	revision := "before"
	if side == 1 {
		revision = "after"
	}
	assert.Diff(t, output[typed][0]["data"], map[string]any{"count": "7", "marker": "accepted", "revision": revision})
	for _, name := range []string{"volsync-a", "volsync-b"} {
		docs := output[testutil.KustomizationID("apps", name)]
		assert.Equal(t, len(docs), 1)
		assert.Equal(t, manifest.DocMetadata(docs[0]).Name, name)
		assert.Diff(t, docs[0]["data"], map[string]any{"workload": name, "revision": revision})
	}
}

func assertEntrySelfApplierWork(t *testing.T, result *Result, tree manifest.TreeID) {
	t.Helper()
	sync := testutil.KustomizationID("apps", "sync")
	a, b := testutil.KustomizationID("apps", "volsync-a"), testutil.KustomizationID("apps", "volsync-b")
	typed := testutil.HelmReleaseID("apps", "typed")
	source := (&manifest.GitRepository{Name: "self", Namespace: "apps"}).Named()
	assert.Diff(
		t,
		witnessWork(result.Work, manifest.WorkKustomize),
		map[manifest.NamedResource]uint64{sync: 1, a: 1, b: 1},
	)
	assert.Diff(t, witnessWork(result.Work, manifest.WorkHelm), map[manifest.NamedResource]uint64{typed: 1})
	assert.Diff(
		t,
		witnessWork(result.Work, manifest.WorkSubstitution),
		map[manifest.NamedResource]uint64{sync: 1, a: 1, b: 1},
	)
	for _, owner := range []manifest.NamedResource{sync, a, b, typed} {
		assert.Equal(t, workCounts(result.Work, owner, manifest.WorkAdmission).Attempts, uint64(1))
	}
	keys := map[manifest.NamedResource]string{}
	for recordIndex := range result.Work {
		record := &result.Work[recordIndex]
		assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
		if record.Key.Phase != manifest.WorkKustomize && record.Key.Phase != manifest.WorkHelm &&
			record.Key.Phase != manifest.WorkSubstitution {
			continue
		}
		assert.Equal(t, record.Key.Tree, tree)
		expectedSource := source
		if record.Key.Phase == manifest.WorkHelm {
			expectedSource = manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "apps-typed",
			}
		}
		assert.Equal(t, record.Key.Source, expectedSource)
		assert.Equal(t, record.Counts.Attempts, uint64(1))
		assert.Equal(t, record.Counts.Completed, uint64(1))
		assert.Equal(t, record.Counts.Failures, uint64(0))
		if record.Key.Phase == manifest.WorkSubstitution {
			continue
		}
		assert.ErrorIf(t, record.Key.Input == "", "accepted engine work has no content identity")
		assert.ErrorIf(t, keys[record.Key.Owner] != "", "accepted owner executed a second content identity")
		keys[record.Key.Owner] = record.Key.Input
	}
	assert.ErrorIf(t, keys[a] == keys[b], "distinct substitution applications share an input key")
}

func entrySelfApplierTrees(t *testing.T) [2]Tree {
	t.Helper()
	const url = "https://example.test/self-applier/repo"
	files := map[string]string{
		"entry/kustomization.yaml": "resources: [application.yaml, source.yaml, vars.yaml, typed.yaml, volsync-a.yaml, volsync-b.yaml]\n",
		"entry/source.yaml": "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
			"metadata: {name: self, namespace: apps}\nspec:\n  url: " + url + "\n  ignore: |\n    /*\n    !/entry\n    !/charts\n    !/shared\n",
		"entry/application.yaml": `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: apps}
spec:
  interval: 1h
  path: ./entry
  sourceRef: {kind: GitRepository, name: self}
  decryption: {provider: sops}
  postBuild:
    substitute: {COUNT: "7"}
    substituteFrom: [{kind: Secret, name: vars, optional: false}]
  patches:
    - target: {kind: HelmRelease, name: typed}
      patch: |-
        - op: replace
          path: /spec/values/marker
          value: accepted
`,
		"entry/vars.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: vars, namespace: apps}\n" +
			"stringData: {TOKEN: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
		"entry/typed.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: typed, namespace: apps}
spec:
  interval: 1h
  chart:
    spec:
      chart: charts/typed
      sourceRef: {kind: GitRepository, name: self}
  values:
    count: ${COUNT}
    marker: raw
`,
		"charts/typed/Chart.yaml":         "apiVersion: v2\nname: typed\nversion: 1.0.0\n",
		"charts/typed/values.schema.json": `{"type":"object","required":["count"],"properties":{"count":{"type":"integer"}}}`,
		"charts/typed/templates/cm.yaml": `apiVersion: v1
kind: ConfigMap
metadata: {name: typed-output, namespace: apps}
data:
  count: {{ .Values.count | quote }}
  marker: {{ .Values.marker | quote }}
  revision: before
`,
		"shared/kustomization.yaml": "resources: [cm.yaml]\n",
		"shared/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '${WORKLOAD_NAME}', namespace: apps}\n" +
			"data: {workload: '${WORKLOAD_NAME}', revision: before}\n",
	}
	for _, name := range []string{"a", "b"} {
		files["entry/volsync-"+name+".yaml"] = testutil.KustomizationYAML("volsync-"+name, "apps",
			"  interval: 1h\n  path: ./shared\n  sourceRef: {kind: GitRepository, name: self}\n"+
				"  postBuild:\n    substitute: {WORKLOAD_NAME: volsync-"+name+"}\n")
	}
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		for _, name := range slices.Sorted(maps.Keys(files)) {
			body := files[name]
			if side == 1 && (name == "charts/typed/templates/cm.yaml" || name == "shared/cm.yaml") {
				body = strings.ReplaceAll(body, "revision: before", "revision: after")
			}
			if side == 1 && name == "entry/volsync-a.yaml" {
				body = strings.ReplaceAll(body, "interval: 1h", "interval: 2h")
			}
			testutil.WriteFile(t, root, name, body)
		}
	}
	inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
	inputs[0].LocalPath, inputs[0].LocalRoot, inputs[0].URL = roots[1], roots[1], "file://"+roots[1]
	var trees [2]Tree
	for side := range trees {
		trees[side] = Tree{
			ID: manifest.TreeID{
				Cluster: ".",
				Side:    []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead}[side],
			},
			Path:      filepath.Join(roots[1], "entry"),
			RepoRoot:  roots[1],
			SelfURLs:  []string{url},
			Input:     inputs[side],
			BaseInput: inputs[1-side],
			ExternalChanges: change.NewSet(
				[]string{"charts/typed/templates/cm.yaml", "shared/cm.yaml", "entry/volsync-a.yaml"},
			),
		}
	}
	return trees
}
