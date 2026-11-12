package orchestrator

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_UnavailableValuesPairedSides(t *testing.T) {
	t.Parallel()
	for _, unavailable := range []string{"encrypted", "produced"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", unavailable, reverse), func(t *testing.T) {
				t.Parallel()
				testRenderTreesUnavailableValuesPairedSides(t, unavailable, reverse)
			})
		}
	}
}

func testRenderTreesUnavailableValuesPairedSides(t *testing.T, unavailable string, reverse bool) {
	t.Helper()
	trees := make([]Tree, 2)
	states := [2]string{"readable", "readable"}
	for side := range trees {
		if (side == 0) != reverse {
			states[side] = unavailable
		}
		trees[side] = writeUnavailableValuesTree(t, states[side], side)
	}
	consumer := testutil.HelmReleaseID("apps", "consumer")
	var expected [2][]byte
	for side, tree := range trees {
		runs, err := renderTreesForTest(
			t.Context(),
			t,
			[]Tree{tree},
			Config{CacheDir: t.TempDir(), Concurrency: 2, AllowMissingKinds: []string{manifest.KindSecret}},
		)
		assert.NoError(t, err)
		expected[side] = unavailableValuesBytes(t, runs[0], states[side])
		trees[side].ExternalChanges = change.NewSet([]string{"app/input.yaml"})
	}
	for range 2 {
		runs, err := renderTreesForTest(
			t.Context(),
			t,
			trees,
			Config{CacheDir: t.TempDir(), Concurrency: 2, AllowMissingKinds: []string{manifest.KindSecret}},
		)
		assert.NoError(t, err)
		for side, run := range runs {
			assert.Equal(t, run.selection.Snapshot().Output(consumer), true)
			assert.Equal(t, run.selection.Snapshot().Changed(consumer), true)
			got := unavailableValuesBytes(t, run, states[side])
			assert.Equal(t, string(got), string(expected[side]))
		}
	}
}

func writeUnavailableValuesTree(t *testing.T, state string, side int) Tree {
	t.Helper()
	root := t.TempDir()

	testutil.WriteFile(
		t,
		root,
		"flux/app.yaml",
		"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: flux-system}\nspec:\n  interval: 1m\n  path: ./app\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [release.yaml, input.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/release.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}\n  valuesFrom:\n    - {kind: Secret, name: input, valuesKey: value, targetPath: value}\n    - {kind: Secret, name: input, valuesKey: value, targetPath: value}\n    - {kind: Secret, name: input, valuesKey: keep, targetPath: keep}\n",
	)
	value := "readable"
	unknown := state != "readable"
	if unknown && state == "encrypted" {
		value = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	}
	input := fmt.Sprintf(
		"apiVersion: v1\nkind: Secret\nmetadata: {name: input, namespace: apps}\nstringData: {value: '%s', keep: retained}\n",
		value,
	)
	if unknown && state == "produced" {
		input = "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: cluster, namespace: apps}\nspec: {target: {name: input}}\n"
	}
	testutil.WriteFile(t, root, "app/input.yaml", input)
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata:\n  value: {{ .Values.value | default \"fallback\" | quote }}\n  keep: {{ .Values.keep | default \"fallback\" | quote }}\n",
	)
	return Tree{
		RepoRoot: root,
		Path:     filepath.Join(root, "flux"),
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
	}
}

func unavailableValuesBytes(t *testing.T, run Rendered, state string) []byte {
	t.Helper()
	assert.NoError(t, run.Err)
	consumer := testutil.HelmReleaseID("apps", "consumer")
	docs := outputByParent(run.Result.Manifests)[consumer]
	assert.FatalIf(t, len(docs) != 1, "state=%s documents=%+v", state, docs)
	wantValue, wantKeep, wantOmissions := "readable", "retained", 0
	if state != "readable" {
		wantValue, wantOmissions = "fallback", 1
	}
	if state == "produced" {
		wantKeep = "fallback"
	}
	assert.Diff(t, docs[0]["data"], map[string]any{"value": wantValue, "keep": wantKeep})
	omissions := 0
	for i := range run.Result.Warnings {
		warning := &run.Result.Warnings[i]
		if warning.Resource == consumer && warning.Category == manifest.WarnOmittedValues {
			omissions++
		}
	}
	assert.Equal(t, omissions, wantOmissions)
	result, err := json.Marshal(docs)
	assert.NoError(t, err)
	return result
}
