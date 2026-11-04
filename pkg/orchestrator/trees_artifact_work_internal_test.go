package orchestrator

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_ArtifactInputCurrentSelection(t *testing.T) {
	t.Parallel()
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	artifact := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "generated",
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	trees := make([]Tree, 0, 2)
	var full [2]*Result
	for side, value := range []string{"old", "new"} {
		tree := artifactWorkTree(t, value, manifest.TreeSide(side))
		trees = append(trees, tree)
		runs, err := RenderTrees(t.Context(), []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		full[side] = runs[0].Result
		assert.Equal(
			t,
			workCounts(full[side].Work, consumer, manifest.WorkKustomize),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
		assert.Equal(
			t,
			workCounts(full[side].Work, generator, manifest.WorkEvaluation),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
		assert.Equal(
			t,
			workCounts(
				full[side].Work,
				generator,
				manifest.WorkAcquisition,
			),
			manifest.WorkCounts{
				Attempts:          1,
				Completed:         1,
				LocalAcquisitions: 1,
			},
		)
		assert.Equal(t, runs[0].Store().EmissionOwner(artifact), generator)
		payload, ok := runs[0].Store().GetArtifact(artifact, store.ArtifactContent).(*store.SourceArtifact)
		if !ok {
			t.Fatal("full render did not publish generated content")
		}
		assert.Equal(t, payload.Key != "", true)
		for recordIndex := range full[side].Work {
			record := full[side].Work[recordIndex]
			assert.Equal(t, record.Key.Tree, tree.ID)
			assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
			if record.Key.Owner == consumer && record.Key.Phase == manifest.WorkKustomize {
				assert.Equal(t, record.Key.Source, artifact)
			}
			if record.Key.Owner == generator && record.Key.Phase == manifest.WorkAcquisition {
				assert.Equal(t, record.Key.Input, payload.Key)
			}
		}
		docs := outputByParent(full[side].Manifests)[consumer]
		assert.Equal(t, len(docs), 1)
		assert.Diff(t, docs[0]["data"], map[string]any{"value": value})
	}
	assertArtifactSelection(t, trees, full)
}

func artifactWorkTree(t *testing.T, value string, side manifest.TreeSide) Tree {
	t.Helper()
	files := map[string]string{
		"flux/generator.yaml": "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\n" +
			"metadata: {name: generator, namespace: apps}\nspec:\n  sources: [{alias: " +
			"repo, kind: GitRepository, name: missing}]\n  artifacts:\n    - name: gen" +
			"erated\n      copy: [{from: '@repo/app/', to: '@artifact/app/'}]\n",

		"flux/consumer.yaml": testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./app/app\n  sourceRef: {kind: ExternalArtifact, name: generated}\n",
		),

		"flux/stable.yaml": testutil.KustomizationYAML("stable", "apps", "  path: ./stable\n"),

		"app/kustomization.yaml": "resources: [cm.yaml]\n",

		"app/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-output, names" +
			"pace: apps}\ndata: {value: " + value + "}\n",

		"stable/kustomization.yaml": "resources: [cm.yaml]\n",

		"stable/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: stable-output, namespace: apps}\n",
	}
	root := writeWitness(t, files)
	return Tree{ID: manifest.TreeID{Cluster: ".", Side: side}, RepoRoot: root, Path: filepath.Join(root, "flux")}
}

func assertArtifactSelection(t *testing.T, trees []Tree, full [2]*Result) {
	t.Helper()
	stable := testutil.KustomizationID("apps", "stable")
	consumer := testutil.KustomizationID("apps", "consumer")
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	artifact := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "generated",
	}
	var first []byte
	for repeat := range 2 {
		runs, err := RenderTrees(t.Context(), trees, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		outputs := make([][]manifest.DocumentBatch, 0, len(runs))
		for _, run := range runs {
			assert.Diff(t, visibleWitnessOutput(run.Result), witnessOutput(full[run.ID.Side], nil))
			assert.Equal(t, len(outputByParent(run.Result.Manifests)[stable]), 0)
			assert.Equal(t, workCounts(run.Result.Work, stable, manifest.WorkKustomize), manifest.WorkCounts{})
			assert.Equal(t, workCounts(run.Result.Work, consumer, manifest.WorkKustomize), manifest.WorkCounts{})
			assert.Equal(t, workCounts(run.Result.Work, generator, manifest.WorkEvaluation), manifest.WorkCounts{})
			assert.Equal(t, workCounts(run.Result.Work, generator, manifest.WorkAcquisition), manifest.WorkCounts{})
			assert.Equal(t, run.Store().GetArtifact(artifact, store.ArtifactContent) == nil, true)
			for recordIndex := range run.Result.Work {
				record := run.Result.Work[recordIndex]
				assert.Equal(t, record.Key.Tree, run.ID)
				assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
			}
			outputs = append(outputs, run.Result.Manifests)
		}
		body, err := json.Marshal(outputs)
		assert.NoError(t, err)
		if repeat == 0 {
			first = body
		} else {
			assert.Equal(t, string(body), string(first))
		}
	}
}
