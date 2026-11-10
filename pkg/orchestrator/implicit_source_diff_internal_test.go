package orchestrator

import (
	"bytes"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_ChangedOnlyAndDiff_ImplicitSource(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, len(roots))
	for side, root := range roots {
		name, value := []string{"removed", "added"}[side], []string{"before", "after"}[side]
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux/producer.yaml, flux/sibling.yaml]\n")
		for _, owner := range []string{"producer", "sibling"} {
			testutil.WriteFile(t, root, "flux/"+owner+".yaml", testutil.KustomizationYAML(
				owner, "flux-system", "  path: ./"+owner+"\n",
			))
		}
		testutil.WriteFile(t, root, "producer/kustomization.yaml", fmt.Sprintf(
			"resources: [consumer.yaml, %s.yaml]\nnamespace: flux-system\ngeneratorOptions: {disableNameSuffixHash: true}\nconfigMapGenerator:\n- name: settings\n  literals: [VALUE=%s]\n",
			name,
			value,
		))
		testutil.WriteFile(t, root, "producer/consumer.yaml", testutil.KustomizationYAML(
			"consumer", "flux-system",
			"  path: ./consumer\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
		))
		testutil.WriteFile(t, root, "producer/"+name+".yaml", fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s}\n", name,
		))
		testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [result.yaml]\n")
		testutil.WriteFile(t, root, "consumer/result.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${VALUE}'}\n",
		)
		testutil.WriteFile(t, root, "sibling/kustomization.yaml", "resources: [result.yaml]\n")
		testutil.WriteFile(t, root, "sibling/result.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: untouched}\n",
		)
		trees[side] = Tree{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}}
	}

	full := make([]Rendered, len(trees))
	for side := range trees {
		runs, err := renderTreesForTest(
			t.Context(),
			t,
			trees[side:side+1],
			Config{CacheDir: t.TempDir(), Concurrency: 2},
		)
		assert.NoError(t, err)
		full[side] = runs[0]
	}
	_, forward := implicitSourceDiff(t, full)
	statuses := map[string]diff.ChangeStatus{}
	for _, change := range forward {
		statuses[change.Name] = change.Status
	}
	assert.Diff(t, statuses, map[string]diff.ChangeStatus{
		"removed": diff.StatusRemoved, "added": diff.StatusAdded,
		"settings": diff.StatusChanged, "result": diff.StatusChanged,
	})
	for _, reverse := range []bool{false, true} {
		ordered, oracles := trees, full
		if reverse {
			ordered = []Tree{trees[1], trees[0]}
			ordered[0].ID.Side, ordered[1].ID.Side = manifest.TreeBase, manifest.TreeHead
			oracles = []Rendered{full[1], full[0]}
		}
		var first []byte
		for run := range 2 {
			paired, err := renderTreesForTest(t.Context(), t, ordered, Config{CacheDir: t.TempDir(), Concurrency: 2})
			assert.NoError(t, err)
			for side, rendered := range paired {
				assertImplicitSourceSelection(t, &rendered, &oracles[side])
			}
			body, changes := implicitSourceDiff(t, paired)
			want, _ := implicitSourceDiff(t, oracles)
			assert.Equal(t, string(body), string(want))
			if run == 0 {
				first = body
			} else {
				assert.Equal(t, string(body), string(first))
			}
			assertImplicitSourceDiff(t, changes, forward, reverse)
		}
	}
}

func assertImplicitSourceSelection(t *testing.T, rendered, full *Rendered) {
	t.Helper()
	producer := testutil.KustomizationID("flux-system", "producer")
	consumer := testutil.KustomizationID("flux-system", "consumer")
	sibling := testutil.KustomizationID("flux-system", "sibling")
	assert.FatalIf(t, rendered.selection == nil, "missing paired selection")
	assert.Diff(t, rendered.cfg.ExternalChanges.Paths(), []string{
		"producer/added.yaml", "producer/kustomization.yaml", "producer/removed.yaml",
	})
	assert.Equal(t, len(rendered.Result.Blocked), 0)
	assert.Equal(t, len(rendered.Result.Failed), 0)
	assert.Equal(t, rendered.selection.Snapshot().Reconcile(sibling), false)
	byParent := outputByParent(rendered.Result.Manifests)
	assert.Equal(t, len(byParent[sibling]), 0)
	assert.Equal(t, len(outputByParent(full.Result.Manifests)[sibling]), 1)
	for _, owner := range []manifest.NamedResource{producer, consumer} {
		assert.Diff(t, byParent[owner], outputByParent(full.Result.Manifests)[owner])
		obj, ok := rendered.Store().Get[*manifest.Kustomization](owner)
		assert.FatalIf(t, !ok, "missing omitted-source owner %s", owner)
		assert.Equal(t, obj.SourceRef.Name, "")
	}
	assert.Diff(t, testutil.Owners(rendered.Store().Productions(consumer, nil)), []manifest.NamedResource{producer})
}

func assertImplicitSourceDiff(t *testing.T, got, forward []diff.Change, reverse bool) {
	t.Helper()
	want := slices.Clone(forward)
	if reverse {
		for i := range want {
			change := &want[i]
			change.Old, change.New = change.New, change.Old
			switch change.Status {
			case diff.StatusAdded:
				change.Status = diff.StatusRemoved
			case diff.StatusRemoved:
				change.Status = diff.StatusAdded
			case diff.StatusChanged:
			}
		}
	}
	assert.Diff(t, got, want)
}

func implicitSourceDiff(t *testing.T, runs []Rendered) ([]byte, []diff.Change) {
	t.Helper()
	selected, errors := Select(runs, nil, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
	assert.NoError(t, errors.Selected)
	assert.NoError(t, errors.Remaining)
	snapshots := make([]diff.Snapshot, len(selected))
	for side, result := range selected {
		snapshots[side] = diff.Snapshot{
			Subjects: result.Subjects, Manifests: result.Manifests,
			ProducerKeys: result.ProducerKeys, Unavailable: result.Unavailable,
		}
	}
	comparison1, preparationErr1 := diff.Compare(
		[]diff.Group{{Orig: snapshots[0], Current: snapshots[1], SelectedErr: nil}},
		diff.Options{Format: diff.FormatJSON},
	)
	var comparisonOutput1 bytes.Buffer
	writeErr1 := comparison1.Write(&comparisonOutput1, nil)
	body, _, _, err := comparisonOutput1.Bytes(), comparison1.Groups[0].Warnings, comparison1.Groups[0].Suppressed, stderrors.Join(
		preparationErr1,
		writeErr1,
	)
	assert.NoError(t, err)
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(body, &output))
	return body, output.Changes
}
