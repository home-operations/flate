package orchestrator

import (
	"bytes"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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

func TestRenderTrees_ChangedOnlyAndDiff_BootstrapAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		transition string
		kind       string
	}{
		{transition: "application removed", kind: manifest.KindOCIRepository},
		{transition: "source declared", kind: manifest.KindGitRepository},
		{transition: "source replaced", kind: manifest.KindGitRepository},
	} {
		t.Run(tc.transition, func(t *testing.T) {
			t.Parallel()
			roots := [2]string{t.TempDir(), t.TempDir()}
			for side, root := range roots {
				testutil.WriteFile(
					t,
					root,
					"flux/entry.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: entry}\n",
				)
				testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [hello.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"payload/hello.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {value: local}\n",
				)
				if side == 0 || tc.transition != "application removed" {
					testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps",
						"  path: ./payload\n  sourceRef: {kind: "+tc.kind+", name: shared}\n"))
				}
			}
			if tc.transition == "source replaced" {
				testutil.WriteFile(
					t,
					roots[0],
					"flux/source.yaml",
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: shared, namespace: apps}\nspec: {url: https://fixture.invalid/self, interval: 1m}\n",
				)
			}
			if tc.transition != "application removed" {
				remote := t.TempDir()
				testutil.WriteFile(t, remote, "payload/kustomization.yaml", "resources: [hello.yaml]\n")
				testutil.WriteFile(
					t,
					remote,
					"payload/hello.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {value: remote}\n",
				)
				repository := testutil.GitInit(t, remote)
				testutil.GitCommitAll(
					t,
					repository,
					"declared source",
					&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
				)
				testutil.WriteFile(
					t,
					roots[1],
					"flux/source.yaml",
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: shared, namespace: apps}\nspec: {url: 'file://"+remote+"', interval: 1m}\n",
				)
			}
			for _, reverse := range []bool{false, true} {
				testBootstrapAuthorityDiff(t, roots, tc.transition, reverse)
			}
		})
	}
}

func testBootstrapAuthorityDiff(t *testing.T, roots [2]string, transition string, reverse bool) {
	t.Helper()
	trees := make([]Tree, 2)
	for side := range trees {
		index := side
		if reverse {
			index = 1 - side
		}
		trees[side] = Tree{
			Path:     filepath.Join(roots[index], "flux"),
			RepoRoot: roots[index],
			SelfURLs: []string{"https://fixture.invalid/self"},
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	full := make([]Rendered, 2)
	for side := range trees {
		runs, err := renderTreesForTest(
			t.Context(),
			t,
			trees[side:side+1],
			Config{Concurrency: 2, CacheDir: t.TempDir()},
		)
		assert.NoError(t, err)
		full[side] = runs[0]
	}
	var previous []byte
	for run := range 2 {
		paired, err := renderTreesForTest(t.Context(), t, trees, Config{Concurrency: 2, CacheDir: t.TempDir()})
		assert.NoError(t, err)
		for side, rendered := range paired {
			assertBootstrapAuthoritySide(t, &[2]Rendered{rendered, full[side]}, transition, reverse)
		}
		body, _ := implicitSourceDiff(t, paired)
		want, _ := implicitSourceDiff(t, full)
		assert.Equal(t, string(body), string(want))
		if run > 0 {
			assert.Equal(t, string(body), string(previous))
		}
		previous = body
	}
}

func assertBootstrapAuthoritySide(t *testing.T, pair *[2]Rendered, transition string, reverse bool) {
	t.Helper()
	rendered, full := pair[0], pair[1]
	assert.Diff(t, rendered.Result.Failed, full.Result.Failed)
	assert.Diff(t, rendered.Result.Blocked, full.Result.Blocked)
	assert.Diff(t, rendered.Result.Warnings, full.Result.Warnings)
	assert.Diff(t, rendered.Result.ClusterWarnings, full.Result.ClusterWarnings)
	assert.Diff(t, outputByParent(rendered.Result.Manifests), outputByParent(full.Result.Manifests))
	consumer := testutil.KustomizationID("apps", "consumer")
	kind := manifest.KindOCIRepository
	if transition != "application removed" {
		kind = manifest.KindGitRepository
	}
	sourceID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "shared"}
	side := int(rendered.ID.Side)
	originalSide := side
	if reverse {
		originalSide = 1 - side
	}
	assert.Diff(t, outputByParent(rendered.Result.Manifests)[consumer], outputByParent(full.Result.Manifests)[consumer])
	artifact := rendered.Store().GetArtifact(sourceID, store.ArtifactContent)
	if transition == "application removed" && originalSide == 1 {
		assert.Equal(t, artifact == nil, true)
		assert.Equal(t, rendered.Store().GetObject(sourceID) == nil, true)
		assert.Equal(t, rendered.Store().GetObject(consumer) == nil, true)
		return
	}
	assert.FatalIf(t, artifact == nil, "side %d lost source", side)
	input := artifact.(*store.SourceArtifact)
	declared := transition != "application removed" && originalSide == 1
	assert.Equal(t, input.InputAlias, !declared)
	docs := outputByParent(rendered.Result.Manifests)[consumer]
	assert.Equal(t, len(docs), 1)
	value := "local"
	if declared {
		value = "remote"
	}
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"], any(value))
}
