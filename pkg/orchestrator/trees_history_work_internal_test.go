package orchestrator

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/baseline"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func crdMutationHistoryFixture(t *testing.T, tc crdMutationCase) (Tree, string, plumbing.Hash) {
	t.Helper()
	tree := crdWorkTree(t, tc.before, tc.foreign)
	testutil.WriteFile(t, tree.RepoRoot, "flux/kustomization.yaml", "resources: [source.yaml, owner.yaml, idle.yaml]\n")
	idle := strings.ReplaceAll(workWitnessFiles()["flux/release.yaml"], "name: release,", "name: idle,")
	idle = strings.ReplaceAll(idle, "charts/demo", "charts/idle")
	testutil.WriteFile(t, tree.RepoRoot, "flux/idle.yaml", idle)
	testutil.WriteFile(t, tree.RepoRoot, "charts/idle/Chart.yaml", "apiVersion: v2\nname: idle\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		tree.RepoRoot,
		"charts/idle/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: idle-output, namespace: apps}\n",
	)
	repo := testutil.GitInit(t, tree.RepoRoot)
	author := &object.Signature{Name: "fixture", Email: "fixture@example.test"}
	base := testutil.GitCommitAll(t, repo, "before CRD", &git.CommitOptions{Author: author})
	beforeRoot := t.TempDir()
	testutil.CopyTree(t, tree.RepoRoot, beforeRoot)
	resources := "resources: [selected.yaml, unrelated.yaml"
	if tc.after {
		resources += ", crd.yaml"
		body := helmReleaseCRD
		if tc.before {
			body = strings.Replace(
				body,
				"metadata: {name: helmreleases.helm.toolkit.fluxcd.io}",
				"metadata: {name: helmreleases.helm.toolkit.fluxcd.io, annotations: {revision: changed}}",
				1,
			)
		}
		if tc.fault {
			body = "apiVersion: [invalid\n"
		}
		testutil.WriteFile(t, tree.RepoRoot, "apps/crd.yaml", body)
	} else {
		assert.NoError(t, os.Remove(filepath.Join(tree.RepoRoot, "apps", "crd.yaml")))
	}
	testutil.WriteFile(t, tree.RepoRoot, "apps/kustomization.yaml", resources+"]\n")
	testutil.GitCommitAll(t, repo, "mutate CRD", &git.CommitOptions{Author: author})
	return tree, beforeRoot, base
}

type crdMutationCase struct {
	name           string
	before, after  bool
	foreign, fault bool
}

func mutateHistoryWitnessFiles(files, beforeFiles map[string]string, name string) {
	file := "apps/a/cm.yaml"
	files[file] = strings.ReplaceAll(files[file], "busybox:1", "busybox:2")
	if name == "untracked" || name == "add" {
		files["apps/a/kustomization.yaml"] = "resources: [cm.yaml, added.yaml]\ncomponents: [../../shared]\n"
		files["apps/a/added.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added, namespace: apps}\n"
	}
	switch name {
	case "remove":
		files["apps/a/kustomization.yaml"] = "resources: []\n"
		delete(files, file)
	case "rename-file":
		files["apps/a/kustomization.yaml"] = "resources: [renamed.yaml]\ncomponents: [../../shared]\n"
		files["apps/a/renamed.yaml"] = beforeFiles[file]
		delete(files, file)
	case "move-directory":
		files["flux/a.yaml"] = strings.ReplaceAll(files["flux/a.yaml"], "./apps/a", "./apps/moved")
		files["apps/moved/kustomization.yaml"] = beforeFiles["apps/a/kustomization.yaml"]
		files["apps/moved/cm.yaml"] = beforeFiles[file]
		delete(files, "apps/a/kustomization.yaml")
		delete(files, file)
	case "rename-object":
		files[file] = strings.ReplaceAll(beforeFiles[file], "a-output", "renamed-output")
	}
}

func TestRenderTrees_CRDMutationHistory(t *testing.T) {
	t.Parallel()
	for _, tc := range []crdMutationCase{
		{name: "33.9-add", after: true},
		{name: "33.9-change", before: true, after: true},
		{name: "33.9-remove", before: true},
		{name: "33.9-failure", before: true, after: true, fault: true},
		{name: "33.9-foreign", after: true, foreign: true},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); testCRDMutationHistoryWorkCase(t, &tc) })
	}
}

type historyRepository struct {
	root       string
	beforeRoot string
	ref        string
}

func historyWitnessRepository(t *testing.T, name string) historyRepository {
	t.Helper()
	files := maps.Clone(workWitnessFiles())
	files["flux/kustomization.yaml"] = "resources: [a.yaml, b.yaml, vars.yaml]\n"
	delete(files, "flux/release.yaml")
	delete(files, "flux/set.yaml")
	root := writeWitness(t, files)
	repo := testutil.GitInit(t, root)
	author := &object.Signature{Name: "fixture", Email: "fixture@example.test"}
	base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: author})
	beforeRoot := writeWitness(t, files)
	beforeFiles := maps.Clone(files)
	file := "apps/a/cm.yaml"
	mutateHistoryWitnessFiles(files, beforeFiles, name)
	for path := range beforeFiles {
		if _, exists := files[path]; !exists {
			assert.NoError(t, os.Remove(filepath.Join(root, path)))
		}
	}
	for path, body := range files {
		testutil.WriteFile(t, root, path, body)
	}
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	if name == "staged-revert" {
		_, err := wt.Add(file)
		assert.NoError(t, err)
	} else if name != "dirty" && name != "untracked" {
		testutil.GitCommitAll(t, repo, "edit", &git.CommitOptions{Author: author})
	}
	reverted := name == "staged-revert" || name == "endpoint-revert"
	if reverted {
		files[file] = workWitnessFiles()[file]
		testutil.WriteFile(t, root, file, files[file])
		if name == "endpoint-revert" {
			testutil.GitCommitAll(t, repo, "revert", &git.CommitOptions{Author: author})
		}
	}
	return historyRepository{root: root, beforeRoot: beforeRoot, ref: historyWitnessRef(t, repo, base, name)}
}

func (tc crdMutationCase) assertRun(t *testing.T, run Rendered, full *Result, side int) {
	t.Helper()
	ks := testutil.KustomizationID("apps", "owner")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "selected",
	}
	hr := testutil.HelmReleaseID("apps", "selected")
	assert.Diff(
		t,
		witnessWork(run.Result.Work, manifest.WorkKustomize),
		map[manifest.NamedResource]uint64{ks: 1},
	)
	owners := []manifest.NamedResource{ks, rs, hr}
	reached := tc.name != "33.9-change" && !tc.fault
	if !reached {
		owners = []manifest.NamedResource{ks}
	}
	want := witnessOutput(full, owners)
	if !reached && want[ks] != nil {
		chart := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindHelmChart,
			Namespace: "apps",
			Name:      "generated",
		}
		want[ks] = slices.DeleteFunc(slices.Clone(want[ks]), func(doc map[string]any) bool {
			id := manifest.DocIdentity(doc)
			return id == chart || id == hr
		})
	}
	assert.Diff(t, visibleWitnessOutput(run.Result), want)
	helm, evaluation := map[manifest.NamedResource]uint64{}, map[manifest.NamedResource]uint64{}
	present := tc.before
	if side == 1 {
		present = tc.after
	}
	if reached && present && !tc.foreign {
		helm[hr], evaluation[rs] = 1, 1
	}
	assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkHelm), helm)
	assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkEvaluation), evaluation)
	if reached && (!present || tc.foreign) {
		dependency := manifest.NamedResource{
			Group: "apiextensions.k8s.io",
			Kind:  manifest.KindCustomResourceDefinition,
			Name:  "helmreleases.helm.toolkit.fluxcd.io",
		}
		if tc.foreign {
			dependency.Group, dependency.Namespace = "fixture.example", "apps"
		}
		assert.Equal(t, run.Result.Failed[rs].Status, store.StatusFailed)
		assert.Diff(t, run.Result.Blocked[rs], []manifest.NamedResource{dependency})
	}
	if tc.fault && side == 1 {
		assert.Equal(t, run.Result.Failed[ks].Status, store.StatusFailed)
	}
}

func historyWitnessRef(t *testing.T, repo *git.Repository, base plumbing.Hash, name string) string {
	t.Helper()
	ref := base.String()
	switch name {
	case "branch":
		assert.NoError(
			t,
			repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("baseline"), base)),
		)
		ref = "baseline"
	case "tag":
		_, err := repo.CreateTag("baseline", base, nil)
		assert.NoError(t, err)
		ref = "baseline"
	case "merge-base":
		assert.NoError(
			t,
			repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewRemoteReferenceName("origin", "main"), base),
			),
		)
		ref = ""
	}
	return ref
}

func historyWorkTrees(resolved *baseline.Result, cluster manifest.ClusterID, selfURLs []string) []Tree {
	return []Tree{
		{
			ID:              manifest.TreeID{Cluster: cluster, Side: manifest.TreeBase},
			Input:           resolved.BaseInput,
			BaseInput:       resolved.Source,
			Path:            resolved.BaseScanPath,
			ExternalChanges: resolved.Changes,
			SelfURLs:        selfURLs,
		},
		{
			ID:              manifest.TreeID{Cluster: cluster, Side: manifest.TreeHead},
			Input:           resolved.Source,
			BaseInput:       resolved.BaseInput,
			Path:            resolved.ScanPath,
			ExternalChanges: resolved.Changes,
			SelfURLs:        selfURLs,
		},
	}
}

func TestRenderTrees_HistoryWorkWitnesses(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"commit", "branch", "tag", "merge-base", "dirty", "untracked", "add", "remove",
		"rename-file", "move-directory", "rename-object", "staged-revert", "endpoint-revert",
	} {
		t.Run(name, func(t *testing.T) { t.Parallel(); testHistoryWorkWitnessCase(t, &name) })
	}
}

func testCRDMutationHistoryWorkCase(t *testing.T, tc *crdMutationCase) {
	t.Helper()
	tree, beforeRoot, base := crdMutationHistoryFixture(t, (*tc))
	hr := testutil.HelmReleaseID("apps", "selected")
	var full [2]*Result
	for side, root := range []string{beforeRoot, tree.RepoRoot} {
		control := tree
		control.ID.Side, control.Input = manifest.TreeHead, nil
		control.RepoRoot, control.Path = root, filepath.Join(root, "flux")
		runs, err := renderTreesForTest(t.Context(), t, []Tree{control}, Config{RecordWork: true, Concurrency: 2})
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		full[side] = runs[0].Result
	}
	cache := cachetest.New(t, "", sourcecache.New)
	resolved, err := baseline.AutoResolve(t.Context(), tree.Path, base.String(), cache)
	assert.NoError(t, err)
	trees := historyWorkTrees(resolved, "crd-cluster", tree.SelfURLs)
	var previous []byte
	for repeat := range 2 {
		runs, runErr := renderTreesForTest(t.Context(), t, trees, Config{RecordWork: true, Concurrency: 2})
		assert.Equal(t, errors.Is(runErr, manifest.ErrFlux), tc.name != "33.9-change")
		for side, run := range runs {
			tc.assertRun(t, run, full[side], side)
			for recordIndex := range run.Result.Work {
				record := &run.Result.Work[recordIndex]
				assert.Equal(t, record.Key.Tree, trees[side].ID)
				assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
				if record.Key.Phase == manifest.WorkHelm {
					assert.Equal(t, record.Key.Owner, hr)
				}
			}
		}
		body, err := json.Marshal(
			[][]manifest.DocumentBatch{runs[0].Result.Manifests, runs[1].Result.Manifests},
		)
		assert.NoError(t, err)
		if repeat != 0 {
			assert.Equal(t, string(body), string(previous))
		}
		previous = body
	}
}

func testHistoryWorkWitnessCase(t *testing.T, name *string) {
	t.Helper()
	historyWitnessRepositoryResult := historyWitnessRepository(t, (*name))
	root, beforeRoot, ref := historyWitnessRepositoryResult.root, historyWitnessRepositoryResult.beforeRoot, historyWitnessRepositoryResult.ref
	reverted := (*name) == "staged-revert" || (*name) == "endpoint-revert"
	var full [2]*Result
	for side, directory := range []string{beforeRoot, root} {
		runs, err := renderTreesForTest(t.Context(), t,
			[]Tree{witnessTree(directory, manifest.TreeHead)},
			Config{RecordWork: true, Concurrency: 2},
		)
		assert.NoError(t, err)
		full[side] = runs[0].Result
	}
	cache := cachetest.New(t, "", sourcecache.New)
	resolved, err := baseline.AutoResolve(t.Context(), filepath.Join(root, "flux"), ref, cache)
	assert.NoError(t, err)
	cfg := Config{RecordWork: true, Concurrency: 2}
	trees := historyWorkTrees(resolved, ".", nil)
	var first []byte
	for repeat := range 2 {
		runs, err := renderTreesForTest(t.Context(), t, trees, cfg)
		assert.NoError(t, err)
		a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
		for side, run := range runs {
			var owners []manifest.NamedResource
			var attempts uint64
			if !reverted {
				owners, attempts = []manifest.NamedResource{a}, 1
			}
			assert.Diff(t, visibleWitnessOutput(run.Result), witnessOutput(full[side], owners))
			assert.Equal(t, workCounts(run.Result.Work, a, manifest.WorkKustomize).Attempts, attempts)
			assert.Equal(t, workCounts(run.Result.Work, b, manifest.WorkKustomize), manifest.WorkCounts{})
			assertWitnessSources(t, run)
		}
		body, err := json.Marshal(
			[][]manifest.DocumentBatch{runs[0].Result.Manifests, runs[1].Result.Manifests},
		)
		assert.NoError(t, err)
		if repeat == 0 {
			first = body
		} else {
			assert.Equal(t, string(body), string(first))
		}
	}
}
