package orchestrator

import (
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_UnchangedNamedSelection(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	for _, root := range roots {
		writeCluster(t, root, "same", "same")
	}
	runs, err := RenderTrees(t.Context(), []Tree{
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Path: roots[0]},
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: roots[1]},
	}, Config{Concurrency: 2})
	assert.NoError(t, err)
	id := testutil.KustomizationID("flux-system", "app-a")
	selected, selectedErr, remainder := Select(runs, err, []manifest.NamedResource{id}, nil)
	assert.NoError(t, selectedErr)
	assert.NoError(t, remainder)
	assert.Equal(t, len(selected), 2)
	for side, result := range selected {
		_, known := runs[side].index.Lookup(id)
		assert.Equal(t, known, true)
		assert.Equal(t, runs[side].store.GetObject(id) == nil, true)
		assert.Equal(t, result.Counts[manifest.KindKustomization], 1)
		if len(result.Objects) != 1 {
			t.Fatalf("side %d: unchanged named declaration missing: %+v", side, result)
		}
		assert.Equal(t, result.Objects[0].Named(), id)
		assert.Equal(t, len(result.Manifests), 0)
	}
}

func TestRenderTrees_SelectedSelfSourceReproducer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	const url = "git://fixture.invalid/cluster"
	source := (&manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}).Named()
	want := []string{source.String(), (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "selected"}).String()}
	files := []string{"source.yaml"}
	var resources strings.Builder
	resources.WriteString("resources:\n- source.yaml\n")
	testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+", ref: {branch: selected}}\n")
	for i := range 145 {
		name := fmt.Sprintf("app-%03d", i)
		id := testutil.KustomizationID("flux-system", name)
		want = append(want, id.String())
		files = append(files, name+".yaml")
		fmt.Fprintf(&resources, "- %s.yaml\n", name)
		testutil.WriteFile(t, root, name+".yaml", testutil.KustomizationYAML(name, id.Namespace, "  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
	}
	testutil.WriteFile(t, root, "kustomization.yaml", resources.String())
	slices.Sort(want)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: before}\n")
	baseHash := testutil.GitCommitAll(t, repo, "before", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: after}\n")
	headHash := testutil.GitCommitAll(t, repo, "after", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("selected"), headHash)))
	cache := cachetest.New(t, "", sourcecache.New)
	selected, err := baseline.AutoResolve(t.Context(), root, baseHash.String(), cache)
	assert.NoError(t, err)
	var declarations []map[string]any
	for _, file := range files {
		body, err := fs.ReadFile(selected.Source.FS, file)
		assert.NoError(t, err)
		parsed, err := testutil.DecodeDocs(strings.NewReader(string(body)))
		assert.NoError(t, err)
		declarations = append(declarations, parsed...)
	}
	cfg := treeConfig{Cache: cache, Concurrency: 2, Input: selected.Source, BaseInput: selected.BaseInput, ExternalChanges: selected.Changes, SelfURLs: []string{url}}
	for side := range 2 {
		one := cfg
		if side == 0 {
			one.Input, one.BaseInput = cfg.BaseInput, cfg.Input
		}
		one.Path = one.Input.LocalPath
		o, err := newTestOrchestrator(t, one.Tree, one.Config)
		assert.NoError(t, err)
		assert.NoError(t, o.bootstrap(t.Context()))
		prerequisites := []string{o.entryBinding.Producer.String(), manifest.BootstrapSourceID.String()}
		slices.Sort(prerequisites)
		if side == 0 {
			assert.Diff(t, o.filter.KeepNames(), prerequisites)
		} else {
			expected := append(slices.Clone(want), prerequisites...)
			slices.Sort(expected)
			assert.Diff(t, o.filter.KeepNames(), expected)
		}
		t.Logf("current-only side=%d exact keep=%v", side, o.filter.KeepNames())
		o.stop()
	}
	for run := range 2 {
		renderedTrees1, err := RenderTrees(t.Context(), []Tree{{RepoRoot: cfg.BaseInput.LocalPath, SelfURLs: []string{url}, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, ExternalChanges: cfg.ExternalChanges, Input: cfg.BaseInput, BaseInput: cfg.Input}, {RepoRoot: root, SelfURLs: []string{url}, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
		base, head := renderedTrees1[0], renderedTrees1[1]
		assert.NoError(t, err)
		for side, rendered := range []Rendered{base, head} {
			prerequisites := []string{rendered.entryBinding.Producer.String(), manifest.BootstrapSourceID.String()}
			slices.Sort(prerequisites)
			assert.Diff(t, rendered.filter.KeepNames(), prerequisites)
			assert.Diff(t, outputByParent(rendered.Result.Manifests), map[manifest.NamedResource][]map[string]any{rendered.entryBinding.Producer: declarations})
			delta, err := change.Detect(t.Context(), base.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact), head.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact))
			assert.NoError(t, err)
			assert.Diff(t, delta.Paths(), []string(nil))
			t.Logf("paired run=%d side=%d kept prerequisites=%v root declarations=%d", run, side, prerequisites, len(declarations))
		}
	}
}
