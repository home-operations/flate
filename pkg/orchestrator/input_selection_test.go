package orchestrator

import (
	"fmt"
	"slices"
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

func TestRenderTrees_SelectedSelfSourceReproducer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	const url = "git://fixture.invalid/cluster"
	source := (&manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}).Named()
	want := []string{source.String(), (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "selected"}).String()}
	testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+", ref: {branch: selected}}\n")
	for i := range 145 {
		name := fmt.Sprintf("app-%03d", i)
		id := testutil.KustomizationID("flux-system", name)
		want = append(want, id.String())
		testutil.WriteFile(t, root, name+".yaml", testutil.KustomizationYAML(name, id.Namespace, "  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
	}
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
	cfg := Config{Cache: cache, Concurrency: 2, Input: selected.Source, BaseInput: selected.BaseInput, ExternalChanges: selected.Changes, SelfURLs: []string{url}}
	for side := range 2 {
		one := cfg
		if side == 0 {
			one.Input, one.BaseInput = cfg.BaseInput, cfg.Input
		}
		one.Path = one.Input.LocalPath
		o, err := New(t.Context(), one)
		assert.NoError(t, err)
		assert.NoError(t, o.Bootstrap(t.Context()))
		if side == 0 {
			assert.Diff(t, o.filter.KeepNames(), []string{})
		} else {
			assert.Diff(t, o.filter.KeepNames(), want)
		}
		t.Logf("current-only side=%d exact keep=%v", side, o.filter.KeepNames())
		assert.NoError(t, o.Stop())
	}
	for run := range 2 {
		base, head, err := RenderTrees(t.Context(), Tree{RepoRoot: cfg.BaseInput.LocalPath, SelfURLs: []string{url}}, Tree{RepoRoot: root, SelfURLs: []string{url}}, cfg)
		assert.NoError(t, err)
		for side, rendered := range []Rendered{base, head} {
			assert.Diff(t, rendered.filter.KeepNames(), []string{})
			assert.Equal(t, len(rendered.Result.Manifests), 0)
			delta, err := change.Detect(t.Context(), base.store.GetArtifact(source).(*store.SourceArtifact), head.store.GetArtifact(source).(*store.SourceArtifact))
			assert.NoError(t, err)
			assert.Diff(t, delta.Paths(), []string(nil))
			t.Logf("paired run=%d side=%d exact keep=[] rendered bytes=empty", run, side)
		}
	}
}
