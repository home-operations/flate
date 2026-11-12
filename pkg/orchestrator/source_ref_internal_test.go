package orchestrator

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRun_DefaultGitCache(t *testing.T) {
	t.Run("supplied source references", testSuppliedSourceReferences)
	t.Run("supplied source cycle", testSuppliedSourceCycle)
	t.Run("supplied applier readiness", testSuppliedApplierReadiness)
	for _, mode := range []string{"memory", "disk", "injected"} {
		t.Run(mode, func(t *testing.T) {
			testRunDefaultGitCacheCase(t, &mode)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_LocalSourceUsesSelectedCache(t *testing.T) {
	for _, kind := range []string{"configured-directory", "injected-disk", "injected-memory"} {
		t.Run(kind, func(t *testing.T) {
			testBootstrapLocalSourceUsesSelectedCacheCase(
				t,
				&kind,
			)
		},
		)
	}
}

func testRunDefaultGitCacheCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	remote := t.TempDir()
	repo := testutil.GitInit(t, remote)
	testutil.WriteFile(t, remote, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		remote,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\ndata: {value: committed}\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"source",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: flux-system}\nspec:\n  url: file://"+filepath.ToSlash(
			remote,
		)+"\n  interval: 10m\n",
	)
	for _, name := range []string{"first", "second"} {
		testutil.WriteFile(
			t,
			root,
			name+".yaml",
			testutil.KustomizationYAML(
				name,
				"flux-system",
				"  {path: ./app, sourceRef: {kind: GitRepository, name: remote}}\n",
			),
		)
	}
	cfg := treeConfig{Concurrency: 2, Path: root}
	if (*testCaseInput) == "disk" {
		cfg.CacheDir = t.TempDir()
	}
	if (*testCaseInput) == "injected" {
		cfg.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
	}
	o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		o.stop()
	})
	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := o.run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	for _, name := range []string{"first", "second"} {
		id := testutil.KustomizationID("flux-system", name)
		artifact, ok := o.Store().GetArtifact(id, store.ArtifactContent).(*store.Emission)
		assert.FatalIf(t, !ok || len(artifact.Manifests) != 1, "remote render %s = %v", name, artifact)
	}
}

func testBootstrapLocalSourceUsesSelectedCacheCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
	for i, value := range []string{"A", "B"} {
		testutil.WriteFile(t, root, "value.txt", value)
		hash := testutil.GitCommitAll(
			t,
			repo,
			value,
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
		)
		if i == 0 {
			{
				err := repo.Storer.SetReference(
					plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), hash),
				)
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
	}
	selectedRoot := t.TempDir()
	cfg := treeConfig{
		CacheDir: selectedRoot, Concurrency: 2, Path: filepath.Join(root, "flux"), RepoRoot: root,
		SelfURLs: []string{"git://fixture.invalid/cluster"},
	}
	if (*testCaseInput) != "configured-directory" {
		ownerRoot := ""
		if (*testCaseInput) == "injected-disk" {
			ownerRoot = t.TempDir()
		}
		cfg.Cache = cachetest.New(t, ownerRoot, sourcecache.New)
	}
	o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		o.stop()
	})
	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assert.FatalIf(
		t,
		o.hrc.Helm == nil || o.ksc.Trees == nil || (cfg.Cache != nil && o.cache != cfg.Cache),
		"%v",
		"renderers did not receive the selected cache before discovery",
	)
	if cfg.Cache != nil {
		entries, err := os.ReadDir(selectedRoot)
		assert.FatalIf(
			t,
			err != nil || len(entries) != 0,
			"injected owner created independent cache backing: entries=%v err=%v",
			entries,
			err,
		)
	}
	id := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "cluster",
	}
	{
		err := o.run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	artifact := o.Store().GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(
		t,
		artifact.FS == nil || artifact.Key == "",
		"selected cache %s did not reach discovery: %+v",
		selectedRoot,
		artifact,
	)
	{
		content, err := fs.ReadFile(artifact.FS, "value.txt")
		assert.FatalIf(t, err != nil || string(content) != "A", "committed artifact = %q, %v", content, err)
	}
}
