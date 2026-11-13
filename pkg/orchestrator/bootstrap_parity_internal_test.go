package orchestrator

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v6"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The orchestrator helper changes the process-global egress guard.
func TestRenderTrees_BootstrapDeclaredSelfSource(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [source.yaml, consumer.yaml]\n")
	testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: theshire, namespace: flux-system}
spec:
  url: https://fixture.invalid/theshire.git
  ref: {branch: main}
  ignore: |
    /*
    !/flux
    !/apps
`)
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system",
		"  path: ./apps\n  sourceRef: {kind: GitRepository, name: theshire}\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [hello.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/hello.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {value: committed}\n",
	)
	repository := testutil.GitInit(t, root)
	testutil.GitCommitAll(
		t,
		repository,
		"source",
		&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
	)
	testutil.WriteFile(
		t,
		root,
		"apps/hello.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {value: dirty}\n",
	)
	o, err := newTestOrchestrator(t, Tree{
		Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/theshire"},
	}, Config{Concurrency: 2, CacheDir: t.TempDir()})
	assert.NoError(t, err)
	fetcher := &generatedOCIFetcher{err: errors.New("declared self source attempted remote acquisition")}
	o.src.Fetchers[manifest.KindGitRepository] = fetcher
	assert.NoError(t, o.bootstrap(t.Context()))
	result, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(result.Close)
	assert.Equal(t, fetcher.calls.Load(), int64(0))
	assert.NoError(t, err)
	owner := testutil.KustomizationID("flux-system", "cluster-apps")
	assert.Diff(t, outputByParent(result.Manifests)[owner], []map[string]any{{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{
			"name": "hello", "labels": map[string]any{
				"kustomize.toolkit.fluxcd.io/name":      "cluster-apps",
				"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
			},
		}, "data": map[string]any{"value": "dirty"},
	}})
}

//nolint:paralleltest // RenderTrees changes the process-global egress guard.
func TestRenderTrees_ChangedOnlyAndDiff_BootstrapPinnedIgnore(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, len(roots))
	for side, root := range roots {
		testutil.WriteFile(t, root, "flux/config/kustomization.yaml", "resources: [source.yaml, consumer.yaml]\n")
		testutil.WriteFile(t, root, "flux/config/source.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec:
  sync:
    name: cluster-repository
    kind: GitRepository
    url: https://fixture.invalid/cluster.git
    ref: refs/tags/production
    path: flux/config
`)
		testutil.WriteFile(
			t,
			root,
			"flux/config/consumer.yaml",
			testutil.KustomizationYAML("cluster-apps", "flux-system",
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster-repository}\n"),
		)
		testutil.WriteFile(t, root, "apps/.krmignore", "ignored.yaml\n")
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [hello.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/hello.yaml",
			fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {value: '%d'}\n", side),
		)
		repository := testutil.GitInit(t, root)
		hash := testutil.GitCommitAll(
			t,
			repository,
			"pinned source",
			&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
		)
		assert.NoError(
			t,
			repository.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("production"), hash)),
		)
		_, err := repository.CreateRemote(
			&gitconfig.RemoteConfig{Name: "origin", URLs: []string{"https://fixture.invalid/cluster.git"}},
		)
		assert.NoError(t, err)
		trees[side] = Tree{
			Path: filepath.Join(root, "flux"), RepoRoot: root,
			ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	full := make([]Rendered, len(trees))
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
	for _, reverse := range []bool{false, true} {
		ordered, oracles := trees, full
		if reverse {
			ordered = []Tree{trees[1], trees[0]}
			ordered[0].ID.Side, ordered[1].ID.Side = manifest.TreeBase, manifest.TreeHead
			oracles = []Rendered{full[1], full[0]}
		}
		var first []byte
		for run := range 2 {
			paired, err := renderTreesForTest(t.Context(), t, ordered, Config{Concurrency: 2, CacheDir: t.TempDir()})
			assert.NoError(t, err)
			for side, rendered := range paired {
				owner := testutil.KustomizationID("flux-system", "cluster-apps")
				assert.Diff(
					t,
					outputByParent(rendered.Result.Manifests)[owner],
					outputByParent(oracles[side].Result.Manifests)[owner],
				)
			}
			body, _ := implicitSourceDiff(t, paired)
			want, _ := implicitSourceDiff(t, oracles)
			assert.Equal(t, string(body), string(want))
			if run == 0 {
				first = body
			} else {
				assert.Equal(t, string(body), string(first))
			}
		}
	}
}

//nolint:paralleltest // RenderTrees changes the process-global egress guard.
func TestRenderTrees_BootstrapAliasEntryFailure(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"entry/kustomization.yaml",
		"resources: [consumer.yaml, platform.yaml, independent.yaml]\nconfigMapGenerator:\n- name: cluster-vars\n  envs: [.cluster-vars.env]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"entry/consumer.yaml",
		testutil.KustomizationYAML("cluster-config", "flux-system", `  path: ./payload
  sourceRef: {kind: OCIRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: cluster-vars}]}
`),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/platform.yaml",
		testutil.KustomizationYAML("platform", "flux-system", `  path: ./payload
  sourceRef: {kind: OCIRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: cluster-vars}]}
`),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/independent.yaml",
		testutil.KustomizationYAML("velero-restore", "flux-system", `  path: ./empty
  sourceRef: {kind: OCIRepository, name: flux-system}
`),
	)
	testutil.WriteFile(t, root, "empty/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [hello.yaml]\n")
	testutil.WriteFile(t, root, "payload/hello.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\n")
	runs, err := renderTreesForTest(
		t.Context(),
		t,
		[]Tree{
			{
				Path:     filepath.Join(root, "entry"),
				RepoRoot: root,
				ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			},
		},
		Config{Concurrency: 2, CacheDir: t.TempDir()},
	)
	assert.Equal(t, err != nil, true)
	assert.FatalIf(t, len(runs) != 1, "entry failure lost result: %v", err)
	sourceID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "flux-system",
	}
	status, found := runs[0].Store().GetStatus(sourceID)
	assert.Equal(t, found, true)
	assert.Equal(t, status.Status, store.StatusReady)
	artifact, ok := runs[0].Store().GetArtifact(sourceID, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok || artifact.FS == nil, "ready alias has no published content")
	assert.Equal(t, artifact.InputAlias, true)
	assert.Equal(t, artifact.Reads.Valid(), true)
	payload, readErr := fs.ReadFile(artifact.FS, "payload/hello.yaml")
	assert.NoError(t, readErr)
	assert.Equal(t, string(payload), "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\n")
	assert.FatalIf(t, runs[0].entryBinding.Failure == nil, "entry lost the missing generator input failure")
	assert.Equal(
		t,
		runs[0].entryBinding.Failure.Error(),
		"kustomize build entry: loading KV pairs: env source files: [.cluster-vars.env]: resolve entry/.cluster-vars.env: stat entry/.cluster-vars.env: file does not exist",
	)
	consumer := testutil.KustomizationID("flux-system", "cluster-config")
	status, found = runs[0].Store().GetStatus(consumer)
	assert.Equal(t, found, true)
	assert.Equal(t, status.Outcome(), store.OutcomeBlocked)
	assert.Equal(
		t,
		status.Message,
		"dependencies failed: ConfigMap/flux-system/cluster-vars: not found; supply it with --supply",
	)
	platform := testutil.KustomizationID("flux-system", "platform")
	platformStatus, found := runs[0].Store().GetStatus(platform)
	assert.Equal(t, found, true)
	assert.Equal(t, platformStatus.Outcome(), store.OutcomeBlocked)
	assert.Equal(t, platformStatus.Message, status.Message)
	missing := manifest.NamedResource{Kind: "ConfigMap", Namespace: "flux-system", Name: "cluster-vars"}
	assert.Diff(t, runs[0].Result.Blocked[consumer], []manifest.NamedResource{missing})
	assert.Diff(t, runs[0].Result.Blocked[platform], []manifest.NamedResource{missing})
	independentStatus, found := runs[0].Store().GetStatus(testutil.KustomizationID("flux-system", "velero-restore"))
	assert.Equal(t, found, true)
	assert.Equal(t, independentStatus.Status, store.StatusReady)
}

//nolint:paralleltest // RenderTrees changes the process-global egress guard.
func TestRenderTrees_BootstrapInferredRemote(t *testing.T) {
	root, remote := t.TempDir(), t.TempDir()
	remoteURL := "https://fixture.invalid/theshire.git"
	for _, path := range []string{root, remote} {
		testutil.WriteFile(t, path, "flux/kustomization.yaml", "resources: [source.yaml, consumer.yaml]\n")
		testutil.WriteFile(
			t,
			path,
			"flux/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: theshire, namespace: flux-system}\nspec: {url: '"+remoteURL+"', ref: {branch: main}}\n",
		)
		testutil.WriteFile(
			t,
			path,
			"flux/consumer.yaml",
			testutil.KustomizationYAML(
				"cluster-apps",
				"flux-system",
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: theshire}\n",
			),
		)
		testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [hello.yaml]\n")
		testutil.WriteFile(
			t,
			path,
			"apps/hello.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {value: remote}\n",
		)
		repository := testutil.GitInit(t, path)
		hash := testutil.GitCommitAll(
			t,
			repository,
			"source",
			&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
		)
		assert.NoError(
			t,
			repository.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), hash)),
		)
		if path == root {
			_, err := repository.CreateRemote(&gitconfig.RemoteConfig{Name: "origin", URLs: []string{remoteURL}})
			assert.NoError(t, err)
		}
	}
	testutil.WriteFile(
		t,
		root,
		"apps/hello.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {value: dirty}\n",
	)
	runs, err := renderTreesForTest(
		t.Context(),
		t,
		[]Tree{
			{
				Path:     filepath.Join(root, "flux"),
				RepoRoot: root,
				ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			},
		},
		Config{Concurrency: 2, CacheDir: t.TempDir()},
	)
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1, "self source lost result: %v", err)
	owner := testutil.KustomizationID("flux-system", "cluster-apps")
	docs := outputByParent(runs[0].Result.Manifests)[owner]
	assert.FatalIf(t, len(docs) != 1, "self source lost output: %+v", runs[0].Result)
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"], any("dirty"))
}

//nolint:paralleltest // RenderTrees changes the process-global egress guard.
func TestRenderTrees_BootstrapDeclaredSourceChartAuthority(t *testing.T) {
	root, remote := t.TempDir(), t.TempDir()
	remoteURL := "https://fixture.invalid/theshire.git"
	for _, path := range []string{root, remote} {
		testutil.WriteFile(t, path, "flux/kustomization.yaml", "resources: [source.yaml, consumer.yaml]\n")
		testutil.WriteFile(
			t,
			path,
			"flux/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster-repository, namespace: flux-system}\nspec: {url: '"+remoteURL+"', ref: {branch: main}}\n",
		)
		testutil.WriteFile(
			t,
			path,
			"flux/consumer.yaml",
			testutil.KustomizationYAML(
				"cluster-apps",
				"flux-system",
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster-repository}\n",
			),
		)
		testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [helmrelease.yaml]\n")
		version := "5.3.0"
		if path == root {
			version = "5.2.1"
		}
		testutil.WriteFile(
			t,
			path,
			"apps/helmrelease.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: apache-musicindex, namespace: apache-musicindex}\nspec:\n  suspend: true\n  chart:\n    spec:\n      chart: app-template\n      version: "+version+"\n      sourceRef: {kind: HelmRepository, name: bjw-s, namespace: flux-system}\n",
		)
		repository := testutil.GitInit(t, path)
		hash := testutil.GitCommitAll(
			t,
			repository,
			"source",
			&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
		)
		assert.NoError(
			t,
			repository.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), hash)),
		)
		if path == root {
			_, err := repository.CreateRemote(&gitconfig.RemoteConfig{Name: "origin", URLs: []string{remoteURL}})
			assert.NoError(t, err)
		}
	}
	runs, err := renderTreesForTest(
		t.Context(),
		t,
		[]Tree{
			{
				Path:     filepath.Join(root, "flux"),
				RepoRoot: root,
				ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			},
		},
		Config{Concurrency: 2, CacheDir: t.TempDir()},
	)
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1, "chart authority lost result: %v", err)
	owner := testutil.KustomizationID("flux-system", "cluster-apps")
	assert.Diff(t, outputByParent(runs[0].Result.Manifests)[owner], []map[string]any{
		{
			"apiVersion": "helm.toolkit.fluxcd.io/v2",
			"kind":       "HelmRelease",
			"metadata": map[string]any{
				"name":      "apache-musicindex",
				"namespace": "apache-musicindex",
				"labels": map[string]any{
					"kustomize.toolkit.fluxcd.io/name":      "cluster-apps",
					"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
				},
			},
			"spec": map[string]any{"suspend": true, "chart": map[string]any{"spec": map[string]any{
				"chart":     "app-template",
				"version":   "5.2.1",
				"sourceRef": map[string]any{"kind": "HelmRepository", "name": "bjw-s", "namespace": "flux-system"},
			}}},
		},
	})
}

//nolint:paralleltest // RenderTrees changes the process-global egress guard.
func TestRenderTrees_BootstrapSyncChartAuthority(t *testing.T) {
	for _, tc := range []struct {
		ref, version string
		inputAlias   bool
	}{
		{ref: "refs/tags/production", version: "5.2.1", inputAlias: true},
		{ref: "refs/heads/main", version: "5.2.1", inputAlias: true},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/config/kustomization.yaml", "resources: [sync.yaml, consumer.yaml]\n")
			testutil.WriteFile(t, root, "flux/config/sync.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec:
  sync:
    name: cluster-repository
    kind: GitRepository
    url: https://fixture.invalid/cluster.git
    ref: `+tc.ref+`
    path: flux/config
`)
			testutil.WriteFile(
				t,
				root,
				"flux/config/consumer.yaml",
				testutil.KustomizationYAML("cluster-apps", "flux-system",
					"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster-repository}\n"),
			)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [helmrelease.yaml]\n")
			release := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: apache-musicindex, namespace: apache-musicindex}\nspec:\n  suspend: true\n  chart:\n    spec:\n      chart: app-template\n      version: %s\n      sourceRef: {kind: HelmRepository, name: bjw-s, namespace: flux-system}\n"
			testutil.WriteFile(t, root, "apps/helmrelease.yaml", fmt.Sprintf(release, "5.2.1"))
			repository := testutil.GitInit(t, root)
			options := &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}}
			checkout := testutil.GitCommitAll(t, repository, "checkout", options)
			testutil.WriteFile(t, root, "apps/helmrelease.yaml", fmt.Sprintf(release, "5.3.0"))
			selected := testutil.GitCommitAll(t, repository, "authored reference", options)
			assert.NoError(
				t,
				repository.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(tc.ref), selected)),
			)
			assert.NoError(t, repository.Storer.SetReference(plumbing.NewHashReference(plumbing.HEAD, checkout)))
			testutil.WriteFile(t, root, "apps/helmrelease.yaml", fmt.Sprintf(release, "5.2.1"))
			_, err := repository.CreateRemote(
				&gitconfig.RemoteConfig{Name: "origin", URLs: []string{"https://fixture.invalid/cluster.git"}},
			)
			assert.NoError(t, err)
			for range 2 {
				runs, err := renderTreesForTest(t.Context(), t, []Tree{{
					Path: filepath.Join(root, "flux"), RepoRoot: root,
					ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				}}, Config{Concurrency: 2, CacheDir: t.TempDir()})
				assert.NoError(t, err)
				assert.FatalIf(t, len(runs) != 1, "sync source lost result: %v", err)
				sourceID := manifest.NamedResource{
					Group: manifest.SourceDomain, Kind: manifest.KindGitRepository,
					Namespace: "flux-system", Name: "cluster-repository",
				}
				material, ok := runs[0].Store().GetArtifact(sourceID, store.ArtifactContent).(*store.SourceArtifact)
				assert.FatalIf(t, !ok || material.FS == nil, "entry source content is unpublished")
				assert.Equal(t, material.InputAlias, tc.inputAlias)
				body, err := fs.ReadFile(material.FS, "apps/helmrelease.yaml")
				assert.NoError(t, err)
				assert.Equal(t, string(body), fmt.Sprintf(release, tc.version))
				owner := testutil.KustomizationID("flux-system", "cluster-apps")
				assert.Diff(t, outputByParent(runs[0].Result.Manifests)[owner], []map[string]any{
					{
						"apiVersion": "helm.toolkit.fluxcd.io/v2",
						"kind":       "HelmRelease",
						"metadata": map[string]any{
							"name":      "apache-musicindex",
							"namespace": "apache-musicindex",
							"labels": map[string]any{
								"kustomize.toolkit.fluxcd.io/name":      "cluster-apps",
								"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
							},
						},
						"spec": map[string]any{"suspend": true, "chart": map[string]any{"spec": map[string]any{
							"chart":   "app-template",
							"version": tc.version,
							"sourceRef": map[string]any{
								"kind":      "HelmRepository",
								"name":      "bjw-s",
								"namespace": "flux-system",
							},
						}}},
					},
				})
			}
		})
	}
}
