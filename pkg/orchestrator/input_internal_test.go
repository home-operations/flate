package orchestrator

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type scopedPeerReader struct {
	fs.FS

	unrelated atomic.Int64
}

func (r *scopedPeerReader) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == "clusters/b" {
		r.unrelated.Add(1)
	}
	return fs.ReadDir(r.FS, name)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_AdmittedViews(t *testing.T) {
	for _, scenario := range []string{"scoped_peer", "equal_selected_alias", "selected_payload_only", "source_declaration_equal", "equal_labels", "synthetic_labels", "authored_pins", "committed_detection", "equal_selected_pin", "unavailable_host_decoy"} {
		t.Run(scenario, func(t *testing.T) {
			testRenderTreesAdmittedViewsCase(t, &scenario)
		},
		)
	}
}

func TestRenderTrees_EntrySubstitutionSelectsChangedChild(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [root.yaml, child.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"root.yaml",
			testutil.KustomizationYAML(
				"root",
				"flux-system",
				"  path: ./\n  postBuild:\n    substitute: {LOCATION: "+[]string{"before", "after"}[side]+"}\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"child.yaml",
			testutil.KustomizationYAML("child", "flux-system", "  path: ./workloads/${LOCATION}\n"),
		)
		for _, value := range []string{"before", "after"} {
			testutil.WriteFile(t, root, "workloads/"+value+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"workloads/"+value+"/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: "+value+"}\n",
			)
		}
	}
	runs, err := RenderTrees(t.Context(), []Tree{
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Path: roots[0]},
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: roots[1]},
	}, Config{Concurrency: 2})
	assert.NoError(t, err)
	child := testutil.KustomizationID("flux-system", "child")
	for side, run := range runs {
		want := []string{"before", "after"}[side]
		obj, ok := run.store.Get[*manifest.Kustomization](child)
		{
			assert.FatalIf(t, !ok, "side %d: child declaration missing", side)
		}
		assert.Equal(t, obj.Path, "./workloads/"+want)
		docs := outputByParent(run.Result.Manifests)[child]
		{
			assert.FatalIf(t, len(docs) != 1, "side %d: shaped child not rendered: %v", side, docs)
		}
		assert.Equal(t, docs[0]["data"].(map[string]any)["value"], any(want))
	}
}

type preparationReader struct {
	fs.FS

	parsed       atomic.Int64
	fingerprints atomic.Int64
}

func (r *preparationReader) Open(name string) (fs.File, error) {
	if name == "ks-a.yaml" {
		r.parsed.Add(1)
	}
	if name == "fingerprint.txt" {
		r.fingerprints.Add(1)
	}
	return r.FS.Open(name)
}

func TestRenderTrees_PreparesEachSideOnce(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	var readers [2]*preparationReader
	inputs := [2]*store.SourceArtifact{}
	for side, root := range roots {
		writeCluster(t, root, []string{"before", "after"}[side], "same")
		for _, name := range []string{"ks-a.yaml", "ks-b.yaml"} {
			body, err := os.ReadFile(filepath.Join(root, name))
			assert.NoError(t, err)
			testutil.WriteFile(
				t,
				root,
				name,
				strings.ReplaceAll(
					string(body),
					"name: flux-system, namespace: flux-system",
					"name: selected, namespace: flux-system",
				),
			)
		}
		testutil.WriteFile(t, root, "fingerprint.txt", "selected content evidence\n")
		readers[side] = &preparationReader{FS: os.DirFS(root)}
		inputs[side] = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: readers[side]}
	}
	renderedTrees3, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				RepoRoot:        roots[0],
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           inputs[0],
				BaseInput:       inputs[1],
				ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}),
			},
			{
				RepoRoot:        roots[1],
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           inputs[1],
				BaseInput:       inputs[0],
				ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}),
			},
		},
		Config{Concurrency: 2},
	)
	base, head := renderedTrees3[0], renderedTrees3[1]
	assert.NoError(t, err)
	for side, rendered := range []Rendered{base, head} {
		assert.Equal(t, readers[side].fingerprints.Load(), int64(2))
		// Mutable source proofs MUST retain two validations, alongside one census,
		// one fingerprint, one required build, and seven stat-only opens.
		assert.Equal(t, readers[side].parsed.Load(), int64(12))
		assert.Diff(t, configMapNames(rendered.Result), map[string]bool{"cfg-a": true})
		t.Logf(
			"side=%d declaration reads=%d selected evidence reads=%d",
			side,
			readers[side].parsed.Load(),
			readers[side].fingerprints.Load(),
		)
	}
}

func testRenderTreesAdmittedViewsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	baseRoot, headRoot := t.TempDir(), t.TempDir()
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, Tree: Tree{}}
	changed := testutil.KustomizationID("flux-system", "app-a")
	same := changed
	same.Name = "app-b"
	const url = "git://fixture.invalid/cluster"
	if returned := testRenderTreesAdmittedViewsCaseSegment(
		t,
		&testRenderTreesAdmittedViewsCaseSegmentInputs{
			testCaseInput: &testCaseInput,
			baseRoot:      &baseRoot,
			headRoot:      &headRoot,
			cfg:           &cfg,
			changed:       &changed,
			same:          &same,
		},
	); returned {
		return
	}
	renderedTrees1, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				Path:            baseRoot,
				SelfURLs:        []string{url},
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           cfg.BaseInput,
				BaseInput:       cfg.Input,
				ExternalChanges: cfg.ExternalChanges,
			},
			{
				Path:            headRoot,
				SelfURLs:        []string{url},
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
				Input:           cfg.Input,
			},
		},
		cfg.Config,
	)
	base, head := renderedTrees1[0], renderedTrees1[1]
	assert.NoError(t, err)
	if (*testCaseInput) == "equal_selected_pin" {
		for side, rendered := range []Rendered{base, head} {
			if len(outputByParent(rendered.Result.Manifests)[changed]) != 0 ||
				len(outputByParent(rendered.Result.Manifests)[same]) != 0 {
				t.Fatalf(
					"side %d rendered dirty checkout changes behind equal selected pin: %v",
					side,
					rendered.Result.Manifests,
				)
			}
			selected := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "cluster",
			}
			art, ok := rendered.Store().GetArtifact(selected, store.ArtifactContent).(*store.SourceArtifact)
			{
				assert.FatalIf(t, !ok, "side %d has no selected pinned source", side)
			}
			body, err := fs.ReadFile(art.FS, "apps/app-a/cm.yaml")
			assert.NoError(t, err)
			assert.Equal(
				t,
				string(body),
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-a}\ndata: {k: A}\n",
			)
		}
		return
	}
	for side, rendered := range []Rendered{base, head} {
		docs := outputByParent(rendered.Result.Manifests)[changed]
		if len(docs) != 1 || len(outputByParent(rendered.Result.Manifests)[same]) != 0 {
			t.Fatalf(
				"side %d lost producer ownership or unchanged selection: %v",
				side,
				rendered.Result.Manifests,
			)
		}
		name, ns := manifest.DocMetadata(docs[0])
		{
			assert.FatalIf(t, name != "cfg-a", "side %d producer emitted %q, want cfg-a", side, name)
		}
		assert.Equal(t, ns, "")
		obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
		assert.NoError(t, err)
		owners := testutil.Owners(rendered.Store().Productions(obj.Named(), nil))
		{
			assert.FatalIf(
				t,
				!slices.Contains(owners, changed),
				"side %d owners=%v want %s",
				side,
				owners,
				changed,
			)
		}
		{
			{
				value := docs[0]["data"].(map[string]any)["k"]
				assert.FatalIf(
					t,
					value != []string{"A", "B"}[side],
					"side %d read %v, want selected source bytes",
					side,
					value,
				)
			}
		}
	}
}

type testRenderTreesAdmittedViewsCaseCaseInputs struct {
	missing   *bool
	synthetic *bool
}

func testRenderTreesAdmittedViewsCaseCase(t *testing.T, testCaseInput testRenderTreesAdmittedViewsCaseCaseInputs) {
	t.Helper()

	roots := [2]string{t.TempDir(), t.TempDir()}
	inputs := [2]*store.SourceArtifact{}
	testRenderTreesAdmittedViewsCaseCaseSegment(t, &testCaseInput, &roots, &inputs)
	peerReader := &scopedPeerReader{FS: inputs[0].FS}
	if *testCaseInput.synthetic {
		inputs[0].FS = peerReader
	}
	cache := cachetest.New(t, "", sourcecache.New)
	var previous map[manifest.NamedResource][]map[string]any
	for run := range 2 {
		peerReader.unrelated.Store(0)
		directCfg := treeConfig{
			Cache:       cache,
			Concurrency: 2,
			Path:        filepath.Join(roots[1], "clusters/a"),
			Input:       inputs[1],
			BaseInput:   inputs[0],
		}
		direct, err := newTestOrchestrator(t, directCfg.Tree, directCfg.Config)
		assert.NoError(t, err)
		assert.NoError(t, direct.bootstrap(t.Context()))
		assert.NoError(t, direct.bootstrap(t.Context()))
		result, err := direct.render(t.Context())
		assert.NoError(t, err)
		assert.Diff(t, configMapNames(result), map[string]bool{"cfg-a": true})
		if *testCaseInput.synthetic {
			assert.Equal(t, peerReader.unrelated.Load(), int64(1))
		}
		foreign := testutil.KustomizationID("flux-system", "app-b")
		assert.Equal(t, direct.Store().GetObject(foreign) == nil, true)
		pairedCfg := directCfg
		pairedCfg.Path = ""
		renderedTrees1, err := RenderTrees(
			t.Context(),
			[]Tree{
				{
					Path:            filepath.Join(roots[0], "clusters/a"),
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					ExternalChanges: pairedCfg.ExternalChanges,
					Input:           pairedCfg.BaseInput,
					BaseInput:       pairedCfg.Input,
				},
				{
					Path:            filepath.Join(roots[1], "clusters/a"),
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:           pairedCfg.Input,
					BaseInput:       pairedCfg.BaseInput,
					ExternalChanges: pairedCfg.ExternalChanges,
				},
			},
			pairedCfg.Config,
		)
		_, head := renderedTrees1[0], renderedTrees1[1]
		assert.NoError(t, err)
		assert.Diff(t, outputByParent(result.Manifests), outputByParent(head.Result.Manifests))
		if run == 1 {
			assert.Diff(t, outputByParent(result.Manifests), previous)
		}
		previous = outputByParent(result.Manifests)
	}
}

func testRenderTreesAdmittedViewsCaseCaseSegment(
	t *testing.T,
	testCaseInput *testRenderTreesAdmittedViewsCaseCaseInputs,
	roots *[2]string,
	inputs *[2]*store.SourceArtifact,
) {
	t.Helper()
	for side, root := range *roots {
		writeCluster(t, root, []string{"A", "B"}[side], "same")
		testutil.WriteFile(t, root, "clusters/b/broken.yaml", "invalid: [\n")
		if side == 1 || !(*testCaseInput.missing) {
			body, err := os.ReadFile(filepath.Join(root, "ks-a.yaml"))
			assert.NoError(t, err)
			testutil.WriteFile(t, root, "clusters/a/ks.yaml", string(body))
		} else {
			testutil.WriteFile(t, root, "clusters/a/kustomization.yaml", "resources: []\n")
		}
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
		assert.NoError(t, err)
		if *testCaseInput.synthetic {
			files := fstest.MapFS{}
			assert.NoError(
				t,
				fs.WalkDir(
					input.FS,
					".",
					func(name string, entry fs.DirEntry, err error) error {
						if err != nil {
							return err
						}
						if !entry.IsDir() {
							data, err := fs.ReadFile(input.FS, name)
							if err != nil {
								return err
							}
							files[name] = &fstest.MapFile{Data: data}
						}
						return nil
					},
				),
			)
			input.FS = files
			input.LocalPath = filepath.Join(root, "label")
			input.LocalRoot = input.LocalPath
			roots[side] = input.LocalPath
		}
		inputs[side] = input
	}
}
