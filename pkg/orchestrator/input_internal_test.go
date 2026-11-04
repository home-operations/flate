package orchestrator

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"log/slog"
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
	t.Run("prepares_each_side_once", testRenderTreesPreparesEachSideOnce)
	t.Run("flux_instance_sync_unchanged_root", testRenderTreesFluxInstanceSyncUnchangedRoot)
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
		assert.FatalIf(t, !ok, "side %d: child declaration missing", side)
		assert.Equal(t, obj.Path, "./workloads/"+want)
		docs := outputByParent(run.Result.Manifests)[child]
		assert.FatalIf(t, len(docs) != 1, "side %d: shaped child not rendered: %v", side, docs)
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

func testRenderTreesPreparesEachSideOnce(t *testing.T) {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
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
	builds := 0
	for line := range strings.SplitSeq(logs.String(), "\n") {
		var event struct {
			Msg  string `json:"msg"`
			Path string `json:"path"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Msg == "kustomize: build" &&
			manifest.NormalizeClaimBase(event.Path) == "." {
			builds++
		}
	}
	assert.Equal(t, builds, 0)
	t.Logf("paired root builds=%d", builds)
	for side, rendered := range []Rendered{base, head} {
		assert.Equal(t, rendered.entryBinding.Producer, entry)
		assert.Equal(t, readers[side].fingerprints.Load(), int64(2))
		assert.Equal(t, readers[side].parsed.Load(), int64(5))
		assert.Diff(t, configMapNames(rendered.Result), map[string]bool{"cfg-a": true})
		t.Logf(
			"side=%d declaration reads=%d selected evidence reads=%d",
			side,
			readers[side].parsed.Load(),
			readers[side].fingerprints.Load(),
		)
	}
	for side, root := range roots {
		o, err := newTestOrchestrator(
			t,
			Tree{
				RepoRoot:        root,
				Input:           inputs[side],
				BaseInput:       inputs[1-side],
				ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}),
			},
			Config{Concurrency: 2},
		)
		assert.NoError(t, err)
		assert.NoError(t, o.bootstrap(t.Context()))
		calls, admitted := map[manifest.NamedResource]*atomic.Int64{
			entry: {},
		}, map[manifest.NamedResource]*atomic.Int64{
			entry: {},
		}
		for id := range o.index.Records {
			if id.Kind == manifest.KindKustomization && id.Group == manifest.FluxKustomizeDomain {
				calls[id], admitted[id] = new(atomic.Int64), new(atomic.Int64)
			}
		}
		o.controllers.Kustomization = countedNodeController{o.ksc, calls, admitted}
		assert.NoError(t, o.run(t.Context()))
		t.Logf("side=%d root controller calls=%d admitted=%d", side, calls[entry].Load(), admitted[entry].Load())
		assert.Equal(t, admitted[entry].Load(), int64(0))
		o.stop()
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
			assert.FatalIf(t, !ok, "side %d has no selected pinned source", side)
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
		metadata := manifest.DocMetadata(docs[0])
		name, ns := metadata.Name, metadata.Namespace
		assert.FatalIf(t, name != "cfg-a", "side %d producer emitted %q, want cfg-a", side, name)
		assert.Equal(t, ns, "")
		parsed2, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
		obj := parsed2.Object
		assert.NoError(t, err)
		owners := testutil.Owners(rendered.Store().Productions(obj.Named(), nil))
		assert.FatalIf(
			t,
			!slices.Contains(owners, changed),
			"side %d owners=%v want %s",
			side,
			owners,
			changed,
		)
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
			Path:        filepath.Join(roots[1], "clusters", "a"),
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
					Path:            filepath.Join(roots[0], "clusters", "a"),
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					ExternalChanges: pairedCfg.ExternalChanges,
					Input:           pairedCfg.BaseInput,
					BaseInput:       pairedCfg.Input,
				},
				{
					Path:            filepath.Join(roots[1], "clusters", "a"),
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

func testRenderTreesFluxInstanceSyncUnchangedRoot(t *testing.T) {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	roots := [2]string{t.TempDir(), t.TempDir()}
	var inputs [2]*store.SourceArtifact
	const url = "https://example.test/sync/repo"
	for side, root := range roots {
		testutil.GitInit(t, root)
		testutil.WriteFile(
			t,
			root,
			"entry/generator.yaml",
			testutil.KustomizationYAML(
				"generator",
				"apps",
				"  path: ./entry/bootstrap\n  sourceRef: {kind: GitRepository, name: self}\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"entry/bootstrap/kustomization.yaml",
			"namespace: apps\nresources: [release.yaml]\n",
		)
		testutil.WriteFile(t, root, "entry/bootstrap/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: generator}
spec:
  chart:
    spec:
      chart: charts/bootstrap
      sourceRef: {kind: GitRepository, name: self, namespace: apps}
`)
		testutil.WriteFile(t, root, "charts/bootstrap/Chart.yaml", "apiVersion: v2\nname: bootstrap\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			root,
			"charts/bootstrap/templates/application.yaml",
			`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync: {name: self, kind: GitRepository, url: `+url+", ref: main, path: ./entry}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
			),
		)
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"workload/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: "+[]string{"before", "after"}[side]+"}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"outside/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: outside}\ndata: {value: "+[]string{"before", "after"}[side]+"}\n",
		)
		inputs[side] = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)}
	}
	results, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				ID:              manifest.TreeID{Cluster: "entry", Side: manifest.TreeBase},
				Path:            filepath.Join(roots[0], "entry"),
				RepoRoot:        roots[0],
				SelfURLs:        []string{url},
				Input:           inputs[0],
				BaseInput:       inputs[1],
				ExternalChanges: change.NewSet([]string{"workload/cm.yaml", "outside/cm.yaml"}),
			},
			{
				ID:              manifest.TreeID{Cluster: "entry", Side: manifest.TreeHead},
				Path:            filepath.Join(roots[1], "entry"),
				RepoRoot:        roots[1],
				SelfURLs:        []string{url},
				Input:           inputs[1],
				BaseInput:       inputs[0],
				ExternalChanges: change.NewSet([]string{"workload/cm.yaml", "outside/cm.yaml"}),
			},
		},
		Config{Concurrency: 2, Selection: manifest.Selection{Changed: true}},
	)
	assert.NoError(t, err)
	for line := range strings.SplitSeq(logs.String(), "\n") {
		var event struct {
			Msg  string `json:"msg"`
			Path string `json:"path"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Msg == "kustomize: build" &&
			manifest.NormalizeClaimBase(event.Path) == "entry" {
			t.Error("unchanged FluxInstance sync root was constructed")
		}
	}
	for side, result := range results {
		assert.Equal(t, result.entryBinding.Producer == (manifest.NamedResource{}), false)
		assert.Diff(t, configMapNames(result.Result), map[string]bool{"selected": true})
		for _, batch := range result.Result.Manifests {
			for _, doc := range batch.Documents {
				if doc["kind"] == "ConfigMap" {
					assert.Diff(t, doc["data"], any(map[string]any{"value": []string{"before", "after"}[side]}))
				}
			}
		}
	}
}
