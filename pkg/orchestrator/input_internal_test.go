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

	"github.com/home-operations/flate/pkg/selection"

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
	runs, err := renderTreesForTest(t.Context(), t, []Tree{
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
	renderedTrees3, err := renderTreesForTest(t.Context(), t,
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
		Config{RecordWork: true, Concurrency: 2},
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
	t.Run(
		"work_visits",
		func(t *testing.T) {
			t.Parallel()
			testPreparedSideWorkVisits(t, roots, &[2]Rendered{base, head})
		},
	)
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
	renderedTrees1, err := renderTreesForTest(t.Context(), t,
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
			assert.FatalIf(t, len(outputByParent(rendered.Result.Manifests)[changed]) != 0 ||
				len(
					outputByParent(rendered.Result.Manifests)[same],
				) != 0, "side %d rendered dirty checkout changes behind equal selected pin: %v", side, rendered.Result.Manifests)
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
		assert.FatalIf(
			t,
			len(docs) != 1 || len(outputByParent(rendered.Result.Manifests)[same]) != 0,
			"side %d lost producer ownership or unchanged selection: %v",
			side,
			rendered.Result.Manifests,
		)
		docMetadataResult := manifest.DocMetadata(docs[0])
		name, ns := docMetadataResult.Name, docMetadataResult.Namespace

		assert.FatalIf(t, name != "cfg-a", "side %d producer emitted %q, want cfg-a", side, name)

		assert.Equal(t, ns, "")
		parseDocResult, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
		obj := parseDocResult.Object
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
		result, err := direct.render(t.Context(), t)
		assert.NoError(t, err)
		assert.Diff(t, configMapNames(result), map[string]bool{"cfg-a": true})
		if *testCaseInput.synthetic {
			assert.Equal(t, peerReader.unrelated.Load(), int64(1))
		}
		foreign := testutil.KustomizationID("flux-system", "app-b")
		assert.Equal(t, direct.Store().GetObject(foreign) == nil, true)
		pairedCfg := directCfg
		pairedCfg.Path = ""
		renderedTrees1, err := renderTreesForTest(t.Context(), t,
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
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
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

func testPreparedSideWorkVisits(t *testing.T, roots [2]string, sides *[2]Rendered) {
	t.Helper()

	selected := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "selected",
	}
	for side, rendered := range sides {
		want := map[string]manifest.WorkCounts{
			"scope":      {FileEntries: 6, ReaderEdges: 4},
			"root":       {Attempts: 1, Completed: 1, OwnerPrepares: 1, FileEntries: 3, ReaderEdges: 2},
			"apps/app-a": {Attempts: 1, Completed: 1, OwnerPrepares: 1, FileEntries: 2, ReaderEdges: 1},
			"apps/app-b": {Attempts: 1, Completed: 1, OwnerPrepares: 1, FileEntries: 2, ReaderEdges: 1},
		}
		got := map[string]manifest.WorkCounts{}
		for recordIndex := range rendered.Result.Work {
			record := &rendered.Result.Work[recordIndex]
			assert.Equal(t, record.Key.Tree, rendered.ID)
			recordPreparedSideVisit(record, roots[side], selected, got)
		}
		assert.Diff(t, got, want)
	}
}

type generatedInputWork struct {
	root, scenario string
	side, run      int
	full           map[manifest.NamedResource][]map[string]any
}

func assertGeneratedInputWork(t *testing.T, rendered *Rendered, state generatedInputWork) {
	t.Helper()
	producer := testutil.KustomizationID("flux-system", "producer")
	consumer := testutil.KustomizationID("flux-system", "consumer")
	entry := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "\x00",
		Name:      ".",
	}
	assert.Equal(t, rendered.entryBinding.Producer, entry)
	generatedInputWorkObservedResult := generatedInputWorkObserved(t, rendered, state.root)
	gotWork, admitted := generatedInputWorkObservedResult.work, generatedInputWorkObservedResult.admitted
	wantAdmitted := map[manifest.NamedResource]manifest.WorkCounts{
		manifest.BootstrapSourceID: {Attempts: 1, Completed: 1},
		entry:                      {Attempts: 1, Completed: 1},
	}
	if state.scenario == "target" {
		wantAdmitted[testutil.KustomizationID("flux-system", "sibling")] = manifest.WorkCounts{
			Attempts:  1,
			Completed: 1,
		}
	}
	if state.scenario != "unrelated" {
		wantAdmitted[producer] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		wantAdmitted[consumer] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		generated := manifest.NamedResource{
			Kind:      manifest.KindConfigMap,
			Namespace: "flux-system",
			Name:      "settings",
		}
		if state.side == 1 && state.scenario == "target" {
			generated.Name = "renamed-settings"
		}
		wantAdmitted[generated] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		wantAdmitted[manifest.NamedResource{
			Kind: manifest.KindConfigMap,
			Name: "result",
		}] = manifest.WorkCounts{
			Attempts:  1,
			Completed: 1,
		}
	}
	assert.Diff(t, admitted, wantAdmitted)
	acquisition := make(map[manifest.WorkPhase]manifest.WorkCounts)
	acquisition[manifest.WorkAcquisition] = manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1}
	wantWork := map[manifest.NamedResource]map[manifest.WorkPhase]manifest.WorkCounts{{}: acquisition}
	if state.scenario == "target" {
		acquisition[manifest.WorkCache] = manifest.WorkCounts{
			Attempts:     1,
			Completed:    1,
			CacheService: uint64(state.run),
		}
		entryWork := make(map[manifest.WorkPhase]manifest.WorkCounts)
		entryWork[manifest.WorkCache] = manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1}
		wantWork[entry] = entryWork
		if state.run == 0 {
			acquisition[manifest.WorkKustomize] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		}
	}
	if state.scenario != "unrelated" {
		phaseCounts3 := make(map[manifest.WorkPhase]manifest.WorkCounts)
		phaseCounts3[manifest.WorkCache] = manifest.WorkCounts{
			Attempts:  1,
			Completed: 1,
			CacheService: uint64(
				state.run,
			),
		}
		wantWork[producer] = phaseCounts3
		phaseCounts4 := make(map[manifest.WorkPhase]manifest.WorkCounts)
		phaseCounts4[manifest.WorkCache] = manifest.WorkCounts{
			Attempts:     1,
			Completed:    1,
			CacheService: uint64(state.run),
		}
		phaseCounts4[manifest.WorkSubstitution] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		wantWork[consumer] = phaseCounts4
		if state.run == 0 {
			wantWork[producer][manifest.WorkKustomize] = manifest.WorkCounts{
				Attempts:  1,
				Completed: 1,
			}
			wantWork[consumer][manifest.WorkKustomize] = manifest.WorkCounts{
				Attempts:  1,
				Completed: 1,
			}
		}
		for _, id := range []manifest.NamedResource{producer, consumer} {
			assert.Diff(t, outputByParent(rendered.Result.Manifests)[id], state.full[id])
		}
	}
	assert.Diff(t, gotWork, wantWork)
}

func recordPreparedSideVisit(
	record *manifest.WorkRecord,
	root string,
	selected manifest.NamedResource,
	got map[string]manifest.WorkCounts,
) {
	if record.Key.Phase != manifest.WorkPreparation {
		return
	}
	key := record.Key
	if key.Owner == (manifest.NamedResource{}) && key.Source == manifest.BootstrapSourceID {
		if key.Input == "" {
			got["scope"] = record.Counts
		}
		if key.Input == root {
			got["root"] = record.Counts
		}
	}
	if key.Owner == selected && key.Source == selected {
		for _, path := range []string{"apps/app-a", "apps/app-b"} {
			if strings.HasSuffix(filepath.ToSlash(key.Input), "/"+path) {
				got[path] = record.Counts
			}
		}
	}
}

type generatedInputWorkObservedResult struct {
	work     map[manifest.NamedResource]map[manifest.WorkPhase]manifest.WorkCounts
	admitted map[manifest.NamedResource]manifest.WorkCounts
}

func generatedInputWorkObserved(
	t *testing.T,
	rendered *Rendered,
	root string,
) generatedInputWorkObservedResult {
	t.Helper()
	gotWork := map[manifest.NamedResource]map[manifest.WorkPhase]manifest.WorkCounts{}
	admitted := map[manifest.NamedResource]manifest.WorkCounts{}
	for recordIndex := range rendered.Result.Work {
		record := &rendered.Result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, rendered.ID)
		assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
		switch record.Key.Phase {
		case manifest.WorkAdmission:
			counts := admitted[record.Key.Owner]
			counts.Add(&record.Counts)
			admitted[record.Key.Owner] = counts
		case manifest.WorkKustomize,
			manifest.WorkSubstitution,
			manifest.WorkAcquisition,
			manifest.WorkCache,
			manifest.WorkEvaluation,
			manifest.WorkHelm,
			manifest.WorkPostRender:
			id := record.Key.Owner
			if gotWork[id] == nil {
				gotWork[id] = make(map[manifest.WorkPhase]manifest.WorkCounts)
			}
			counts := gotWork[id][record.Key.Phase]
			counts.Add(&record.Counts)
			gotWork[id][record.Key.Phase] = counts
			if record.Key.Phase == manifest.WorkKustomize ||
				record.Key.Phase == manifest.WorkCache {
				assert.Equal(t, record.Key.Input != "", true)
			}
			if record.Key.Phase == manifest.WorkAcquisition {
				assert.Equal(t, record.Key.Input, root)
			}

		case manifest.WorkPreparation, manifest.WorkValidation, manifest.WorkIndexDecode, manifest.WorkChartDecode:
		}
	}

	return generatedInputWorkObservedResult{work: gotWork, admitted: admitted}
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
	results, err := renderTreesForTest(t.Context(), t,
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
		Config{Concurrency: 2, Selection: selection.Request{Changed: true}},
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

func generatedInputExpectedOwners(
	scenario string,
	owners []manifest.NamedResource,
	entry manifest.NamedResource,
) []manifest.NamedResource {
	if scenario == "target" {
		return append(owners, entry)
	}
	if scenario == "unrelated" {
		return nil
	}
	return owners
}
