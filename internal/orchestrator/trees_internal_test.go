package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // Orchestrator setup changes process-global source policy.
func TestConstructTrees_CancellationDuringRound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		roots := [2]string{t.TempDir(), t.TempDir()}
		for _, root := range roots {
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml, ../peer]\n")
			testutil.WriteFile(
				t,
				root,
				"flux/owner.yaml",
				testutil.KustomizationYAML("owner", "apps", "  path: ./peer\n"),
			)
			testutil.WriteFile(t, root, "peer/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "peer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n")
		}
		testutil.WriteFile(t, roots[0], "peer/kustomization.yaml", "resources: [child.yaml]\n")
		testutil.WriteFile(
			t,
			roots[0],
			"peer/child.yaml",
			testutil.KustomizationYAML("child", "apps", "  path: ./work\n"),
		)
		testutil.WriteFile(t, roots[0], "work/kustomization.yaml", "resources: []\n")
		peer := &admissionPeerFS{FS: os.DirFS(roots[1]), entered: make(chan struct{}), release: make(chan struct{})}
		r := Invocation{Config: &Config{Concurrency: 2}, byID: map[manifest.TreeID]int{}}
		for side, root := range roots {
			input := os.DirFS(root)
			if side == 1 {
				input = peer
			}
			id := manifest.TreeID{
				Cluster: manifest.ClusterID([]string{"first", "second"}[side]),
				Side:    manifest.TreeHead,
			}
			o, err := newTestOrchestrator(t, Tree{
				ID:       id,
				Path:     filepath.Join(root, "flux"),
				RepoRoot: root,
				Input:    &store.SourceArtifact{LocalPath: root, FS: input},
				ExternalChanges: detectChanges(
					t,
					[]string{"peer/cm.yaml", "peer/child.yaml"},
					[]string{"peer/cm.yaml", "peer/child.yaml"},
				),
			}, Config{Concurrency: 2, Selection: selection.Request{Changed: true}})
			assert.NoError(t, err)
			t.Cleanup(o.stop)
			assert.NoError(t, o.discover(t.Context()))
			r.byID[id] = side
			r.results = append(r.results, Rendered{ID: id, Orchestrator: o})
		}
		peer.armed.Store(true)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- r.constructTrees(ctx) }()
		<-peer.entered
		synctest.Wait()
		assert.Equal(t, r.results[0].Orchestrator != nil, true)
		assert.Equal(t, r.results[0].Orchestrator.preparation == nil, true)
		r.results[0].Orchestrator.cfg.ExternalChanges = nil
		cancel()
		close(peer.release)
		assert.Equal(t, errors.Is(<-done, context.Canceled), true)
		assert.Equal(t, r.results[0].Orchestrator != nil, true)
	})
}

type discoveryWalkFS struct {
	fs.FS

	walks atomic.Int32
}

func (f *discoveryWalkFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == "." {
		f.walks.Add(1)
	}
	return fs.ReadDir(f.FS, name)
}

func TestRenderTrees_FullDiscoveryOnce(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	input := &discoveryWalkFS{FS: fstest.MapFS{
		"object.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: kept}\n"),
		},
	}}
	results, err := renderTreesForTest(t.Context(), t, []Tree{{
		ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: root,
		Input: &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input},
	}}, Config{Concurrency: 2})
	assert.NoError(t, err)
	assert.Equal(t, len(results), 1)
	assert.Equal(t, len(results[0].Result.Manifests), 0)
	assert.Equal(t, input.walks.Load(), int32(1))
}

func TestProjectEntryReaders_PreservesSideEvidence(t *testing.T) {
	t.Parallel()
	authored := testutil.KustomizationID("apps", "owner")
	internal := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	other := testutil.KustomizationID("apps", "other")
	for _, pair := range [][2]manifest.NamedResource{{authored, internal}, {internal, authored}} {
		t.Run(pair[0].NamespacedName(), func(t *testing.T) {
			t.Parallel()
			from, to := pair[0], pair[1]
			readers := []manifest.FileReader{
				{
					Reader: from,
					Source: manifest.BootstrapSourceID,
					Declaration: manifest.NamedResource{
						Kind:      manifest.KindConfigMap,
						Namespace: "apps",
						Name:      "settings",
					},
					Exact: true,
				},
				{
					Reader: other,
					Source: manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindGitRepository,
						Namespace: "apps",
						Name:      "other",
					},
				},
			}
			physical := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "removed"}
			visible := manifest.InputVisibility{
				Source:   manifest.BootstrapSourceID,
				Root:     ".",
				Complete: true,
				Inputs:   map[manifest.NamedResource][]string{physical: {"removed.yaml"}},
			}
			empty := visible
			empty.Inputs = nil
			index := &manifest.DiscoveryIndex{
				Readers:    map[string][]manifest.FileReader{"removed.yaml": readers},
				Visibility: []manifest.InputVisibility{visible},
			}
			index.Declarations = map[manifest.NamedResource]map[string]manifest.FileDeclarations{
				manifest.BootstrapSourceID: {
					"removed.yaml": {
						Present:  true,
						Complete: true,
						Objects:  map[manifest.NamedResource]string{physical: "content"},
					},
				},
			}
			projected := projectEntryReaders(index, from, to)
			assert.Diff(t, projected.Declarations, index.Declarations)
			assert.Diff(t, index.Readers["removed.yaml"], readers)
			want := append([]manifest.FileReader{}, readers...)
			want[0].Reader = to
			assert.Diff(t, projected.Readers["removed.yaml"], want)

			changes, err := change.Qualify(
				t.Context(), detectChanges(t, []string{"removed.yaml"}, nil),
				[2]*store.Store{store.New(), store.New()}, manifest.WorkScope{},
			)
			assert.NoError(t, err)

			opposite := &manifest.DiscoveryIndex{Visibility: []manifest.InputVisibility{empty}}
			paired, err := changes.WithReaders(
				t.Context(),
				[2]*manifest.DiscoveryIndex{projected, opposite},
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			replaced := projectEntryReaders(
				index,
				from,
				other,
			)
			current, err := paired.WithReaders(
				t.Context(),
				[2]*manifest.DiscoveryIndex{replaced, opposite},
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			assert.Diff(
				t,
				maps.Collect(current.Inputs(manifest.NamedResource{})),
				map[manifest.NamedResource]change.InputChange{
					physical: {File: "removed.yaml"},
					other:    {File: "removed.yaml"},
				},
			)
			assert.Diff(
				t,
				maps.Collect(paired.Inputs(manifest.NamedResource{})),
				map[manifest.NamedResource]change.InputChange{
					physical: {File: "removed.yaml"},
					to:       {File: "removed.yaml"},
				},
			)
			assert.Diff(t, projected.Visibility, index.Visibility)
			assert.Diff(t, index.Visibility[0].Inputs[physical], []string{"removed.yaml"})
			st := store.New()
			local := &manifest.Kustomization{Name: to.Name, Namespace: to.Namespace}
			st.AddObject(local)
			root := t.TempDir()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			filter := admissiontest.Decision(
				t,
				admissiontest.Config{Changes: paired, Index: &manifest.DiscoveryIndex{}, Inventory: st, Root: root},
			)
			assert.Equal(t, filter.Reconcile(to), true)
			assert.Equal(t, filter.Reconcile(from), false)
			assert.Diff(t, changes.Paths(), []string{"removed.yaml"})
			assert.Diff(t, index.Readers["removed.yaml"], readers)
			projected.Readers["removed.yaml"][0].Exact = false
			assert.Equal(t, index.Readers["removed.yaml"][0].Exact, true)
		})
	}
}

// writeCluster lays out a two-app Flux cluster under dir: app-a renders
// ConfigMap cfg-a (data k=aVal), app-b renders ConfigMap cfg-b (data
// k=bVal). The two per-app Kustomizations are independent, so a change to
// one's rendered output must not pull the other into changed-only scope.
func writeCluster(t *testing.T, dir, aVal, bVal string) {
	t.Helper()
	ks := func(name string) string {
		return testutil.KustomizationYAML(name, "flux-system", "  path: ./apps/"+name+"\n"+
			"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
	}
	cm := func(name, val string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + "}\ndata: {k: " + val + "}\n"
	}
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks-a.yaml, ks-b.yaml]\n")
	testutil.WriteFile(t, dir, "ks-a.yaml", ks("app-a"))
	testutil.WriteFile(t, dir, "ks-b.yaml", ks("app-b"))
	testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", cm("cfg-a", aVal))
	testutil.WriteFile(t, dir, "apps/app-b/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-b/cm.yaml", cm("cfg-b", bVal))
}

// configMapNames returns the set of ConfigMap names across every parent's
// rendered manifests in a Result — what each side of the comparison
// actually rendered.
func configMapNames(res *Result) map[string]bool {
	out := map[string]bool{}
	for _, docs := range outputByParent(res.Manifests) {
		for _, m := range docs {
			if m["kind"] == "ConfigMap" {
				if meta, ok := m["metadata"].(map[string]any); ok {
					if name, ok := meta["name"].(string); ok {
						out[name] = true
					}
				}
			}
		}
	}
	return out
}

type completionFailureFS struct {
	fs.FS

	cause  error
	cancel context.CancelFunc
	armed  bool
}

func (f *completionFailureFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil || name != "outside/input.yaml" || !f.armed {
		return file, err
	}
	return &completionFailureFile{File: file, source: f}, nil
}

type completionFailureFile struct {
	fs.File

	source *completionFailureFS
}

func (f *completionFailureFile) Read([]byte) (int, error) {
	if f.source.cancel != nil {
		f.source.cancel()
	}
	return 0, f.source.cause
}

//nolint:paralleltest // Orchestrator setup changes process-global source policy.
func TestQualifyReaders_CompletionFailure(t *testing.T) {
	for _, phase := range []string{"construction", "declarations"} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/canceled_%t", phase, canceled), func(t *testing.T) {
				cancellation := "active"
				if canceled {
					cancellation = "binding"
				}
				first := testQualifyReadersCompletionFailure(t, phase, cancellation)
				if phase == "declarations" && !canceled {
					assert.Diff(t, testQualifyReadersCompletionFailure(t, phase, cancellation), first)
				}
			})
		}
	}
	t.Run("declarations/pre_canceled", func(t *testing.T) {
		testQualifyReadersCompletionFailure(t, "declarations", "before")
	})
}

func testQualifyReadersCompletionFailure(t *testing.T, phase, cancellation string) []string {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cause := errors.New("declaration bytes unavailable")
	failing := &completionFailureFS{FS: fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: []\n")},
		"outside/input.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unseen}\n"),
		},
	}, cause: cause}
	if cancellation == "binding" {
		failing.cancel = cancel
	}
	r := Invocation{Config: &Config{Concurrency: 2, RecordWork: true}, byID: map[manifest.TreeID]int{}}
	var pair [2]*store.SourceArtifact
	var owners [2]*Orchestrator
	for side := range pair {
		root := t.TempDir()
		input := fs.FS(fstest.MapFS{"entry/kustomization.yaml": {Data: []byte("resources: []\n")}})
		if side == 0 {
			input = failing
		}
		id := manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}
		o, err := newTestOrchestrator(t, Tree{
			ID: id, Path: filepath.Join(root, "entry"), RepoRoot: root,
			Input: &store.SourceArtifact{LocalPath: root, FS: input},
		}, Config{CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true, Selection: selection.Request{Changed: true}})
		assert.NoError(t, err)
		t.Cleanup(o.stop)
		assert.NoError(t, o.discover(t.Context()))
		o.store.AddWarning(
			manifest.Warning{
				Category: manifest.WarnBaselineStatusUnavailable,
				Message:  "baseline status unavailable",
			},
		)
		owners[side] = o
		pair[side], _ = o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		r.byID[id] = len(r.results)
		r.results = append(r.results, Rendered{ID: id, Orchestrator: o})
	}
	changes, err := change.Detect(t.Context(), pair[0], pair[1], change.DetectConfig{
		Candidates: map[string]bool{"outside/input.yaml": false},
	})
	assert.NoError(t, err)
	changes, err = change.Qualify(
		t.Context(),
		changes,
		[2]*store.Store{owners[0].store, owners[1].store},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for _, o := range owners {
		o.cfg.ExternalChanges = changes
	}
	if phase == "declarations" {
		for _, o := range owners {
			o.loader.Options.Changes = changes
			assert.NoError(t, o.loader.FinalizeIndex(t.Context(), o.loader.SourceRoot, true))
			o.index = o.loader.Index
		}
	}
	var healthy [2]completionHealthyPair
	if phase == "declarations" {
		healthy = prepareCompletionHealthyPairs(t, &r)
	}
	previous := owners[0].loader.Index
	assert.Equal(t, previous.Declarations[manifest.BootstrapSourceID]["outside/input.yaml"].Present, false)
	failing.armed = true
	if cancellation == "before" {
		cancel()
	}
	if phase == "construction" {
		err = r.qualifyReaders(ctx, 0)
	} else {
		err = r.qualifyDeclarations(ctx)
	}
	if cancellation != "active" {
		cause = context.Canceled
	}
	assert.Equal(t, err == nil || errors.Is(err, cause), true)
	if phase == "declarations" {
		assert.Equal(t, errors.Is(err, ctx.Err()), true)
		assert.Equal(t, errors.Is(err, context.Canceled), cancellation != "active")
	}
	assert.Equal(t, owners[0].loader.Index, previous)
	var output []string
	for _, result := range r.results[:2] {
		assert.Equal(t, errors.Is(result.Err, cause), true)
		attributed, ok := errors.AsType[*treeError](result.Err)
		assert.FatalIf(t, !ok, "completion error lost tree attribution: %v", result.Err)
		assert.Equal(t, attributed.id, result.ID)
		assert.Equal(t, attributed.attributed, true)
		assert.Equal(t, result.Orchestrator == nil, true)
		assert.Equal(t, len(result.Result.Manifests), 0)
		assert.Equal(t, len(result.Result.Warnings), 1)
		assert.Equal(t, result.Result.Warnings[0].Category, manifest.WarnBaselineStatusUnavailable)
		assert.Equal(t, owners[result.ID.Side].cfg.ExternalChanges, changes)
		assert.Equal(t, result.Result.Warnings[0].Message, "baseline status unavailable")
		message := strings.ReplaceAll(result.Err.Error(), owners[0].cfg.RepoRoot, "<root>")
		output = append(output, message)
	}
	if phase == "construction" {
		return output
	}
	for i := range healthy {
		healthy[i].assertQualification(t, cancellation != "active")
	}
	for _, result := range r.results[2:] {
		assert.NoError(t, result.Err)
		assert.Equal(t, result.Orchestrator == nil, false)
	}
	if cancellation != "active" {
		return output
	}
	assert.NoError(t, err)
	return append(output, renderCompletionHealthyPairs(ctx, t, &r)...)
}

func renderCompletionHealthyPairs(ctx context.Context, t *testing.T, r *Invocation) []string {
	t.Helper()
	r.installAdmission(ctx)
	changedReader := testutil.KustomizationID("apps", "changed-reader")
	for _, result := range r.results[2:] {
		assert.NoError(t, result.Err)
		o := result.Orchestrator
		assert.FatalIf(t, o == nil, "healthy tree stopped: %v", result.ID)
		var selected []manifest.NamedResource
		for id := range o.index.Records {
			if id.KindInfo().Render && o.selection.Snapshot().Reconcile(id) {
				selected = append(selected, id)
			}
		}
		slices.SortFunc(selected, manifest.NamedResource.Compare)
		want := []manifest.NamedResource{o.entryBinding.Producer, changedReader}
		slices.SortFunc(want, manifest.NamedResource.Compare)
		assert.Diff(t, selected, want)
	}
	r.renderTrees(ctx)
	ids := make([]manifest.TreeID, 0, len(r.results))
	for _, result := range r.results {
		ids = append(ids, result.ID)
	}
	assert.Diff(t, ids, []manifest.TreeID{
		{Cluster: ".", Side: manifest.TreeBase},
		{Cluster: ".", Side: manifest.TreeHead},
		{Cluster: "alpha", Side: manifest.TreeBase},
		{Cluster: "alpha", Side: manifest.TreeHead},
		{Cluster: "omega", Side: manifest.TreeBase},
		{Cluster: "omega", Side: manifest.TreeHead},
	})
	output := make([]string, 0, len(r.results)-2)
	for _, result := range r.results[2:] {
		assert.NoError(t, result.Err)
		assert.Diff(t, witnessWork(result.Result.Work, manifest.WorkKustomize), map[manifest.NamedResource]uint64{
			{}: 1, changedReader: 1,
		})
		assert.Diff(t, witnessWork(result.Result.Work, manifest.WorkHelm), map[manifest.NamedResource]uint64{})
		outputs := outputByParent(result.Result.Manifests)
		assert.Equal(t, len(outputs[changedReader]), 1)
		assert.Equal(t, outputs[changedReader][0]["metadata"].(map[string]any)["name"],
			any([]string{"before", "after"}[result.ID.Side]))
		assert.Equal(t, len(outputs[testutil.KustomizationID("apps", "unchanged-reader")]), 0)
		body, marshalErr := json.Marshal(result.Result.Manifests)
		assert.NoError(t, marshalErr)
		output = append(output, fmt.Sprintf("%s/%d: %s", result.ID.Cluster, result.ID.Side, body))
	}
	return output
}

type completionHealthyPair struct {
	owners  [2]*Orchestrator
	indexes [2]*manifest.DiscoveryIndex
	changes *change.Set
}

func prepareCompletionHealthyPairs(t *testing.T, r *Invocation) [2]completionHealthyPair {
	t.Helper()
	var healthy [2]completionHealthyPair
	for clusterIndex, cluster := range []manifest.ClusterID{"alpha", "omega"} {
		var inputs [2]*store.SourceArtifact
		for side := range inputs {
			root := t.TempDir()
			name := []string{"before", "after"}[side]
			url := "https://fixture.invalid/" + string(cluster)
			// Separate ConfigMaps share a file so a raw path change cannot isolate their readers.
			files := fstest.MapFS{
				"entry/kustomization.yaml": {
					Data: []byte(
						"resources: [source.yaml, changed-reader.yaml, unchanged-reader.yaml, settings.yaml]\n",
					),
				},
				"entry/source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
					"metadata: {name: self, namespace: apps}\nspec: {url: " + url + "}\n")},
				"entry/settings.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\n" +
					"metadata: {name: settings-changed, namespace: apps}\ndata: {NAME: " + name + "}\n---\n" +
					"apiVersion: v1\nkind: ConfigMap\n" +
					"metadata: {name: settings-unchanged, namespace: apps}\ndata: {NAME: unchanged}\n")},
			}
			for _, reader := range []string{"changed", "unchanged"} {
				files["entry/"+reader+"-reader.yaml"] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(
					reader+"-reader", "apps", "  path: ./"+reader+"\n  sourceRef: {kind: GitRepository, name: self}\n"+
						"  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings-"+reader+"}]}\n",
				))}
				files[reader+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
				files[reader+"/cm.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\n" +
					"metadata: {name: '${NAME}', namespace: apps}\n")}
			}
			id := manifest.TreeID{Cluster: cluster, Side: manifest.TreeSide(side)}
			o, setupErr := newTestOrchestrator(t, Tree{
				ID: id, Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url},
				Input: &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files},
			}, Config{CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true, Selection: selection.Request{Changed: true}})
			assert.NoError(t, setupErr)
			t.Cleanup(o.stop)
			assert.NoError(t, o.discover(t.Context()))
			healthy[clusterIndex].owners[side] = o
			inputs[side], _ = o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			r.byID[id] = len(r.results)
			r.results = append(r.results, Rendered{ID: id, Orchestrator: o})
		}
		comparison, setupErr := change.Detect(t.Context(), inputs[0], inputs[1], change.DetectConfig{})
		assert.NoError(t, setupErr)
		comparison, setupErr = change.Qualify(
			t.Context(),
			comparison,
			[2]*store.Store{
				healthy[clusterIndex].owners[0].store,
				healthy[clusterIndex].owners[1].store,
			},
			manifest.WorkScope{},
		)
		assert.NoError(t, setupErr)
		preparation := Invocation{
			Config:  r.Config,
			results: r.results[len(r.results)-2:],
			byID:    map[manifest.TreeID]int{},
		}
		for side, o := range healthy[clusterIndex].owners {
			o.cfg.ExternalChanges = comparison
			preparation.byID[o.cfg.ID] = side
		}
		assert.NoError(t, preparation.constructTrees(t.Context()))
		preparation.completeDiscovery(t.Context())
		healthy[clusterIndex].changes = comparison
		for side, o := range healthy[clusterIndex].owners {
			assert.NoError(t, preparation.results[side].Err)
			o.cfg.ExternalChanges = comparison
			o.loader.Options.Changes = comparison
			// Qualification MUST complete evidence independently of the prepared reader graph.
			index := *o.loader.Index
			index.Declarations = nil
			o.index, o.loader.Index = &index, &index
			o.store.SetDiscoveryIndex(&index)
			healthy[clusterIndex].indexes[side] = &index
			assert.Equal(
				t,
				o.index.Declarations[manifest.BootstrapSourceID]["entry/settings.yaml"].Complete,
				false,
			)
		}
	}
	return healthy
}

func (p *completionHealthyPair) assertQualification(t *testing.T, canceled bool) {
	t.Helper()
	for side, o := range p.owners {
		if canceled {
			assert.Equal(t, o.loader.Index, p.indexes[side])
			assert.Equal(t, o.index, p.indexes[side])
			assert.Equal(t, o.cfg.ExternalChanges, p.changes)
			continue
		}
		assert.Equal(t, o.loader.Index != p.indexes[side], true)
		assert.Equal(t, o.index, o.loader.Index)
		assert.Equal(t, o.cfg.ExternalChanges != p.changes, true)
		assert.Equal(t, o.cfg.ExternalChanges != p.owners[1-side].cfg.ExternalChanges, true)
		assert.Equal(t, o.index.Declarations[manifest.BootstrapSourceID]["entry/settings.yaml"].Complete, true)
		for _, reader := range []string{"changed", "unchanged"} {
			id := testutil.KustomizationID("apps", reader+"-reader")
			record, _ := o.index.Lookup(id)
			object := record.Declaration
			assert.FatalIf(t, object == nil, "missing reader %s", id)
			matched := 0
			for ref := range manifest.References(object, manifest.ReferenceRelations) {
				if ref.Name == "settings-"+reader {
					matched++
					assert.Equal(
						t,
						o.cfg.ExternalChanges.OutputChanged(o.cfg.ID.Side, manifest.BootstrapSourceID, ref, object),
						change.Comparison{Known: true, Changed: reader == "changed"},
					)
				}
			}
			assert.Equal(t, matched, 1)
		}
	}
}
