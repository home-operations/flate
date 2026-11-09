package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	controllerbase "github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_UnchangedNamedSelection(t *testing.T) {
	t.Parallel()
	roots := unchangedNamedSelectionRoots(t)
	runs, err := renderTreesForTest(t.Context(), t, []Tree{
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Path: roots[0]},
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: roots[1]},
	}, Config{Concurrency: 2})
	assert.NoError(t, err)
	for _, id := range []manifest.NamedResource{
		testutil.KustomizationID("flux-system", "app-a"), testutil.HelmReleaseID("flux-system", "release"),
	} {
		t.Run(id.Kind, func(t *testing.T) {
			t.Parallel()
			projectedResults0, projectionErrors0 := Select(runs, err, []manifest.NamedResource{id}, nil)
			selected, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
			assert.NoError(t, selectedErr)
			assert.NoError(t, remainder)
			assert.Equal(t, len(selected), 2)
			for side, result := range selected {
				_, known := runs[side].index.Lookup(id)
				assert.Equal(t, known, true)
				assert.Equal(t, runs[side].store.GetObject(id) == nil, true)
				assert.Equal(t, result.Counts[id.Kind], 1)
				assert.Equal(t, result.Statuses[id].Outcome(), store.OutcomeUnchanged)
				if len(result.Objects) != 1 {
					t.Fatalf("side %d: unchanged named declaration missing: %+v", side, result)
				}
				assert.Equal(t, result.Objects[0].Named(), id)
				assert.Equal(t, manifest.KindOf(result.Objects[0]), id.KindInfo())
				switch obj := result.Objects[0].(type) {
				case *manifest.Kustomization:
					assert.Equal(t, obj.Path, "./apps/app-a")
					assert.Equal(
						t,
						obj.SourceID(),
						manifest.NamedResource{
							Group:     manifest.SourceDomain,
							Kind:      manifest.KindGitRepository,
							Namespace: "flux-system",
							Name:      "flux-system",
						},
					)
					assert.Equal(t, obj.Labels["team"], "platform")
					assert.Equal(t, obj.Prune && obj.Wait && obj.Suspend, true)
					assert.Equal(t, obj.TargetNamespace, "workloads")
				case *manifest.HelmRelease:
					assert.Equal(t, obj.Chart.Name, "demo")
					assert.Equal(t, obj.Chart.Version, "2.3.4")
					assert.Equal(t, obj.ChartVersion(), "2.3.4")
					assert.Equal(
						t,
						obj.ChartSource(),
						manifest.NamedResource{
							Group: manifest.SourceDomain, Kind: manifest.KindHelmChart,
							Namespace: "flux-system", Name: "flux-system-release",
						},
					)
					assert.Equal(t, obj.ReleaseName(), "chosen-name")
					assert.Equal(t, obj.TargetNamespace, "workloads")
					assert.Equal(t, obj.Suspend, true)
					assert.Equal(t, obj.Labels["team"], "platform")
					assert.Equal(t, obj.Values == nil, true)
				default:
					t.Fatalf("catalog returned another manifest kind: %T", obj)
				}
				assert.Equal(t, len(result.Manifests), 0)
			}
			projectedResults1, projectionErrors1 := Select(
				runs,
				err,
				[]manifest.NamedResource{id},
				map[string]string{"team": "other"},
			)
			filtered, selectedErr, remainder := projectedResults1, projectionErrors1.Selected, projectionErrors1.Remaining
			assert.NoError(t, selectedErr)
			assert.NoError(t, remainder)
			for _, result := range filtered {
				assert.Equal(t, result.Counts[id.Kind], 1)
				assert.Equal(t, len(result.Objects), 0)
			}
			full, fullErr := renderTreesForTest(t.Context(), t,
				[]Tree{{Path: roots[0], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
				Config{Concurrency: 2, Selection: selection.Request{Roots: []manifest.NamedResource{id}}},
			)
			assert.NoError(t, fullErr)
			projectedResults2, projectionErrors2 := Select(full, fullErr, []manifest.NamedResource{id}, nil)
			objects, selectedErr, remainder := projectedResults2, projectionErrors2.Selected, projectionErrors2.Remaining
			assert.NoError(t, selectedErr)
			assert.NoError(t, remainder)
			assert.Equal(t, len(objects[0].Objects), 1)
			assert.Equal(t, full[0].store.GetObject(id) != nil, true)
			switch obj := objects[0].Objects[0].(type) {
			case *manifest.Kustomization:
				assert.Equal(t, obj.Interval.Duration, time.Hour)
			case *manifest.HelmRelease:
				assert.Equal(t, obj.Values["payload"], any("must-not-be-retained"))
			}
		})
	}
}

func TestRenderTrees_SelectedSelfSourceReproducer(t *testing.T) {
	t.Parallel()
	fixture := newSelectedSelfFixture(t)
	fixture.assertBootstrap(t)
	cfg, root, source := fixture.cfg, fixture.root, fixture.source
	const url = "git://fixture.invalid/cluster"
	for _, cold := range []bool{false, true} {
		for run := range 2 {
			runCfg := cfg.Config
			if cold {
				runCfg.Cache = cachetest.New(t, "", sourcecache.New)
			}
			renderedTrees1, err := renderTreesForTest(t.Context(), t, []Tree{
				{
					RepoRoot: cfg.BaseInput.LocalPath, SelfURLs: []string{url},
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					ExternalChanges: cfg.ExternalChanges, Input: cfg.BaseInput, BaseInput: cfg.Input,
				},
				{
					RepoRoot: root, SelfURLs: []string{url},
					ID:    manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges,
				},
			}, runCfg)
			base, head := renderedTrees1[0], renderedTrees1[1]
			assert.NoError(t, err)
			for side, rendered := range []Rendered{base, head} {
				fixture.assertWork(t, rendered, side, cold)
				assert.Equal(t, len(selectionNamesForTest(rendered.selection, rendered.store, rendered.index)), 0)
				assert.Equal(t, len(rendered.Result.Manifests), 0)
				assert.Equal(t, rendered.selection.Snapshot().Reconcile(rendered.entryBinding.Producer), false)
				delta, err := change.Detect(
					t.Context(),
					base.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact),
					head.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact),
					manifest.WorkScope{},
				)
				assert.NoError(t, err)
				assert.Diff(t, delta.Paths(), []string(nil))
				t.Logf("paired run=%d side=%d unchanged root and source payload excluded", run, side)
			}
		}
	}
}

func TestRenderTrees_LocalAliasSelfProducer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	const url = "git://fixture.invalid/cluster"
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [source.yaml, root.yaml, apps.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n"+
			"metadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+"}\n",
	)
	for _, name := range []string{"root", "apps"} {
		path := "./apps"
		if name == "root" {
			path = "./flux"
		}
		testutil.WriteFile(
			t,
			root,
			"flux/"+name+".yaml",
			testutil.KustomizationYAML(
				name,
				"flux-system",
				"  path: "+path+"\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: before}\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"before",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: after}\n",
	)
	cache := cachetest.New(t, "", sourcecache.New)
	selected, err := baseline.AutoResolve(t.Context(), root, "HEAD", cache)
	assert.NoError(t, err)
	trees := make([]Tree, 0, 2)
	for side, input := range []*store.SourceArtifact{selected.BaseInput, selected.Source} {
		trees = append(
			trees,
			Tree{
				ID: manifest.TreeID{
					Cluster: ".",
					Side:    manifest.TreeSide(side),
				},
				Path:            filepath.Join(input.LocalPath, "flux"),
				RepoRoot:        input.LocalPath,
				Input:           input,
				SelfURLs:        []string{url},
				ExternalChanges: selected.Changes,
			},
		)
	}
	rendered, err := renderTreesForTest(t.Context(), t, trees, Config{Cache: cache, Concurrency: 2})
	assert.NoError(t, err)
	for side, result := range rendered {
		value := "before"
		if side == 1 {
			value = "after"
		}
		output, err := json.Marshal(result.Result.Manifests)
		assert.NoError(t, err)
		if !strings.Contains(string(output), "\"value\":\""+value+"\"") {
			t.Fatalf("side %d lost selected output: %s", side, output)
		}
		source := (&manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}).Named()
		input, _ := result.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		if input == nil || !input.InputAlias || input.LocalPath == trees[side].Input.LocalPath {
			t.Fatalf("side %d did not exercise a distinct admitted alias: %+v", side, input)
		}
	}
}

type selectedSelfFixture struct {
	root       string
	source     manifest.NamedResource
	entry      manifest.NamedResource
	admissions map[manifest.NamedResource]uint64
	want       []string
	cfg        treeConfig
}

func (f *selectedSelfFixture) assertWork(t *testing.T, rendered Rendered, side int, cold bool) {
	t.Helper()
	source, entry, admissions := f.source, f.entry, f.admissions
	assert.Equal(t, rendered.entryBinding.Producer, entry)
	assert.Diff(t, witnessWork(rendered.Result.Work, manifest.WorkAdmission), admissions)
	assert.Equal(
		t,
		workCounts(rendered.Result.Work, manifest.NamedResource{}, manifest.WorkAdmission),
		manifest.WorkCounts{Attempts: 3, Completed: 3},
	)
	for _, phase := range []manifest.WorkPhase{
		manifest.WorkHelm,
		manifest.WorkPostRender,
		manifest.WorkSubstitution,
		manifest.WorkEvaluation,
		manifest.WorkIndexDecode,
		manifest.WorkChartDecode,
	} {
		assert.Diff(t, witnessWork(rendered.Result.Work, phase), map[manifest.NamedResource]uint64{})
	}
	assert.Diff(t, witnessWork(rendered.Result.Work, manifest.WorkKustomize), map[manifest.NamedResource]uint64{})
	assert.Diff(
		t,
		witnessWork(rendered.Result.Work, manifest.WorkAcquisition),
		map[manifest.NamedResource]uint64{source: 2},
	)
	assert.Equal(
		t,
		workCounts(rendered.Result.Work, source, manifest.WorkAcquisition),
		manifest.WorkCounts{Attempts: 2, Completed: 2, LocalAcquisitions: 2},
	)
	assert.Diff(
		t,
		witnessWork(rendered.Result.Work, manifest.WorkCache),
		map[manifest.NamedResource]uint64{},
	)
	assert.Diff(t, witnessWork(rendered.Result.Work, manifest.WorkValidation), map[manifest.NamedResource]uint64{})
	assert.Equal(
		t,
		workCounts(rendered.Result.Work, manifest.NamedResource{}, manifest.WorkCache),
		manifest.WorkCounts{},
	)
	assert.Equal(
		t,
		workCounts(rendered.Result.Work, manifest.NamedResource{}, manifest.WorkValidation),
		manifest.WorkCounts{},
	)
	preparation := f.assertRecords(t, rendered, side, cold)
	f.assertPreparation(t, preparation, side)
}

func newSelectedSelfFixture(t *testing.T) selectedSelfFixture {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	const url = "git://fixture.invalid/cluster"
	source := (&manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}).Named()
	entry := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "\x00",
		Name:      ".",
	}
	admissions := map[manifest.NamedResource]uint64{source: 1, manifest.BootstrapSourceID: 1, entry: 1}
	want := make([]string, 0, 147)
	want = append(
		want,
		source.String(),
		(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "selected"}).String(),
	)
	var resources strings.Builder
	resources.WriteString("resources:\n- source.yaml\n")
	testutil.WriteFile(
		t,
		root,
		"source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n"+
			"metadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+", ref: {name: refs/heads/selected}}\n",
	)
	for i := range 145 {
		name := fmt.Sprintf("app-%03d", i)
		id := testutil.KustomizationID("flux-system", name)
		want = append(want, id.String())
		fmt.Fprintf(&resources, "- %s.yaml\n", name)
		testutil.WriteFile(
			t,
			root,
			name+".yaml",
			testutil.KustomizationYAML(
				name,
				id.Namespace,
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "kustomization.yaml", resources.String())
	slices.Sort(want)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: before}\n",
	)
	baseHash := testutil.GitCommitAll(
		t,
		repo,
		"before",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected}\ndata: {value: after}\n",
	)
	headHash := testutil.GitCommitAll(
		t,
		repo,
		"after",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	assert.NoError(
		t,
		repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("selected"), headHash)),
	)
	cache := cachetest.New(t, "", sourcecache.New)
	selected, err := baseline.AutoResolve(t.Context(), root, baseHash.String(), cache)
	assert.NoError(t, err)

	cfg := treeConfig{
		RecordWork:      true,
		Cache:           cache,
		Concurrency:     2,
		Input:           selected.Source,
		BaseInput:       selected.BaseInput,
		ExternalChanges: selected.Changes,
		SelfURLs:        []string{url},
	}
	return selectedSelfFixture{
		root: root, source: source, entry: entry, admissions: admissions,
		want: want, cfg: cfg,
	}
}

func unchangedNamedSelectionRoots(t *testing.T) [2]string {
	t.Helper()
	roots := [2]string{t.TempDir(), t.TempDir()}
	for _, root := range roots {
		writeCluster(t, root, "same", "same")
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [ks-a.yaml, ks-b.yaml, hr.yaml]\n")
		testutil.WriteFile(t, root, "ks-a.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app-a, namespace: flux-system, labels: {team: platform}}
spec:
  interval: 1h
  path: ./apps/app-a
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  targetNamespace: workloads
  prune: true
  wait: true
  suspend: true
`)
		testutil.WriteFile(t, root, "hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: flux-system, labels: {team: platform}}
spec:
  interval: 1h
  releaseName: chosen-name
  targetNamespace: workloads
  suspend: true
  chart:
    spec:
      chart: demo
      version: 2.3.4
      sourceRef: {kind: HelmRepository, name: charts}
  values: {payload: must-not-be-retained}
`)
	}
	return roots
}

func (f *selectedSelfFixture) assertPreparation(
	t *testing.T,
	preparation map[manifest.WorkKey]manifest.WorkCounts,
	side int,
) {
	t.Helper()
	source := f.source
	bootstrapKey := manifest.WorkKey{
		Owner:  manifest.BootstrapSourceID,
		Source: manifest.BootstrapSourceID,
		Phase:  manifest.WorkPreparation,
	}
	assert.Equal(t, preparation[bootstrapKey], manifest.WorkCounts{})
	files, readers := uint64(2066), uint64(2)
	sourcePrepares, sourceEntries, sourceReaders := uint64(2), uint64(6), uint64(3)
	rootReaders, rootEntries := uint64(438), uint64(2503)
	if side == 1 {
		files, readers, rootReaders, rootEntries = 884, 1, 728, 1621
		sourcePrepares, sourceEntries, sourceReaders = 1, 4, 2
	}
	assertWitnessPreparations(
		t,
		preparation,
		map[manifest.WorkKey]manifest.WorkCounts{
			{Source: source, Phase: manifest.WorkPreparation}: {FileEntries: sourceEntries, ReaderEdges: sourceReaders},
			{Source: manifest.BootstrapSourceID, Phase: manifest.WorkPreparation}: {
				Attempts:       3,
				Completed:      3,
				FileEntries:    rootEntries,
				ReaderEdges:    rootReaders,
				OwnerPrepares:  3,
				RawKeyPrepares: 0,
			},
			{Owner: source, Source: source, Phase: manifest.WorkPreparation}: {
				Attempts:      sourcePrepares,
				Completed:     sourcePrepares,
				FileEntries:   files,
				ReaderEdges:   readers,
				OwnerPrepares: sourcePrepares,
			},
		},
	)
}

func (f *selectedSelfFixture) assertRecords(
	t *testing.T,
	rendered Rendered,
	side int,
	cold bool,
) map[manifest.WorkKey]manifest.WorkCounts {
	t.Helper()
	source := f.source
	preparation := map[manifest.WorkKey]manifest.WorkCounts{}
	var cacheInput string
	for recordIndex := range rendered.Result.Work {
		record := &rendered.Result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)})
		assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
		if record.Key.Phase == manifest.WorkAcquisition {
			assert.Equal(t, record.Key.Owner, source)
			assert.Equal(t, record.Key.Source, source)
			assert.Equal(t, record.Key.Input, "")
		} else if record.Key.Phase != manifest.WorkPreparation {
			assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
		}
		if record.Key.Phase == manifest.WorkPreparation {
			key := record.Key
			key.Input, key.Tree = "", manifest.TreeID{}
			counts := preparation[key]
			counts.Add(&record.Counts)
			preparation[key] = counts
		}
		if record.Key.Phase == manifest.WorkCache {
			want := manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1}
			if cold && record.Key.Owner == (manifest.NamedResource{}) {
				want.CacheService = 0
			}
			assert.Equal(t, record.Counts, want)
			assert.Equal(t, record.Key.Input != "", true)
			if cacheInput == "" {
				cacheInput = record.Key.Input
			}
			assert.Equal(t, record.Key.Input, cacheInput)
		}
	}
	return preparation
}

func (f *selectedSelfFixture) assertBootstrap(t *testing.T) {
	t.Helper()
	cfg, want := f.cfg, f.want
	for side := range 2 {
		one := cfg
		if side == 0 {
			one.Input, one.BaseInput = cfg.BaseInput, cfg.Input
		}
		one.Path = one.Input.LocalPath
		one.ID = manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}
		o, err := newTestOrchestrator(t, one.Tree, one.Config)
		assert.NoError(t, err)
		assert.NoError(t, o.bootstrap(t.Context()))
		var execution *controllerbase.Execution
		o.tasks.Go(t.Context(), "bootstrap-proof", func(ctx context.Context) {
			execution = o.src.ReconcileNode(ctx, manifest.BootstrapSourceID, manifest.DrainNone, schedule.Result{})
		})
		o.tasks.BlockTillDone()
		assert.Equal(
			t,
			workCounts(o.freezeWork(), manifest.BootstrapSourceID, manifest.WorkPreparation),
			manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1},
		)
		execution.Close()
		if side == 0 {
			assert.Equal(t, len(selectionNamesForTest(o.selection, o.store, o.index)), 0)
		} else {
			assert.Diff(t, selectionNamesForTest(o.selection, o.store, o.index), want)
		}
		t.Logf("current-only side=%d exact keep=%v", side, selectionNamesForTest(o.selection, o.store, o.index))
		o.stop()
	}
}
