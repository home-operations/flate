package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/engine/baseline"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	enginesource "github.com/home-operations/flate/internal/engine/source"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/internal/testutil/testfixture"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPatchedOrderingOwners_SurvivingIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		side    manifest.TreeSide
		indexed bool
	}{
		{name: "base store", side: manifest.TreeBase},
		{name: "head store", side: manifest.TreeHead},
		{name: "base index", side: manifest.TreeBase, indexed: true},
		{name: "head index", side: manifest.TreeHead, indexed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owners := [2]*manifest.Kustomization{
				{Name: "before", Namespace: "apps", Path: "./db"},
				{Name: "after", Namespace: "apps", Path: "db"},
			}
			var trees [2]*Orchestrator
			for side, owner := range owners {
				st := store.New()
				st.AddObject(owner)
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
					FS: fstest.MapFS{"db/obj.yaml": {}},
				})
				l := loader.New(st)
				t.Cleanup(l.Close)
				trees[side] = &Orchestrator{store: st, loader: l}
			}
			before, after := owners[0].Named(), owners[1].Named()
			candidates := [2][]manifest.NamedResource{{before}, {after}}
			assert.Diff(
				t, testPairedOrderingOwners(t, trees, candidates, false), [][2]manifest.NamedResource{{before, after}},
			)
			assert.Diff(t, testPairedOrderingOwners(t,
				[2]*Orchestrator{trees[1], trees[0]}, [2][]manifest.NamedResource{{after}, {before}}, false,
			), [][2]manifest.NamedResource{{after, before}})

			survivor := *owners[1-tc.side]
			survivor.Path = "./other"
			id := survivor.Named()
			tree := trees[tc.side]
			if tc.indexed {
				tree.loader.Index.Records[id] = manifest.DiscoveryRecord{Declaration: &survivor}
			} else {
				tree.store.AddObject(&survivor)
			}
			assert.Equal(t, tree.store.GetObject(id) == nil, tc.indexed)
			assert.Equal(t, tree.loader.CatalogObject(id) != nil, true)

			same := [][2]manifest.NamedResource{{id, id}}
			assert.Diff(t, testPairedOrderingOwners(t, trees, candidates, false), same)
			assert.Diff(t, testPairedOrderingOwners(t,
				[2]*Orchestrator{trees[1], trees[0]}, [2][]manifest.NamedResource{{after}, {before}}, false,
			), same)

			other := 1 - int(tc.side)
			candidates[tc.side] = nil
			assert.Diff(t, testPairedOrderingOwners(t, trees, candidates, false), same)
			var settled [][2]manifest.NamedResource
			if !tc.indexed {
				settled = same
			}
			assert.Diff(t, testPairedOrderingOwners(t, trees, candidates, true), settled)
			candidates[tc.side], candidates[other] = []manifest.NamedResource{owners[tc.side].Named()}, nil
			assert.Equal(t, len(testPairedOrderingOwners(t, trees, candidates, false)), 0)
		})
	}
}

func TestOrderingPackageOwners_Unique(t *testing.T) {
	t.Parallel()
	first := &manifest.Kustomization{Name: "first", Namespace: "apps", Path: "./db"}
	second := &manifest.Kustomization{Name: "second", Namespace: "apps", Path: "db"}
	third := &manifest.Kustomization{Name: "third", Namespace: "apps", Path: "./other"}
	objects := make([]selection.OccurrenceFacts, 0, 5)
	for _, obj := range []*manifest.Kustomization{first, first, second, second, third} {
		objects = append(objects, selection.OccurrenceFacts{Identity: obj.Named(), Authored: obj.Named(), Object: obj})
	}
	source := manifest.BootstrapSourceID
	st := store.New()
	st.SetArtifact(source, &store.SourceArtifact{FS: fstest.MapFS{"db/obj.yaml": {}, "other/obj.yaml": {}}})
	l := loader.New(st)
	t.Cleanup(l.Close)
	tree := &Orchestrator{store: st, loader: l}
	owners, err := tree.orderingPackageOwners(t.Context(), objects)
	assert.NoError(t, err)
	assert.Diff(t, owners, map[orderingPackage]manifest.NamedResource{
		{source, "db"}:    {},
		{source, "other"}: third.Named(),
	})
}

func TestPairedOrderingOwners_PhysicalPackages(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, headPath string
		pair           bool
	}{
		{name: "different physical package", headPath: "inputs"},
		{name: "same physical package", headPath: "nested/inputs", pair: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var trees [2]*Orchestrator
			var candidates [2][]manifest.NamedResource
			for side, packagePath := range [2]string{"jump/../inputs", tc.headPath} {
				root := t.TempDir()
				testutil.WriteFile(t, root, "inputs/object.yaml", "")
				testutil.WriteFile(t, root, "nested/child/object.yaml", "")
				testutil.WriteFile(t, root, "nested/inputs/object.yaml", "")
				if side == 0 {
					assert.NoError(t, os.Symlink("nested/child", filepath.Join(root, "jump")))
				}
				owner := &manifest.Kustomization{
					Name: [2]string{"before", "after"}[side], Namespace: "apps", Path: packagePath,
				}
				st := store.New()
				st.AddObject(owner)
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{FS: os.DirFS(root)})
				l := loader.New(st)
				t.Cleanup(l.Close)
				trees[side] = &Orchestrator{store: st, loader: l}
				candidates[side] = []manifest.NamedResource{owner.Named()}
			}
			var want [][2]manifest.NamedResource
			if tc.pair {
				want = [][2]manifest.NamedResource{{candidates[0][0], candidates[1][0]}}
			}
			assert.Diff(t, testPairedOrderingOwners(t, trees, candidates, false), want)
		})
	}
}

func testPairedOrderingOwners(
	t *testing.T, trees [2]*Orchestrator, candidates [2][]manifest.NamedResource, settled bool,
) [][2]manifest.NamedResource {
	t.Helper()
	owners, err := pairedOrderingOwners(t.Context(), trees, candidates, settled)
	assert.NoError(t, err)
	return owners
}

func assertHeadOnlyOrderingWork(t *testing.T, unrelatedCharts bool) {
	t.Helper()
	fixture := testfixture.Materialize(t, "readiness-parity", "helmrelease-rename-patch-target")
	root := filepath.Join(fixture.Repo, "..", "helmrelease-rename")
	baseRef := "HEAD~1"
	if unrelatedCharts {
		release, err := os.ReadFile(filepath.Join(root, "database", "db.yaml"))
		assert.NoError(t, err)
		failing := strings.Replace(string(release), "./chart", "./failing-chart", 1)
		recipe, err := os.ReadFile(filepath.Join(root, "database", "kustomization.yaml"))
		assert.NoError(t, err)
		repo, err := git.PlainOpen(root)
		assert.NoError(t, err)

		testutil.WriteFile(t, root, "database/kustomization.yaml", "resources: [db.yaml, emitted.yaml]\n")
		testutil.WriteFile(t, root, "database/emitted.yaml", strings.Replace(failing, "name: db", "name: emitted", 1))
		testutil.WriteFile(t, root, "flux/unrelated.yaml", strings.Replace(failing, "name: db", "name: unrelated", 1))
		testutil.WriteFile(t, root, "failing-chart/Chart.yaml", "apiVersion: v2\nname: failing\nversion: 1.0.0\n")
		testutil.WriteFile(t, root, "failing-chart/templates/error.yaml", "{{ fail \"baseline chart executed\" }}\n")
		testutil.GitCommitAll(t, repo, "base with unrelated charts", &git.CommitOptions{
			Author: &object.Signature{Name: "Fixture", Email: "fixture@example.test"},
		})
		testutil.WriteFile(t, root, "database/kustomization.yaml", string(recipe))
		assert.NoError(t, os.Remove(filepath.Join(root, "database", "emitted.yaml")))
		assert.NoError(t, os.Remove(filepath.Join(root, "flux", "unrelated.yaml")))
		baseRef = "HEAD"
	}

	cache := cachetest.New(t, "", sourcecache.New)
	resolved, err := baseline.AutoResolve(t.Context(), baseline.Config{
		Network: networktest.New(t), Path: filepath.Join(root, "flux"), Base: baseRef, Cache: cache,
	})
	assert.NoError(t, err)
	runs, err := renderTreesForTest(t.Context(), t, scopeInputTrees(resolved, resolved.SelfURLs), Config{
		Cache: cache, Concurrency: 2, RecordWork: true, HeadOnly: true, Selection: selection.Request{Changed: true},
	})
	assert.FatalIf(t, len(runs) != 2, "missing pair: %v", err)
	base, head := runs[0], runs[1]
	baseWork := assertWorkOnlyBase(t, base)
	assert.Diff(t, witnessWork(baseWork, manifest.WorkKustomize), map[manifest.NamedResource]uint64{
		testutil.KustomizationID("flux-system", "database"): 1,
	})
	assert.Diff(t, witnessWork(baseWork, manifest.WorkHelm), map[manifest.NamedResource]uint64{})
	assert.Equal(t, workCounts(baseWork, manifest.NamedResource{}, manifest.WorkChartDecode).Attempts, uint64(0))

	reader, missing := testutil.HelmReleaseID("web", "web"), testutil.HelmReleaseID("web", "db")
	assert.FatalIf(t, err == nil || head.Result == nil || head.Orchestrator == nil,
		"missing partial head failure: %v", err)
	assert.Diff(t, head.Result.Failed[reader].BlockedBy(), []manifest.NamedResource{missing})
	assert.Diff(t, witnessWork(head.Result.Work, manifest.WorkHelm),
		map[manifest.NamedResource]uint64{testutil.HelmReleaseID("web", "database"): 1})
}

func TestRenderTrees_CanceledBaseOrderingComparison(t *testing.T) {
	t.Parallel()
	fixture := testfixture.Materialize(t, "readiness-parity", "helmrelease-required-base-failure")
	root := filepath.Join(fixture.Repo, "..", "helmrelease-rename", "flux")
	cache := cachetest.New(t, "", sourcecache.New)
	resolved, err := baseline.AutoResolve(t.Context(), baseline.Config{
		Network: networktest.New(t), Path: root, Base: "HEAD~1", Cache: cache,
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	producer := testutil.KustomizationID("flux-system", "database")
	runs, err := renderTreesForTest(ctx, t, scopeInputTrees(resolved, resolved.SelfURLs), Config{
		Cache: cache, Concurrency: 2, HeadOnly: true,
		StatusListener: func(tree manifest.TreeID, resource manifest.DiagnosticIdentity, status store.StatusInfo) {
			if tree.Side == manifest.TreeBase && resource.ID == producer && status.Outcome() == store.OutcomeFailed {
				cancel()
			}
		},
	})
	assert.FatalIf(t, len(runs) != 2, "missing pair: %v", err)
	base, head := runs[0], runs[1]
	assert.FatalIf(t, !errors.Is(base.Err, context.Canceled), "base lost scheduler cancellation: %v", base.Err)
	const patchPath = "/metadata/noSuchParent/name"
	if !strings.Contains(base.Err.Error(), patchPath) {
		t.Errorf("base lost the producer failure: %v", base.Err)
	}
	if head.Err != nil && strings.Contains(head.Err.Error(), "compare base ordering outputs:") {
		t.Errorf("head received a base comparison error: %v", head.Err)
	}
}

func TestQualifyOrdering_CanceledRoot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		qualify func(*Invocation, context.Context) error
	}{
		{name: "readers", qualify: func(r *Invocation, ctx context.Context) error { return r.qualifyReaders(ctx, 0) }},
		{name: "declarations", qualify: (*Invocation).qualifyDeclarations},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := discoverRelativeOrderingPair(ctx, t, "ordering-relative-unrelated")
			base := r.results[0].Orchestrator
			var err error
			base.selection, err = selection.New(
				base.store,
				&cancelOrderingRoot{Loader: base.loader, cancel: cancel},
				base.cfg.Selection,
			)
			assert.NoError(t, err)

			err = tc.qualify(r, ctx)
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			for i := range r.results {
				assert.Equal(t, errors.Is(r.results[i].Err, context.Canceled), true)
				assert.Equal(t, r.results[i].Orchestrator == nil, true)
			}
		})
	}
}

func TestOrderingOwners_PreservesExecutionPreparation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	r := discoverRelativeOrderingPair(ctx, t, "ordering-relative-component")
	assert.NoError(t, r.qualifyReaders(ctx, 0))
	base, head := r.results[0].Orchestrator, r.results[1].Orchestrator
	var reads store.ReadSet
	reads.Init(head.store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	reads.RequireData(testutil.KustomizationID("flux-system", "database"))
	request := selection.PrepareRequest{
		Changes: head.cfg.ExternalChanges, Preparation: &head.admissionPreparation, Reads: &reads,
	}
	result, accepted, err := head.selection.Admit(ctx, selection.AdmissionOptions{
		Request: request, Stage: selection.Execution,
	})
	assert.NoError(t, err)
	assert.Equal(t, accepted, true)
	assert.FatalIf(t, len(result.Needed) == 0, "execution admission retained no demands")
	preparation := head.admissionPreparation
	before, err := head.selection.Prepare(ctx, request)
	assert.NoError(t, err)
	assert.FatalIf(t, len(before.Needed) == 0 || &before.Needed[0] != &result.Needed[0],
		"execution admission did not reuse the retained result")
	before.Retire()

	assert.NoError(t, r.prepareOrderingComparison(ctx, base, head))
	assert.Equal(t, len(base.orderingPairs) != 0, true)
	assert.Equal(t, head.admissionPreparation == preparation, true)
	after, err := head.selection.Prepare(ctx, request)
	assert.NoError(t, err)
	defer after.Retire()
	assert.FatalIf(t, len(after.Needed) == 0 || &after.Needed[0] != &result.Needed[0],
		"ordering comparison replaced the retained execution result")
}

func discoverRelativeOrderingPair(ctx context.Context, t *testing.T, fixtureName string) *Invocation {
	t.Helper()
	fixture := testfixture.Materialize(t, "readiness-parity", fixtureName)
	cache := cachetest.New(t, "", sourcecache.New)
	network := networktest.New(t)
	resolved, err := baseline.AutoResolve(ctx, baseline.Config{
		Network: network, Path: filepath.Join(fixture.Repo, "flux"), Base: "HEAD~1", Cache: cache,
	})
	assert.NoError(t, err)
	r, err := NewInvocation(ctx, Config{
		Cache: cache, Network: network, Concurrency: 2, HeadOnly: true, Selection: selection.Request{Changed: true},
	})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	inputs := enginesource.NewHTTPInputs(r.Config.Cache, r.Config.Network, r.budget)
	t.Cleanup(inputs.Close)
	r.trees, err = r.prepareTrees(ctx, scopeInputTrees(resolved, resolved.SelfURLs), &inputs)
	assert.NoError(t, err)
	r.results = make([]Rendered, len(r.trees))
	r.stores = make([]*store.Store, len(r.trees))
	r.byID = make(map[manifest.TreeID]int, len(r.trees))
	t.Cleanup(r.stopRemaining)
	r.discoverTrees(ctx)
	r.Config.projections.Clear()
	assert.NoError(t, r.stopFailedDiscovery())
	return r
}

type cancelOrderingRoot struct {
	*loader.Loader

	cancel context.CancelFunc
}

//nolint:gocritic // The fixture implements the census result signature.
func (f *cancelOrderingRoot) ResolveRoot(
	ctx context.Context, reads *store.ReadSet, source manifest.NamedResource, root string,
) (string, bool, error) {
	f.cancel()
	return f.Loader.ResolveRoot(ctx, reads, source, root)
}

func TestRenderTrees_PairedOrderingTargetTransfer(t *testing.T) {
	t.Parallel()
	const selfURL = "https://example.test/readiness.git"
	const sourceRef = "  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"
	const release = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: db, namespace: web}
spec:
  interval: 1m
  chart:
    spec:
      chart: ./chart
      sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`
	files := map[string]string{
		"entry/source.yaml": `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 1m
  url: ` + selfURL + `
  ref: {branch: main}
`,
		"entry/database.yaml":  testutil.KustomizationYAML("database", "apps", "  path: ./database\n"+sourceRef),
		"entry/alternate.yaml": testutil.KustomizationYAML("alternate", "apps", "  path: ./alternate\n"+sourceRef),
		"entry/web.yaml":       testutil.KustomizationYAML("web", "apps", "  path: ./web\n"+sourceRef),
		"database/kustomization.yaml": `resources: [release.yaml, cm.yaml]
patches:
  - target: {kind: HelmRelease, name: candidate}
    patch: |-
      - {op: replace, path: /metadata/name, value: db}
`,
		"database/release.yaml":        strings.Replace(release, "name: db", "name: candidate", 1),
		"database/cm.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: config}\n",
		"alternate/kustomization.yaml": "resources: [release.yaml]\n",
		"alternate/release.yaml":       release,
		"web/kustomization.yaml": `resources: [release.yaml]
patches:
  - target: {kind: HelmRelease, name: web}
    patch: |-
      - {op: add, path: /spec/dependsOn, value: [{name: db}]}
`,
		"web/release.yaml":        strings.Replace(release, "name: db", "name: web", 1),
		"chart/Chart.yaml":        "apiVersion: v2\nname: readiness\nversion: 1.0.0\n",
		"chart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: {{ .Release.Name }}}\n",
	}
	const packageFile = "database/kustomization.yaml"
	packages := [2]string{files[packageFile], "resources: [cm.yaml]\n"}
	for _, tc := range []struct {
		name     string
		affected manifest.TreeSide
		headOnly bool
	}{
		{name: "forward", affected: manifest.TreeHead},
		{name: "reverse", affected: manifest.TreeBase},
		{name: "head-only", affected: manifest.TreeHead, headOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := writeWitness(t, files)
			testutil.WriteFile(t, root, packageFile, packages[1-tc.affected])
			repo := testutil.GitInit(t, root)
			_, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{selfURL}})
			assert.NoError(t, err)
			testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{
				Author: &object.Signature{Name: "Fixture", Email: "fixture@example.test"},
			})
			testutil.WriteFile(t, root, packageFile, packages[tc.affected])
			cache := cachetest.New(t, "", sourcecache.New)
			resolved, err := baseline.AutoResolve(t.Context(), baseline.Config{
				Network: networktest.New(t), Path: filepath.Join(root, "entry"), Base: "HEAD", Cache: cache,
			})
			assert.NoError(t, err)
			runs, err := renderTreesForTest(t.Context(), t, scopeInputTrees(resolved, resolved.SelfURLs), Config{
				Cache: cache, Concurrency: 2, RecordWork: true, HeadOnly: tc.headOnly,
				Selection: selection.Request{Changed: true},
			})
			assert.Equal(t, len(runs), 2)
			target := testutil.HelmReleaseID("web", "db")
			owner := testutil.KustomizationID("apps", "database")
			alternate := testutil.KustomizationID("apps", "alternate")
			for _, run := range runs {
				side := run.ID.Side
				if tc.headOnly && side == manifest.TreeBase {
					work := assertWorkOnlyBase(t, run)
					assert.Diff(
						t,
						witnessWork(work, manifest.WorkKustomize),
						map[manifest.NamedResource]uint64{owner: 1},
					)
					assert.Equal(t, workCounts(work, manifest.NamedResource{}, manifest.WorkHelm).Attempts, uint64(0))
					continue
				}
				assert.FatalIf(t, run.Orchestrator == nil || run.Result == nil, "missing result: %v", err)
				work := witnessWork(run.Result.Work, manifest.WorkKustomize)
				assert.Equal(t, work[owner], uint64(1))
				assert.Equal(t, work[testutil.KustomizationID("apps", "web")], uint64(0))
				assert.Equal(t, len(run.Result.Failed), 0)
				inputFor := func(source manifest.NamedResource) *store.SourceArtifact {
					input, _ := run.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
					return input
				}
				assert.Equal(t, run.cfg.ExternalChanges.EffectiveDeclarationChanged(side, target, inputFor), false)
				if side == tc.affected {
					record, _ := run.index.Lookup(target)
					assert.Equal(t, record.File, "alternate/release.yaml")
					assert.Equal(t, record.Parent, alternate)
					assert.Equal(t, run.store.GetDeclaration(target), nil)
					assert.Equal(t, work[alternate], uint64(0))
				}
			}
			assert.NoError(t, err)
		})
	}
}
