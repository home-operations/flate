package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestClusterAdvisories_ContributorsAndCarrier(t *testing.T) {
	t.Parallel()
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
	newStore := func(overrides []store.SuppliedOverride) *store.Store {
		t.Helper()
		st := store.New()
		owner := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
		st.AddObject(owner)
		reads := new(store.ReadSet)
		reads.Init(st, owner.Named(), nil)
		reads.GetObject(owner.Named())
		for _, override := range overrides {
			reads.RecordSuppliedOverride(override)
		}
		reads.RecordLocalSSHAgent(source)
		reads.Accept()
		reads.Close()
		for range st.Sweep() {
		}
		return st
	}
	shared := store.SuppliedOverride{Resource: settings, SupplyPath: "first.yaml", Keys: []string{"Z", "A"}}
	stores := []*store.Store{
		newStore([]store.SuppliedOverride{shared}),
		newStore([]store.SuppliedOverride{shared, {Resource: secret, SupplyPath: "second.yaml", Keys: []string{"TOKEN"}}}),
		newStore([]store.SuppliedOverride{{Resource: settings, SupplyPath: "omega.yaml", Keys: []string{"OMEGA"}}}),
		nil,
	}
	failure := errors.New("head failed")
	runs := []Rendered{
		{ID: manifest.TreeID{Cluster: "alpha", Side: manifest.TreeBase}, Result: &Result{}},
		{ID: manifest.TreeID{Cluster: "alpha", Side: manifest.TreeHead}, Err: failure},
		{ID: manifest.TreeID{Cluster: "omega", Side: manifest.TreeBase}, Result: &Result{}},
		{ID: manifest.TreeID{Cluster: "omega", Side: manifest.TreeHead}, Absent: true, Result: &Result{}},
	}
	want := []manifest.Warning{
		{Category: manifest.WarnLocalSSHAgent, Message: "authenticated with the local SSH agent", Detail: []string{source.String()}, Count: 1},
		{Category: manifest.WarnSuppliedPrecedence, Message: "repository data takes precedence over supplied data", Detail: []string{
			settings.String() + ": A (first.yaml)", settings.String() + ": Z (first.yaml)", secret.String() + ": TOKEN (second.yaml)",
		}, Count: 1},
	}
	for range 2 {
		clusterAdvisories(runs, stores)
		assert.Diff(t, runs[1].Result.ClusterWarnings, want)
		assert.Equal(t, len(runs[0].Result.ClusterWarnings)+len(runs[3].Result.ClusterWarnings), 0)
		assert.Equal(t, runs[1].Orchestrator == nil && errors.Is(runs[1].Err, failure), true)
		omega := runs[2].Result.ClusterWarnings
		if len(omega) != 2 {
			t.Fatalf("base-only cluster lost advisories: %v", omega)
		}
		assert.Diff(t, omega[1].Detail, []string{settings.String() + ": OMEGA (omega.yaml)"})
		for _, run := range runs {
			assert.Equal(t, len(run.Result.Warnings), 0)
		}
	}
}

func TestRenderTrees_ClusterAdvisories(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"head", "paired", "head_only", "base", "two_clusters", "head_failed", "base_failed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Concurrency: 2, CacheDir: t.TempDir(), HeadOnly: scenario == "head_only", Supply: []store.SuppliedObject{suppliedSubstitution("settings")}}
			clusters := []manifest.ClusterID{"alpha"}
			if scenario == "two_clusters" {
				clusters = append(clusters, "omega")
			}
			var trees []Tree
			for _, cluster := range clusters {
				for _, side := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
					if scenario == "head" && side == manifest.TreeBase || scenario == "base" && side == manifest.TreeHead {
						continue
					}
					root := t.TempDir()
					writeSubstitutionCluster(t, root, "settings", fmt.Sprintf("${VALUE}-%d", side))
					testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, settings.yaml]\n")
					testutil.WriteFile(t, root, "settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n")
					failed := scenario == "head_failed" && side == manifest.TreeHead || scenario == "base_failed" && side == manifest.TreeBase
					if failed {
						testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: [\n")
					}
					trees = append(trees, Tree{ID: manifest.TreeID{Cluster: cluster, Side: side}, RepoRoot: root})
				}
			}
			runs, err := RenderTrees(t.Context(), trees, cfg)
			failed := scenario == "head_failed" || scenario == "base_failed"
			if (err != nil) != failed || failed && !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("render failure=%v, want failed=%t", err, failed)
			}
			carriers := 0
			for _, run := range runs {
				if run.Result == nil {
					continue
				}
				for _, warning := range run.Result.Warnings {
					if warning.Category == manifest.WarnSuppliedPrecedence || warning.Category == manifest.WarnLocalSSHAgent {
						t.Fatal("cluster advisory escaped through side warnings")
					}
				}
				if len(run.Result.ClusterWarnings) == 0 {
					continue
				}
				carriers++
				wantSide := manifest.TreeHead
				if scenario == "base" {
					wantSide = manifest.TreeBase
				}
				assert.Equal(t, run.ID.Side, wantSide)
				assert.Diff(t, run.Result.ClusterWarnings, []manifest.Warning{{Category: manifest.WarnSuppliedPrecedence,
					Message: "repository data takes precedence over supplied data", Detail: []string{"ConfigMap/apps/settings: VALUE (ci-values.yaml)"}, Count: 1}})
			}
			assert.Equal(t, carriers, len(clusters))
		})
	}
}

func TestRenderTrees_ClusterAdvisoriesWarmAcquisition(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "settings", "${VALUE}")
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, source.yaml, settings.yaml]\n")
	testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", "  sourceRef: {kind: GitRepository, name: remote, namespace: apps}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: settings}]\n"))
	testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: apps}\nspec: {url: 'https://fixture.invalid/source', interval: 1h}\n")
	testutil.WriteFile(t, root, "settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n")
	var runs []Rendered
	var stores []*store.Store
	var fetchers []*advisorySourceFetcher
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "remote"}
	consumer := testutil.KustomizationID("apps", "consumer")
	for _, cluster := range []manifest.ClusterID{"alpha", "omega"} {
		for _, side := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
			tree := Tree{ID: manifest.TreeID{Cluster: cluster, Side: side}, RepoRoot: root}
			o, err := newTestOrchestrator(t, tree, Config{Concurrency: 2, CacheDir: t.TempDir(), Supply: []store.SuppliedObject{suppliedSubstitution("settings")}})
			assert.NoError(t, err)
			fetcher := &advisorySourceFetcher{root: root}
			o.src.Fetchers[manifest.KindGitRepository] = fetcher
			assert.NoError(t, o.bootstrap(t.Context()))
			result, err := o.collectResult(o.run(t.Context()))
			assert.NoError(t, err)
			assert.Equal(t, len(result.Failed), 0)
			assert.Equal(t, fetcher.calls.Load(), int64(1))
			docs := outputByParent(result.Manifests)[consumer]
			if len(docs) != 1 || docs[0]["data"].(map[string]any)["value"] != "repository" {
				t.Fatalf("cold render did not use repository values: %v", docs)
			}
			runs = append(runs, Rendered{ID: tree.ID, Orchestrator: o, Result: result})
			stores = append(stores, o.store)
			fetchers = append(fetchers, fetcher)
		}
	}
	for _, phase := range []string{"cold", "warm"} {
		if phase == "warm" {
			for i := range runs {
				o := runs[i].Orchestrator
				before := outputByParent(runs[i].Result.Manifests)
				for _, id := range []manifest.NamedResource{source, consumer} {
					var execution *base.Execution
					o.tasks.Go(t.Context(), "warm/"+id.String(), func(ctx context.Context) {
						execution = o.controllers.ReconcileNode(ctx, id, manifest.DrainNone)
					})
					o.tasks.BlockTillDone()
					if execution == nil || !execution.Reused || !execution.Valid() {
						t.Fatalf("warm %s did not reuse accepted execution: %+v", id, execution)
					}
					execution.Close()
					info, ok := o.store.GetStatus(id)
					if !ok || info.Status != store.StatusReady {
						t.Fatalf("warm %s is not Ready: %+v", id, info)
					}
				}
				result, err := o.collectResult(nil)
				assert.NoError(t, err)
				assert.Diff(t, outputByParent(result.Manifests), before)
				runs[i].Result = result
				assert.Equal(t, fetchers[i].calls.Load(), int64(1))
				artifact := o.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
				assert.Equal(t, artifact.UsedLocalSSHAgent, true)
			}
		}
		clusterAdvisories(runs, stores)
		for _, run := range runs {
			assert.Equal(t, len(run.Result.Warnings), 0)
			if run.ID.Side == manifest.TreeBase {
				assert.Equal(t, len(run.Result.ClusterWarnings), 0)
				continue
			}
			assert.Diff(t, run.Result.ClusterWarnings, []manifest.Warning{
				{Category: manifest.WarnLocalSSHAgent, Message: "authenticated with the local SSH agent", Detail: []string{source.String()}, Count: 1},
				{Category: manifest.WarnSuppliedPrecedence, Message: "repository data takes precedence over supplied data", Detail: []string{"ConfigMap/apps/settings: VALUE (ci-values.yaml)"}, Count: 1},
			})
		}
	}
}

type advisorySourceFetcher struct {
	root  string
	calls atomic.Int64
}

func (f *advisorySourceFetcher) Fetch(context.Context, manifest.BaseManifest, *store.ReadSet, sourcecache.Policy) (*store.SourceArtifact, error) {
	f.calls.Add(1)
	return &store.SourceArtifact{Kind: manifest.KindGitRepository, Key: "advisory-source", FS: os.DirFS(f.root), LocalPath: f.root, UsedLocalSSHAgent: true}, nil
}

func TestRenderTrees_ClusterAdvisoriesDiscoveryFailure(t *testing.T) {
	t.Parallel()
	failure := errors.New("discovery failed before evidence")
	input := &store.SourceArtifact{LocalPath: t.TempDir(), FS: &joinedTreeFS{acquire: func() error { return failure }}}
	runs, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: input}}, Config{Concurrency: 2})
	if !errors.Is(err, failure) || len(runs) != 1 || !errors.Is(runs[0].Err, failure) || runs[0].Orchestrator != nil {
		t.Fatalf("discovery failure lost original incomplete result: %+v, %v", runs, err)
	}
	assert.Equal(t, len(runs[0].Result.ClusterWarnings), 0)
}

func TestSelect_ClusterAdvisories(t *testing.T) {
	t.Parallel()
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "flux-system", Name: "settings"}
	warnings := []manifest.Warning{
		{Category: manifest.WarnLocalSSHAgent, Message: "authenticated with the local SSH agent", Detail: []string{source.String()}, Count: 1},
		{Category: manifest.WarnSuppliedPrecedence, Message: "repository data takes precedence over supplied data", Detail: []string{fmt.Sprintf("%s: token (/supply/settings.yaml)", secret)}, Count: 1},
	}
	ordinary := manifest.Warning{Resource: manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "reader"}, Category: manifest.WarnStaleValues, Message: "ordinary", Detail: []string{"kept"}, Count: 4}
	outside := ordinary
	outside.Resource.Namespace = "flux-system"
	for _, failedSide := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(failedSide), func(t *testing.T) {
			t.Parallel()
			runs := []Rendered{selectedRun(&Result{Warnings: []manifest.Warning{ordinary, outside}}), selectedRun(&Result{ClusterWarnings: warnings})}
			failure := errors.New("original failure")
			var runErr error
			if failedSide >= 0 {
				runs[failedSide].Orchestrator = nil
				runs[failedSide].Err = failure
				runErr = failure
			}
			selected, err, diagnostic := Select(runs, runErr, []manifest.NamedResource{testutil.KustomizationID("apps", "named")}, nil)
			if failedSide >= 0 && (!errors.Is(err, failure) || !errors.Is(diagnostic, failure)) || failedSide < 0 && err != nil {
				t.Fatalf("selection changed fatal error: %v, %v", err, diagnostic)
			}
			assert.Diff(t, selected[0].Warnings, []manifest.Warning{ordinary})
			assert.Diff(t, selected[0].ClusterWarnings, []manifest.Warning(nil))
			assert.Diff(t, selected[1].ClusterWarnings, warnings)
			if failedSide >= 0 {
				for _, side := range selected {
					assert.Equal(t, len(side.Objects)+len(side.Manifests)+len(side.Failed), 0)
				}
			}
		})
	}
}
