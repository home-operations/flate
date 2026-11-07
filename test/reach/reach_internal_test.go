package reach

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"testing"
	"weak"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type reachOracle struct {
	cfg      *orchestrator.Config
	controls map[string]*reachControl
	worlds   []reachWorld
}

func (o *reachOracle) base(t *testing.T) {
	cfg, controls := o.cfg, o.controls
	t.Helper()
	for i := range o.worlds {
		t.Run(o.worlds[i].spec.name, func(t *testing.T) {
			t.Parallel()
			tc := &o.worlds[i].spec
			trees := []orchestrator.Tree{reachTree(controls[tc.name].root, tc, manifest.TreeBase)}
			runs, err := orchestrator.RenderTrees(t.Context(), trees, *cfg)
			for _, run := range runs {
				t.Cleanup(run.Result.Close)
			}
			if err != nil && !errors.Is(err, manifest.ErrFlux) {
				t.Fatal(err)
			}
			controls[tc.name].result = runs[0].Result
		})
	}
}

func reachCases() []reachCase { return slices.Concat(reachFixtureCases(), reachDependencyCases()) }

type reachWorld struct {
	spec  reachCase
	roots [2]string
	want  [2]map[string][]map[string]any
	first string
	refs  []reachWeak
}

func assertReachGlobalUsers(t *testing.T, full [2]*orchestrator.Result) {
	t.Helper()
	outputs := [2]map[manifest.NamedResource][]map[string]any{}
	for side, result := range full {
		outputs[side] = make(map[manifest.NamedResource][]map[string]any, len(result.Manifests))
		for _, batch := range result.Manifests {
			if batch.Parent != nil {
				outputs[side][batch.Parent.Named()] = batch.Documents
			}
		}
	}
	before, after := outputs[0], outputs[1]
	users := []string{}
	for id, docs := range before {
		consumer := id.Kind == manifest.KindKustomization && id.Group == manifest.FluxKustomizeDomain &&
			id.Name != "cluster-vars"
		if consumer && manifest.Fingerprint(docs) != manifest.Fingerprint(after[id]) {
			users = append(users, id.String())
		}
	}
	slices.Sort(users)
	assert.Diff(t, users, globalVariableUsers())
}

func (o *reachOracle) head(t *testing.T) {
	cfg, controls, worlds := o.cfg, o.controls, o.worlds
	t.Helper()
	for i := range worlds {
		t.Run(worlds[i].spec.name, func(t *testing.T) {
			t.Parallel()
			world := &worlds[i]
			tc := &world.spec
			trees := []orchestrator.Tree{reachTree(world.roots[1], tc, manifest.TreeHead)}
			runs, err := orchestrator.RenderTrees(t.Context(), trees, *cfg)
			for _, run := range runs {
				t.Cleanup(run.Result.Close)
			}
			if err != nil && !errors.Is(err, manifest.ErrFlux) {
				t.Fatal(err)
			}
			full := [2]*orchestrator.Result{controls[tc.name].result, runs[0].Result}
			for side, result := range full {
				assertReachControl(t, result, tc)
				world.want[side] = reachOutput(result, tc, true)
			}
			if tc.name == "global-configmap" {
				assertReachGlobalUsers(t, full)
			}
		})
	}
}

//nolint:paralleltest // The lifetime proof changes the process GC percentage and samples retained heap.
func TestRenderTrees_Reach(t *testing.T) {
	previousGC := debug.SetGCPercent(100)
	defer debug.SetGCPercent(previousGC)
	cfg := reachConfig()
	oracleCache, err := sourcecache.New(t.Context(), "")
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, oracleCache.Close()) })
	cfg.Cache = oracleCache
	cfg.HelmClient = helm.NewClient(oracleCache)
	controls, worlds := prepareReachWorlds(t, reachCases())
	oracle := reachOracle{cfg: &cfg, controls: controls, worlds: worlds}
	oracle.render(t)
	assert.NoError(t, oracleCache.Close())
	cfg.Cache = nil
	cfg.HelmClient = nil
	cfg.RecordWork = true
	var baseline uint64
	for pass, name := range []string{"control", "repeat"} {
		t.Run(name, func(t *testing.T) {
			for i := range worlds {
				t.Run(worlds[i].spec.name, func(t *testing.T) {
					t.Parallel()
					encoded, refs := runReachWorld(t, &worlds[i], &cfg)
					worlds[i].refs = refs
					if pass == 0 {
						worlds[i].first = encoded
					} else {
						assert.Equal(t, encoded, worlds[i].first)
					}
				})
			}
		})
		// Two cycles discard sync.Pool victim caches after every request finishes.
		runtime.GC()
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		assertReachReleased(t, worlds)
		if pass == 0 {
			baseline = stats.HeapAlloc
		} else {
			tolerance := max(uint64(1<<20), baseline/50)
			if stats.HeapAlloc > baseline+tolerance {
				t.Fatalf("retained heap: baseline=%d after=%d tolerance=%d", baseline, stats.HeapAlloc, tolerance)
			}
			t.Logf("eight released requests: baseline=%d after=%d tolerance=%d", baseline, stats.HeapAlloc, tolerance)
		}
	}
	runtime.KeepAlive(worlds)
	runtime.KeepAlive(controls)
}

func reachConfig() orchestrator.Config {
	return orchestrator.Config{
		Concurrency: 2,
		HelmOptions: helm.Options{
			DisableChartDigestTracking: true,
			KubeVersion:                helm.BundledKubeVersion(),
			SkipTests:                  true,
			SkipCRDs:                   true,
			SkipKinds:                  []string{manifest.KindSecret},
		},
	}
}

func reachTree(root string, tc *reachCase, side manifest.TreeSide) orchestrator.Tree {
	return orchestrator.Tree{
		ID: manifest.TreeID{
			Cluster: ".",
			Side:    side,
		},
		RepoRoot: root,
		Path: filepath.Join(
			root,
			tc.entry,
		),
		SelfURLs: []string{
			tc.selfURL,
			tc.selfURL + ".git",
		},
	}
}

func reachWork(work manifest.WorkSummary, phase manifest.WorkPhase) map[string]uint64 {
	out := map[string]uint64{}
	for i := range work {
		row := &work[i]
		if row.Key.Phase == phase && row.Counts.Attempts != 0 {
			out[row.Key.Owner.String()] += row.Counts.Attempts
		}
	}
	return out
}

func assertReachFailures(t *testing.T, result *orchestrator.Result, fixture string) {
	t.Helper()
	want := []string{}
	if fixture == "global" {
		want = []string{
			"HelmRelease.helm.toolkit.fluxcd.io/authentik/authentik",
			"HelmRelease.helm.toolkit.fluxcd.io/forgejo/forgejo",
			"HelmRelease.helm.toolkit.fluxcd.io/maddy/maddy",
			"HelmRelease.helm.toolkit.fluxcd.io/tt-rss/tt-rss",
		}
	}
	got := make([]string, 0, len(result.Failed))
	for id := range result.Failed {
		got = append(got, id.String())
	}
	slices.Sort(got)
	assert.Diff(t, got, want)
}

func runReachWorld(t *testing.T, world *reachWorld, cfg *orchestrator.Config) (string, []reachWeak) {
	t.Helper()
	tc := &world.spec
	trees := []orchestrator.Tree{
		reachTree(world.roots[0], tc, manifest.TreeBase),
		reachTree(world.roots[1], tc, manifest.TreeHead),
	}
	runs, err := orchestrator.RenderTrees(t.Context(), trees, *cfg)
	for _, run := range runs {
		t.Cleanup(run.Result.Close)
	}
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), tc.fixture == "global")
	joint := map[string]uint64{}
	output := [2]map[string][]map[string]any{}
	refs := make([]reachWeak, 0, len(runs))
	for side := range runs {
		run := &runs[side]
		assertReachEffectiveWork(t, run.Result)
		assertReachFailures(t, run.Result, tc.fixture)
		assertReachAdmissions(t, run.Result, tc)

		builders := map[string]uint64{}
		for _, id := range tc.builders {
			builders[id] = 1
		}
		assert.Diff(t, reachWork(run.Result.Work, manifest.WorkKustomize), builders)
		for id, n := range reachWork(run.Result.Work, manifest.WorkHelm) {
			joint[id] += n
		}
		output[side] = reachOutput(run.Result, tc, false)
		assert.Diff(t, output[side], world.want[side])
		for i := range run.Result.Work {
			row := &run.Result.Work[i]
			assert.Equal(t, row.Key.Tree, run.ID)
			if row.Key.Phase == manifest.WorkKustomize || row.Key.Phase == manifest.WorkHelm {
				assert.Equal(t, row.Counts.Completed, row.Counts.Attempts)
				assert.Equal(t, row.Counts.Failures, uint64(0))
			}
		}
		refs = append(refs, reachWeak{weak.Make(run.Result), weak.Make(run.Store()), weak.Make(run.Orchestrator)})
	}
	assert.Diff(t, joint, tc.templates)
	encoded, err := json.Marshal(output)
	assert.NoError(t, err)
	var templates uint64
	for _, count := range joint {
		templates += count
	}
	t.Logf("cold request: builds=%d per side, templates=%d jointly", len(tc.builders), templates)
	return string(encoded), refs
}

type reachWeak struct {
	result       weak.Pointer[orchestrator.Result]
	store        weak.Pointer[store.Store]
	orchestrator weak.Pointer[orchestrator.Orchestrator]
}

func globalVariableUsers() []string {
	return []string{
		"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/apache-musicindex",
		"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/jellyfin",
		"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/music-assistant",
		"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/nextcloud",
		"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/vpn-lidarr",
		"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/vpn-prowlarr",
		"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/vpn-qbittorrent",
	}
}

type reachCase struct {
	admissions                                                      []string
	name, fixture, entry, selfURL                                   string
	builders, outputOwners, declarationParents, prerequisiteParents []string
	templates                                                       map[string]uint64
}

type reachControl struct {
	root   string
	result *orchestrator.Result
}

func reachOutput(result *orchestrator.Result, tc *reachCase, full bool) map[string][]map[string]any {
	out := map[string][]map[string]any{}
	for _, batch := range result.Manifests {
		if batch.Parent == nil {
			continue
		}
		parent := batch.Parent.Named()
		owner := parent.String()
		if full && !slices.Contains(tc.outputOwners, owner) {
			continue
		}
		for _, doc := range batch.Documents {
			id := manifest.DocIdentity(doc)
			if full && !tc.includes(owner, id) {
				continue
			}
			out[owner] = append(out[owner], doc)
		}
	}
	for _, docs := range out {
		slices.SortFunc(
			docs,
			func(a, b map[string]any) int { return manifest.DocIdentity(a).Compare(manifest.DocIdentity(b)) },
		)
	}
	return out
}

func prepareReachWorlds(t *testing.T, cases []reachCase) (map[string]*reachControl, []reachWorld) {
	t.Helper()
	controls := map[string]*reachControl{}
	worlds := make([]reachWorld, len(cases))
	for i := range cases {
		tc := &cases[i]
		after := testfixture.Materialize(t, "reach-precision", tc.name)
		root := filepath.Dir(after.Repo)
		controls[tc.name] = &reachControl{root: filepath.Join(root, "identity-base", tc.fixture)}
		for _, step := range after.Plan.Steps {
			if step.Edit != nil {
				after.Edit(t, step)
			}
		}
		worlds[i] = reachWorld{
			spec:  *tc,
			roots: [2]string{controls[tc.name].root, filepath.Join(root, "identity-head", tc.fixture)},
		}
	}
	return controls, worlds
}

func assertReachControl(t *testing.T, result *orchestrator.Result, tc *reachCase) {
	t.Helper()
	assertReachEffectiveWork(t, result)
	for id, failure := range result.Failed {
		if slices.Contains(tc.builders, id.String()) || tc.templates[id.String()] != 0 {
			t.Fatalf("full control failed selected producer %s: %v", id, failure)
		}
	}
}

func assertReachEffectiveWork(t *testing.T, result *orchestrator.Result) {
	t.Helper()
	transformed := map[manifest.NamedResource]manifest.NamedResource{}
	for _, batch := range result.Manifests {
		if batch.Parent == nil {
			continue
		}
		for _, document := range batch.Documents {
			id := manifest.DocIdentity(document)
			if id.Namespace == "" {
				continue
			}
			id.Namespace = ""
			transformed[id] = batch.Parent.Named()
		}
	}
	for i := range result.Work {
		row := &result.Work[i]
		id := row.Key.Owner
		if row.Counts.Attempts == 0 || id.Namespace != "" ||
			row.Key.Phase != manifest.WorkKustomize && row.Key.Phase != manifest.WorkHelm {
			continue
		}
		if parent, found := transformed[id]; found {
			t.Errorf("engine rendered %s before %s assigned its namespace", id, parent)
		}
	}
}

func assertReachReleased(t *testing.T, worlds []reachWorld) {
	t.Helper()
	for i := range worlds {
		world := &worlds[i]
		for _, refs := range world.refs {
			assert.Equal(t, refs.result.Value() == nil, true)
			assert.Equal(t, refs.store.Value() == nil, true)
			assert.Equal(t, refs.orchestrator.Value() == nil, true)
		}
	}
}

func assertReachAdmissions(t *testing.T, result *orchestrator.Result, tc *reachCase) {
	t.Helper()
	admissions := map[string]uint64{}
	for i := range result.Work {
		record := &result.Work[i]
		if record.Key.Phase == manifest.WorkAdmission && record.Key.Owner.KindInfo().Parent {
			admissions[record.Key.Owner.String()] += record.Counts.Attempts
		}
	}
	expectedAdmissions := map[string]uint64{}
	for _, id := range tc.admissions {
		expectedAdmissions[id] = 1
	}
	assert.Diff(t, admissions, expectedAdmissions)
}

func (tc *reachCase) includes(owner string, id manifest.NamedResource) bool {
	if slices.Contains(tc.declarationParents, owner) {
		if id.Kind != manifest.KindKustomization || id.Group != manifest.FluxKustomizeDomain ||
			!slices.Contains(tc.builders, id.String()) {
			return false
		}
	}
	if slices.Contains(tc.prerequisiteParents, owner) {
		kind := id.KindInfo()
		if !kind.Tracked || kind.Data {
			return false
		}
	}
	return true
}

func (o *reachOracle) render(t *testing.T) {
	t.Helper()
	t.Run("full_base", o.base)
	t.Run("full_head", o.head)
}
