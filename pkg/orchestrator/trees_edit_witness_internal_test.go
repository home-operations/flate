package orchestrator

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

type editWitnessOutput struct {
	encoded   map[string][]map[string]any
	templates uint64
}

type editWitnessRun struct {
	test   editWitnessCase
	roots  [2]string
	full   [2]*Result
	files  [2]int
	regime string
}

func (r *editWitnessRun) assertRun(t *testing.T, run Rendered, side int) editWitnessOutput {
	t.Helper()
	tc, regime, roots, full := r.test, r.regime, r.roots, r.full
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "release")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "set",
	}
	owners := tc.owners
	if regime == "hot" {
		owners = []manifest.NamedResource{a, b, hr, rs}
	}
	got := visibleWitnessOutput(run.Result)
	wantSide := side
	if regime == "hot" {
		wantSide = 0
	}
	assert.Diff(t, got, witnessOutput(full[wantSide], owners))
	if regime == "hot" {
		assert.Diff(
			t,
			run.Result.Warnings,
			[]manifest.Warning{
				{
					Category: manifest.WarnPathConfig,
					Message:  "--path and --path-orig resolve to the same root; ignoring --path-orig",
					Detail:   []string{roots[0]},
					Count:    1,
				},
			},
		)
	} else {
		assert.Equal(t, len(run.Result.Warnings), 0)
	}
	assertWitnessSources(t, run)
	engines := r.engineWitnesses(side)
	active := slices.Clone(tc.owners)
	evaluates := slices.Contains(tc.owners, rs)
	if regime == "hot" {
		active, evaluates = []manifest.NamedResource{a, b, hr, rs}, true
	}
	evaluation := map[manifest.NamedResource]uint64{}
	if evaluates {
		evaluation[rs] = 1
	}
	namespace := "apps"
	if tc.name == "namespace" && side == 1 && regime != "hot" {
		namespace = "moved"
	}
	admissions := witnessAdmissions(active, namespace)
	if regime != "hot" && !r.entryChanged() && !r.historicalEntry() {
		for _, owner := range []manifest.NamedResource{a, b, hr, rs} {
			if !slices.Contains(active, owner) {
				delete(admissions, owner)
			}
		}
		if !slices.Contains(active, a) {
			delete(admissions, manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "vars"})
		}
	}
	if child := tc.children[wantSide]; child != "" {
		admissions[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: child}] = 1
	}
	assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkAdmission), admissions)
	assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkKustomize), engines)
	assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkEvaluation), evaluation)
	assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkAcquisition), map[manifest.NamedResource]uint64{{}: 1})

	templates := r.assertCachedWork(t, run, side, active)
	encoded := make(map[string][]map[string]any, len(got))
	for id, docs := range got {
		encoded[id.String()] = docs
	}

	return editWitnessOutput{encoded: encoded, templates: templates}
}

func TestRenderTrees_EditWorkWitnesses(t *testing.T) {
	t.Parallel()
	for _, tc := range editWitnessCases() {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); tc.run(t) })
	}
}

func (r *editWitnessRun) render(t *testing.T) []byte {
	t.Helper()
	roots, regime := r.roots, r.regime
	cacheDir := t.TempDir()
	cfg := Config{RecordWork: true, Concurrency: 2, CacheDir: cacheDir}
	if regime == "hot" || regime == "warm-after-edit" {
		_, err := RenderTrees(t.Context(), []Tree{witnessTree(roots[0], manifest.TreeBase)}, cfg)
		assert.NoError(t, err)
	}
	trees := []Tree{witnessTree(roots[0], manifest.TreeBase), witnessTree(roots[1], manifest.TreeHead)}
	if regime == "hot" {
		trees[1] = witnessTree(roots[0], manifest.TreeHead)
	}
	runs, err := RenderTrees(t.Context(), trees, cfg)
	assert.NoError(t, err)
	outputs := make([]map[string][]map[string]any, 0, len(runs))
	var templates uint64

	for side, run := range runs {
		assertRunResult := r.assertRun(t, run, side)
		encoded, count := assertRunResult.encoded, assertRunResult.templates
		outputs = append(outputs, encoded)
		templates += count
	}
	if r.test.name == "readme" {
		assert.Equal(t, templates, uint64(0))
	}
	body, err := json.Marshal(outputs)
	assert.NoError(t, err)
	return body
}

func (r *editWitnessRun) assertCachedWork(
	t *testing.T,
	run Rendered,
	side int,
	active []manifest.NamedResource,
) uint64 {
	t.Helper()
	tc, regime := r.test, r.regime
	hr := testutil.HelmReleaseID("apps", "release")
	helm := workCounts(run.Result.Work, hr, manifest.WorkHelm)
	postRender := manifest.WorkCounts{}
	if tc.postRender {
		postRender = helm
	}
	assert.Equal(t, workCounts(run.Result.Work, hr, manifest.WorkPostRender), postRender)

	{
		var attempts uint64
		if regime != "hot" && (regime != "warm-after-edit" || side == 1) && slices.Contains(tc.owners, hr) {
			attempts = 1
		}
		assert.Equal(t, helm, manifest.WorkCounts{Attempts: attempts, Completed: attempts})
	}
	r.assertCachedOwners(t, run, active, side)
	entry := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "\x00",
		Name:      "flux",
	}
	entryCache := manifest.WorkCounts{}
	if r.entryChanged() {
		entryCache = manifest.WorkCounts{Attempts: 4, Completed: 4, CacheService: 2}
	}
	if r.historicalEntry() {
		entryCache = manifest.WorkCounts{Attempts: 3, Completed: 3, CacheService: 2}
		if side == 1 {
			entryCache = manifest.WorkCounts{Attempts: 4, Completed: 2, Failures: 2}
		}
	}
	if regime == "hot" {
		entryCache = manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1}
	}
	assertWitnessCacheCounts(
		t, new(workCounts(run.Result.Work, entry, manifest.WorkCache)), &entryCache,
		witnessCacheLimit(len(witnessWork(r.full[side].Work, manifest.WorkAdmission))),
	)

	return helm.Attempts
}

func (tc *editWitnessCase) run(t *testing.T) {
	t.Helper()
	before := maps.Clone(workWitnessFiles())
	maps.Copy(before, tc.prepare)
	after := maps.Clone(before)
	if tc.old == "" {
		after[tc.file] = tc.next
	} else {
		if !strings.Contains(after[tc.file], tc.old) {
			t.Fatal("fixture mutation has no input")
		}
		after[tc.file] = strings.ReplaceAll(after[tc.file], tc.old, tc.next)
	}
	if tc.second != "" {
		after[tc.second] = strings.ReplaceAll(after[tc.second], "value: old", "value: new")
	}
	roots := [2]string{writeWitness(t, before), writeWitness(t, after)}
	var full [2]*Result
	for side, root := range roots {
		runs, err := RenderTrees(
			t.Context(),
			[]Tree{witnessTree(root, manifest.TreeHead)},
			Config{RecordWork: true, Concurrency: 2},
		)
		assert.NoError(t, err)
		full[side] = runs[0].Result
	}

	if len(tc.owners) == 0 {
		// Skipped-input rows MUST retain equal independent full-build oracles.
		assert.Diff(t, visibleWitnessOutput(full[1]), visibleWitnessOutput(full[0]))
	}

	for _, regime := range []string{"cold", "source-warm", "hot", "warm-after-edit"} {
		run := editWitnessRun{
			test:   *tc,
			roots:  roots,
			full:   full,
			files:  [2]int{len(before), len(after)},
			regime: regime,
		}
		if regime == "warm-after-edit" && tc.name == "unused" {
			run.test.owners = []manifest.NamedResource{testutil.KustomizationID("apps", "a")}
		}
		t.Run(regime, run.run)
	}
}

func (r *editWitnessRun) run(t *testing.T) {
	t.Parallel()
	var first []byte
	for repeat := range 2 {
		body := r.render(t)
		if repeat == 0 {
			first = body
		} else {
			assert.Equal(t, string(body), string(first))
		}
	}
}

func (r *editWitnessRun) engineWitnesses(side int) map[manifest.NamedResource]uint64 {
	engines := map[manifest.NamedResource]uint64{}
	if r.regime == "hot" || r.regime == "warm-after-edit" && side == 0 {
		return engines
	}
	if r.entryChanged() {
		engines[manifest.NamedResource{}] = 1
	}
	if r.historicalEntry() {
		engines[testutil.KustomizationID("\x00", "flux")] = 1
	}
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	for _, owner := range r.test.owners {
		if owner == a || owner == b {
			engines[owner] = 1
		}
	}
	return engines
}

func (r *editWitnessRun) assertCachedOwners(
	t *testing.T,
	run Rendered,
	active []manifest.NamedResource,
	side int,
) {
	t.Helper()
	regime := r.regime
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "release")
	for _, owner := range []manifest.NamedResource{a, b, hr} {
		cached := workCounts(run.Result.Work, owner, manifest.WorkCache)
		var want manifest.WorkCounts
		if slices.Contains(active, owner) {
			want = manifest.WorkCounts{Attempts: 4, Completed: 2, Failures: 2}
			if regime == "hot" || regime == "warm-after-edit" && side == 0 {
				want = manifest.WorkCounts{Attempts: 3, Completed: 3, CacheService: 2}
			}
		}
		assertWitnessCacheCounts(
			t, &cached, &want,
			witnessCacheLimit(len(witnessWork(r.full[side].Work, manifest.WorkAdmission))),
		)
		if want.Attempts != 0 {
			preparation := workCounts(run.Result.Work, owner, manifest.WorkPreparation)
			assert.Equal(t, preparation.OwnerPrepares != 0, true)
		}
	}
	visited := workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkPreparation).FileEntries
	assert.Equal(t, visited >= uint64(r.files[side]), true)
}

func (r *editWitnessRun) entryChanged() bool {
	return strings.HasPrefix(r.test.file, "flux/") || strings.HasPrefix(r.test.second, "flux/")
}

func (r *editWitnessRun) historicalEntry() bool {
	return r.regime == "warm-after-edit" && strings.HasPrefix(r.test.file, "apps/a/") && !r.entryChanged()
}
