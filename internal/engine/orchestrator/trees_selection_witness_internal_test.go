package orchestrator

import (
	"context"
	"encoding/json"
	"maps"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/selection"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type selectionWitnessCase struct {
	name   string
	roots  []manifest.NamedResource
	owners []manifest.NamedResource
	values string
}

type selectionWitnessRender struct {
	*Rendered

	wantAdmissions uint64
}

func (tc *selectionWitnessCase) run(
	t *testing.T,
	root string,
	full *Result,
	preparations map[string]map[manifest.WorkKey]manifest.WorkCounts,
) {
	t.Helper()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	for _, regime := range []string{"cold", "hot"} {
		t.Run(regime, func(t *testing.T) {
			t.Parallel()
			expectedRegime := regime
			if !persistent {
				expectedRegime = "cold"
			}
			var first []byte
			for repeat := range 2 {
				cfg := Config{RecordWork: true, Concurrency: 2}
				if regime == "hot" {
					cfg.CacheDir = t.TempDir()
					warmup, err := RenderTrees(t.Context(), []Tree{witnessTree(root, manifest.TreeHead)}, cfg)
					assert.NoError(t, err)
					assert.NoError(t, warmup.Close())
				}
				cfg.Selection.Roots = tc.roots
				tree := witnessTree(root, manifest.TreeHead)
				run := tc.render(t, &tree, &cfg, repeat == 0)
				result := run.Result
				assert.Diff(t, visibleWitnessOutput(result), witnessOutput(full, tc.owners))
				assert.Equal(t, len(result.Warnings), 0)
				assertWitnessSources(t, *run.Rendered)
				counts := preparations[tc.name+"/"+expectedRegime]
				tc.assertWork(t, &run, expectedRegime, counts)
				tc.assertCache(t, result.Work, expectedRegime)
				body, err := json.Marshal(result.Manifests)
				assert.NoError(t, err)
				if repeat == 0 {
					first = body
				} else {
					assert.Equal(t, string(body), string(first))
				}
			}
		})
	}
}

func (tc *selectionWitnessCase) render(
	t *testing.T,
	tree *Tree,
	cfg *Config,
	publicationFirst bool,
) selectionWitnessRender {
	t.Helper()
	if tc.name == "namespace" {
		return renderNamespaceWitness(t, tree, cfg, publicationFirst)
	}
	runs, err := renderTreesForTest(t.Context(), t, []Tree{*tree}, *cfg)
	assert.NoError(t, err)
	return selectionWitnessRender{Rendered: &runs[0]}
}

func selectionWitnessCases() []selectionWitnessCase {
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "release")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "set",
	}
	return []selectionWitnessCase{
		{name: "full", owners: []manifest.NamedResource{a, b, hr, rs}},

		{name: "kustomization", roots: []manifest.NamedResource{a}, owners: []manifest.NamedResource{a}},

		{name: "helmrelease", roots: []manifest.NamedResource{hr}, owners: []manifest.NamedResource{hr}},

		{
			name:   "targetPath",
			roots:  []manifest.NamedResource{hr},
			owners: []manifest.NamedResource{hr},
			values: "{kind: ConfigMap, name: vars, valuesKey: VALUE, targetPath: value}",
		},

		{
			name:   "optional-object",
			roots:  []manifest.NamedResource{hr},
			owners: []manifest.NamedResource{hr},
			values: "{kind: ConfigMap, name: absent, optional: true}",
		},

		{
			name:   "optional-key",
			roots:  []manifest.NamedResource{hr},
			owners: []manifest.NamedResource{hr},
			values: "{kind: ConfigMap, name: vars, valuesKey: absent, targetPath: value, optional: true}",
		},

		{
			name: "namespace",
			roots: []manifest.NamedResource{
				testutil.KustomizationID("apps", ""),
				testutil.HelmReleaseID("apps", ""),
				{Group: rs.Group, Kind: rs.Kind, Namespace: "apps"},
			},
			owners: []manifest.NamedResource{a, b, hr, rs},
		},
	}
}

func (tc *selectionWitnessCase) fixture(t *testing.T, root string, full []Rendered) (string, []Rendered) {
	t.Helper()
	hr := testutil.HelmReleaseID("apps", "release")
	if tc.values != "" {
		files := maps.Clone(workWitnessFiles())
		files["flux/release.yaml"] += "  valuesFrom: [" + tc.values + "]\n"
		if tc.name == "targetPath" {
			files["flux/release.yaml"] = strings.ReplaceAll(
				files["flux/release.yaml"],
				"values: {value: old}",
				"values: {}",
			)
		}
		files["flux/vars.yaml"] = strings.ReplaceAll(files["flux/vars.yaml"], "VALUE: old", "VALUE: selected")
		root = writeWitness(t, files)
		var err error
		full, err = renderTreesForTest(t.Context(), t,
			[]Tree{witnessTree(root, manifest.TreeHead)},
			Config{RecordWork: true, Concurrency: 2},
		)
		assert.NoError(t, err)
		documents := outputByParent(full[0].Result.Manifests)[hr]
		assert.Equal(t, len(documents), 1)
		value := "old"
		if tc.name == "targetPath" {
			value = "selected"
		}
		assert.Diff(t, documents[0]["data"], map[string]any{"value": value})
	}
	return root, full
}

func (tc *selectionWitnessCase) assertCache(t *testing.T, work manifest.WorkSummary, regime string) {
	t.Helper()
	hr := testutil.HelmReleaseID("apps", "release")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "set",
	}
	entry := testutil.KustomizationID("\x00", "flux")
	cache := witnessPhaseCounts(work, manifest.WorkCache)
	wantCache := map[manifest.NamedResource]manifest.WorkCounts{
		{}: {Attempts: 1, Completed: 1}, entry: {Attempts: 1, Completed: 1, CacheService: 1},
	}
	for _, owner := range tc.owners {
		if owner == rs {
			continue
		}
		wantCache[owner] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	}
	if regime == "hot" {
		for owner := range wantCache {
			wantCache[owner] = manifest.WorkCounts{Attempts: 3, Completed: 3, CacheService: 2}
		}
		if tc.name != "full" {
			// Shared lookups can move between the entry and Helm owner during publication.
			combined := cache[manifest.NamedResource{}]
			combined.Add(new(cache[hr]))
			delete(cache, hr)
			cache[manifest.NamedResource{}] = combined
			delete(wantCache, hr)
			wantCache[manifest.NamedResource{}] = manifest.WorkCounts{Attempts: 6, Completed: 6, CacheService: 4}
			if tc.name == "kustomization" {
				wantCache[manifest.NamedResource{}] = manifest.WorkCounts{Attempts: 3, Completed: 3, CacheService: 2}
			}
		}
		counts := wantCache[entry]
		counts.Attempts++
		counts.Completed++
		wantCache[entry] = counts
		if tc.name == "full" || tc.name == "namespace" {
			wantCache[rs] = manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1}
		}
	}
	for owner, counts := range cache {
		assertWitnessCacheCounts(
			t,
			&counts,
			new(wantCache[owner]),
			witnessCacheLimit(len(witnessAdmissions(tc.owners, "apps"))),
		)
		cache[owner] = wantCache[owner]
	}
	assert.Diff(t, cache, wantCache)
}

func (tc *selectionWitnessCase) assertWork(
	t *testing.T,
	run *selectionWitnessRender,
	regime string,
	preparations map[manifest.WorkKey]manifest.WorkCounts,
) {
	t.Helper()
	result := run.Result
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "release")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "set",
	}
	engines := map[manifest.NamedResource]uint64{}
	if regime == "cold" {
		engines[manifest.NamedResource{}] = 1
	}
	helm, evaluation := map[manifest.NamedResource]uint64{}, map[manifest.NamedResource]uint64{}
	for _, owner := range tc.owners {
		switch owner {
		case a, b:
			if regime == "cold" {
				engines[owner] = 1
			}
		case hr:
			if regime == "cold" {
				helm[hr] = 1
			}
		case rs:
			evaluation[rs] = 1
		}
	}
	assert.Diff(t, witnessWork(result.Work, manifest.WorkAdmission), witnessAdmissions(tc.owners, "apps"))
	gotPreparations := witnessPreparations(result.Work)
	if run.wantAdmissions > 0 {
		alias := namespaceWitnessAlias()
		key := selectionPreparationKey(alias, alias)
		artifact, ok := run.store.GetArtifact(alias, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, !ok, "missing namespace source artifact")
		counts, diff := namespacePreparationDiff(gotPreparations.rows[key], artifact.LocalPath, run.wantAdmissions)
		assert.Equal(t, diff, "")
		preparations = maps.Clone(preparations)
		preparations[key] = counts
	}
	if regime == "hot" {
		tc.assertWarmPreparationKeys(t, gotPreparations)
	}
	assertWitnessPreparations(t, gotPreparations.counts, preparations)
	assert.Diff(t, witnessWork(result.Work, manifest.WorkKustomize), engines)
	assert.Diff(t, witnessWork(result.Work, manifest.WorkHelm), helm)
	assert.Diff(t, witnessWork(result.Work, manifest.WorkEvaluation), evaluation)
	assert.Diff(t, witnessWork(result.Work, manifest.WorkAcquisition), map[manifest.NamedResource]uint64{{}: 1})
	assert.Diff(
		t,
		witnessPhaseCounts(result.Work, manifest.WorkAcquisition),
		map[manifest.NamedResource]manifest.WorkCounts{{}: {Attempts: 1, Completed: 1, LocalAcquisitions: 1}},
	)
}

func (tc *selectionWitnessCase) assertWarmPreparationKeys(t *testing.T, got witnessPreparationWork) {
	t.Helper()
	for key, rows := range got.rows {
		if !slices.Contains(tc.owners, key.Owner) {
			continue
		}
		distinct, extra := warmPreparationKeys(rows)
		assert.Diff(t, extra, map[manifest.WorkKey]uint64{})
		// Only Helm and Kustomize prepare both pending and accepted render addresses.
		wantDistinct := 4
		if key.Owner.Kind == manifest.KindResourceSet {
			wantDistinct = 2
		}
		assert.Equal(t, distinct, wantDistinct)
	}
}

//nolint:gocritic // The distinct count and the repeated rows are separate assertions.
func warmPreparationKeys(work manifest.WorkSummary) (int, map[manifest.WorkKey]uint64) {
	extra := make(map[manifest.WorkKey]uint64)
	distinct := 0
	for i := range work {
		row := &work[i]
		if row.Counts.RawKeyPrepares == 0 {
			continue
		}
		distinct++
		if row.Counts.RawKeyPrepares > 1 {
			extra[row.Key] = row.Counts.RawKeyPrepares - 1
		}
	}
	return distinct, extra
}

func TestWarmPreparationKeys_RejectsOrdinaryLookups(t *testing.T) {
	t.Parallel()
	owner := testutil.HelmReleaseID("apps", "release")
	source := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "apps-release",
	}
	prepare := manifest.WorkKey{
		Tree:  manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		Owner: owner, Source: source, Phase: manifest.WorkPreparation,
	}
	work := make(manifest.WorkSummary, 0, 6)
	for i, key := range []string{"plain-1", "plain-2", "render-accepted", "render-other"} {
		row := prepare
		row.Input = key
		count := uint64(1)
		if i >= 2 {
			count = 2
		}
		work = append(work, manifest.WorkRecord{Key: row, Counts: manifest.WorkCounts{RawKeyPrepares: count}})
	}
	first, second := work[2].Key, work[3].Key
	for _, key := range []manifest.WorkKey{first, second} {
		key.Phase = manifest.WorkCache
		work = append(work, manifest.WorkRecord{
			Key: key, Counts: manifest.WorkCounts{Attempts: 2, Completed: 2, CacheService: 2},
		})
	}
	distinct, extra := warmPreparationKeys(witnessPreparations(work).rows[prepare])
	assert.Equal(t, distinct, 4)
	assert.Diff(t, extra, map[manifest.WorkKey]uint64{first: 1, second: 1})
}

func namespaceWitnessAlias() manifest.NamedResource {
	return manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "cluster",
	}
}

type namespaceAdmissionWitness struct {
	reconcileController

	o          *Orchestrator
	index      *manifest.DiscoveryIndex
	production manifest.Production
	inputs     []bool
}

func (w *namespaceAdmissionWitness) entryPublished() bool {
	return slices.Contains(w.o.store.Productions(testutil.KustomizationID("apps", "a"), w.index), w.production)
}

func (w *namespaceAdmissionWitness) ReconcileNode(
	ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel, previous schedule.Result,
) *base.Execution {
	if id == namespaceWitnessAlias() {
		w.inputs = append(w.inputs, w.entryPublished())
	}
	return w.reconcileController.ReconcileNode(ctx, id, level, previous)
}

func renderNamespaceWitness(
	t *testing.T,
	tree *Tree,
	cfg *Config,
	publicationFirst bool,
) selectionWitnessRender {
	t.Helper()
	ctx := t.Context()
	invocation := prepareOrderingTrees(t, []Tree{*tree}, cfg)
	run := &invocation.results[0]
	assert.NoError(t, run.Err)
	o := run.Orchestrator
	alias := namespaceWitnessAlias()
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "flux", manifest.NamedResource{}).Named()
	published := make(chan struct{})
	witness := &namespaceAdmissionWitness{
		reconcileController: o.src, o: o, index: o.loader.SelectionIndex(),
		production: manifest.Production{Owner: entry, Source: manifest.BootstrapSourceID},
	}
	if !publicationFirst {
		prior := &base.Execution{Controller: o.src.Controller}
		prior.Reads.Init(o.store, alias, nil)
		prior.Reads.BeginExecution(nil)
		*o.probeSlot(alias) = prior
		child, artifact, _, err := o.admission.Resolve(ctx, alias, o.store.GetObject(alias), &prior.Reads)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			child.Obj == nil || artifact == nil || !artifact.WorkingTree(),
			"namespace alias did not resolve",
		)
		request := selection.PrepareRequest{
			Invocation: true, Missing: o.cfg.missing, Reads: &prior.Reads,
			Preparation: &prior.AdmissionPreparation, Generation: []store.Child{child},
		}
		witness.inputs = append(witness.inputs, witness.entryPublished())
		for range 2 {
			proposal, err := o.selection.Prepare(ctx, request)
			assert.NoError(t, err)
			proposal.Retire()
		}
		assert.Diff(t, prior.Reads.WorkScope().Admission().Counts, preparationCounts(1, 0, 4, 0))
	}
	o.controllers.Kustomization = &orderedPublisherController{
		reconcileController: o.ksc, publisher: entry, published: published, t: t,
	}
	o.controllers.Source = &afterPublicationController{
		reconcileController: witness, published: published, only: alias, tasks: o.tasks,
	}
	invocation.renderTrees(ctx)
	assert.NoError(t, run.Err)
	t.Cleanup(run.Result.Close)
	wantInputs := []bool{false, true}
	if publicationFirst {
		wantInputs = []bool{true}
	}
	assert.Diff(t, slices.Compact(slices.Clone(witness.inputs)), wantInputs)
	if cfg.CacheDir != "" {
		reason := sourcecache.RenderPersistenceDisabledReason()
		assert.Equal(t, cfg.Cache.CanPersistRenders(), reason == "")
		assert.Equal(t, cfg.Cache.Directory(), cfg.CacheDir)
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" || setting.Key == "vcs.modified" {
					t.Logf("%s=%s", setting.Key, setting.Value)
				}
			}
		}
		t.Logf("disk backing=%t, render persistence disabled reason=%q, entry production=%v",
			cfg.Cache.Directory() != "", reason, witness.inputs)
	}
	return selectionWitnessRender{Rendered: run, wantAdmissions: uint64(len(wantInputs))}
}

//nolint:gocritic // The aggregate and detailed mismatch describe the same preparation rows.
func namespacePreparationDiff(
	work manifest.WorkSummary, root string, wantAdmissions uint64,
) (manifest.WorkCounts, string) {
	var readers uint64
	if wantAdmissions == 2 {
		readers = 4
	}
	got := make(map[string]manifest.WorkCounts, len(work))
	for i := range work {
		row := &work[i]
		got[row.Key.Input] = row.Counts
	}
	want := map[string]manifest.WorkCounts{
		witnessAdmissionInput():          preparationCounts(wantAdmissions, 0, readers, 0),
		filepath.Join(root, "apps", "a"): preparationCounts(1, 3, 2, 0),
		filepath.Join(root, "apps", "b"): preparationCounts(1, 3, 2, 0),
	}
	var total manifest.WorkCounts
	for _, counts := range want {
		total.Add(&counts)
	}
	return total, cmp.Diff(want, got)
}

func TestNamespacePreparations_RejectsUnchangedInput(t *testing.T) {
	t.Parallel()
	alias := namespaceWitnessAlias()
	key := selectionPreparationKey(alias, alias)
	work := manifest.WorkSummary{
		{Key: key, Counts: preparationCounts(2, 0, 0, 0)},
		{Key: key, Counts: preparationCounts(1, 3, 2, 0)},
		{Key: key, Counts: preparationCounts(1, 3, 2, 0)},
		{Key: key, Counts: manifest.WorkCounts{Attempts: 2, Completed: 2, CacheService: 2}},
	}
	work[0].Key.Input = witnessAdmissionInput()
	work[1].Key.Input, work[2].Key.Input = "apps/a", "apps/b"
	work[3].Key.Input, work[3].Key.Phase = work[0].Key.Input, manifest.WorkCache
	_, diff := namespacePreparationDiff(witnessPreparations(work).rows[key], "", 1)
	assert.Equal(t, strings.Contains(diff, "Attempts:"), true)
	work[0].Counts = preparationCounts(1, 0, 0, 0)
	_, diff = namespacePreparationDiff(witnessPreparations(work).rows[key], "", 1)
	assert.Equal(t, diff, "")
}

func TestRenderTrees_SelectionWorkWitnesses(t *testing.T) {
	t.Parallel()
	preparations := coldSelectionPreparations()
	maps.Copy(preparations, hotSelectionPreparations())
	root := writeWitness(t, workWitnessFiles())
	full, err := renderTreesForTest(t.Context(), t,
		[]Tree{witnessTree(root, manifest.TreeHead)},
		Config{RecordWork: true, Concurrency: 2},
	)
	assert.NoError(t, err)
	for _, tc := range selectionWitnessCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			selectedRoot, selectedFull := tc.fixture(t, root, full)
			tc.run(t, selectedRoot, selectedFull[0].Result, preparations)
		})
	}
}
