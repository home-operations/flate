package orchestrator

import (
	"encoding/json"
	"maps"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

type selectionWitnessCase struct {
	name   string
	roots  []manifest.NamedResource
	owners []manifest.NamedResource
	values string
}

func (tc *selectionWitnessCase) run(
	t *testing.T,
	root string,
	full *Result,
	preparations map[string]map[manifest.WorkKey]manifest.WorkCounts,
) {
	t.Helper()
	for _, regime := range []string{"cold", "hot"} {
		t.Run(regime, func(t *testing.T) {
			t.Parallel()
			var first []byte
			for repeat := range 2 {
				cfg := Config{RecordWork: true, Concurrency: 2}
				if regime == "hot" {
					cfg.CacheDir = t.TempDir()
					_, err := RenderTrees(t.Context(), []Tree{witnessTree(root, manifest.TreeHead)}, cfg)
					assert.NoError(t, err)
				}
				cfg.Selection.Roots = tc.roots
				runs, err := RenderTrees(t.Context(), []Tree{witnessTree(root, manifest.TreeHead)}, cfg)
				assert.NoError(t, err)
				result := runs[0].Result
				assert.Diff(t, visibleWitnessOutput(result), witnessOutput(full, tc.owners))
				assert.Equal(t, len(result.Warnings), 0)
				assertWitnessSources(t, runs[0])
				tc.assertWork(t, result, regime, preparations[tc.name+"/"+regime])
				tc.assertCache(t, result.Work, regime)
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
		full, err = RenderTrees(
			t.Context(),
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
			witnessPreparationLimit(len(witnessAdmissions(tc.owners, "apps"))),
		)
		cache[owner] = wantCache[owner]
	}
	assert.Diff(t, cache, wantCache)
}

func (tc *selectionWitnessCase) assertWork(
	t *testing.T,
	result *Result,
	regime string,
	preparations map[manifest.WorkKey]manifest.WorkCounts,
) {
	t.Helper()
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
	assertWitnessPreparations(
		t,
		witnessPreparations(result.Work),
		preparations,
		witnessPreparationLimit(len(witnessAdmissions(tc.owners, "apps"))),
	)
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

func TestRenderTrees_SelectionWorkWitnesses(t *testing.T) {
	t.Parallel()
	preparations := coldSelectionPreparations()
	maps.Copy(preparations, hotSelectionPreparations())
	root := writeWitness(t, workWitnessFiles())
	full, err := RenderTrees(
		t.Context(),
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
