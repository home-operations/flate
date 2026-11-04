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

func TestRenderTrees_ColdReleaseReach(t *testing.T) {
	t.Parallel()
	owner := testutil.KustomizationID("apps", "owner")
	release := testutil.HelmReleaseID("apps", "release")
	files := map[string]string{
		"flux/kustomization.yaml": "resources: [owner.yaml, unrelated.yaml]\n",

		"flux/owner.yaml": testutil.KustomizationYAML(
			"owner",
			"apps",
			"  path: ./apps/owner\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),

		"flux/unrelated.yaml": testutil.KustomizationYAML(
			"unrelated",
			"apps",
			"  path: ./apps/unrelated\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),

		"apps/owner/kustomization.yaml": "resources: [release.yaml]\n",

		"apps/owner/release.yaml": workWitnessFiles()["flux/release.yaml"],

		"apps/unrelated/kustomization.yaml": "resources: [cm.yaml]\n",

		"apps/unrelated/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated, namespace: apps}\n",

		"charts/demo/Chart.yaml": workWitnessFiles()["charts/demo/Chart.yaml"],

		"charts/demo/templates/cm.yaml": workWitnessFiles()["charts/demo/templates/cm.yaml"],
	}
	before := writeWitness(t, files)
	edited := maps.Clone(files)
	edited["apps/owner/release.yaml"] = strings.Replace(
		edited["apps/owner/release.yaml"],
		"value: old",
		"value: new",
		1,
	)
	after := writeWitness(t, edited)
	roots := []string{before, after}
	var scoped [2]*Result
	for side, root := range roots {
		runs, err := RenderTrees(
			t.Context(),
			[]Tree{
				witnessTree(
					root,
					manifest.TreeSide(
						side,
					),
				),
			},
			Config{
				RecordWork:  true,
				Concurrency: 2,
				Selection: manifest.Selection{
					Roots: []manifest.NamedResource{
						release,
					},
				},
			},
		)
		assert.NoError(t, err)
		scoped[side] = runs[0].Result
		assert.Diff(t, witnessWork(scoped[side].Work, manifest.WorkHelm), map[manifest.NamedResource]uint64{release: 1})
		assert.Diff(
			t,
			witnessWork(scoped[side].Work, manifest.WorkKustomize),
			map[manifest.NamedResource]uint64{owner: 1},
		)
	}
	var first []byte
	for repeat := range 2 {
		trees := []Tree{witnessTree(before, manifest.TreeBase), witnessTree(after, manifest.TreeHead)}
		runs, err := RenderTrees(t.Context(), trees, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		var decoded uint64
		for side, run := range runs {
			assert.Diff(
				t,
				witnessWork(run.Result.Work, manifest.WorkAdmission),
				witnessWork(scoped[side].Work, manifest.WorkAdmission),
			)
			assert.Diff(
				t,
				witnessWork(run.Result.Work, manifest.WorkKustomize),
				map[manifest.NamedResource]uint64{owner: 1},
			)
			assert.Diff(
				t,
				witnessWork(run.Result.Work, manifest.WorkHelm),
				map[manifest.NamedResource]uint64{release: 1},
			)
			assert.Diff(t, visibleWitnessOutput(run.Result), visibleWitnessOutput(scoped[side]))
			decoded += workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkChartDecode).Attempts
			for recordIndex := range run.Result.Work {
				record := run.Result.Work[recordIndex]
				assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
			}
		}
		assert.Equal(t, decoded, uint64(1))
		output := make([]map[string]any, 0, len(runs))
		for side, run := range runs {
			output = append(
				output,
				map[string]any{"side": side, "documents": outputByParent(run.Result.Manifests)[release]},
			)
		}
		encoded, err := json.Marshal(output)
		assert.NoError(t, err)
		if repeat == 0 {
			first = encoded
		} else {
			assert.Equal(t, string(encoded), string(first))
		}
	}
}
