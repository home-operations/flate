package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_ConcurrencyWork(t *testing.T) {
	t.Parallel()
	root, _ := writeMemoryCluster(t, 16)
	want := map[manifest.NamedResource]uint64{}
	for i := 1; i < 16; i += 2 {
		name := fmt.Sprintf("producer-%04d", i)
		group := fmt.Sprintf("group-%d", i/4)
		file := "flux/" + name + ".yaml"
		body, err := os.ReadFile(filepath.Join(root, file))
		assert.NoError(t, err)
		testutil.WriteFile(t, root, file, strings.ReplaceAll(string(body), "charts/shared", "charts/"+group))
		testutil.WriteFile(
			t,
			root,
			"charts/"+group+"/Chart.yaml",
			fmt.Sprintf("apiVersion: v2\nname: %s\nversion: 1.0.0\n", group),
		)
		testutil.WriteFile(
			t,
			root,
			"charts/"+group+"/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}'}\n",
		)
		want[testutil.HelmReleaseID("flux-system", name)] = 1
	}
	var output [2][]byte
	var logical [2]map[manifest.WorkPhase]map[manifest.NamedResource]uint64
	var preparation [2]map[manifest.WorkKey]manifest.WorkCounts
	t.Cleanup(func() {
		assert.Diff(t, output[1], output[0])
		assert.Diff(t, logical[1], logical[0])
		assert.Diff(t, preparation[1], preparation[0])
	})
	for index, workers := range []int{1, 8} {
		t.Run(
			fmt.Sprintf("concurrency_%d", workers),
			func(t *testing.T) {
				t.Parallel()
				runs, err := renderTreesForTest(t.Context(), t,
					[]Tree{
						{
							ID: manifest.TreeID{
								Cluster: ".",
								Side:    manifest.TreeHead,
							},
							RepoRoot: root,
							Path: filepath.Join(
								root,
								"flux",
							),
						},
					},
					Config{
						RecordWork:  true,
						Concurrency: workers,
					},
				)
				assert.NoError(t, err)
				result := runs[0].Result
				assert.Diff(t, witnessWork(result.Work, manifest.WorkHelm), want)
				preparation[index] = map[manifest.WorkKey]manifest.WorkCounts{}
				for recordIndex := range result.Work {
					record := result.Work[recordIndex]
					if record.Key.Phase == manifest.WorkPreparation {
						preparation[index][record.Key] = record.Counts
					}
				}
				assertChartDecodes(t, result, workers)
				data, err := json.Marshal(result.Manifests)
				assert.NoError(t, err)
				output[index] = data
				logical[index] = make(map[manifest.WorkPhase]map[manifest.NamedResource]uint64)
				for _, phase := range []manifest.WorkPhase{
					manifest.WorkAdmission,
					manifest.WorkKustomize,
					manifest.WorkHelm,
					manifest.WorkSubstitution,
					manifest.WorkPostRender,
				} {
					logical[index][phase] = witnessWork(result.Work, phase)
				}
			},
		)
	}
}

func assertChartDecodes(t *testing.T, result *Result, workers int) {
	t.Helper()
	var decodes uint64
	var peak uint64
	for recordIndex := range result.Work {
		record := result.Work[recordIndex]
		if record.Key.Phase == manifest.WorkChartDecode {
			assert.Equal(t, record.Counts.Attempts, uint64(1))
			assert.Equal(t, record.Counts.Completed, uint64(1))
			decodes += record.Counts.Attempts
			peak = max(peak, record.Counts.PeakConcurrent)
		}
	}
	assert.Equal(t, decodes, uint64(4))
	if peak == 0 || peak > uint64(workers) {
		t.Fatalf("actual chart decoder high-water=%d exceeds concurrency=%d", peak, workers)
	}
	t.Logf("chart keys=%d decodes=%d high-water=%d", 4, decodes, peak)
}
