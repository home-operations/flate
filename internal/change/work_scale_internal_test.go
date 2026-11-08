package change

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDetect_PhysicalScaleWork(t *testing.T) {
	t.Parallel()
	for _, count := range []int{10, 100, 1000, 5000} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			roots := [2]string{t.TempDir(), t.TempDir()}
			inputs := [2]*store.SourceArtifact{}
			work := &manifest.WorkRecorder{}
			ctx := t.Context()
			index := &manifest.DiscoveryIndex{Readers: map[string][]manifest.FileReader{}}
			want := make(map[manifest.NamedResource]InputChange, count*2)
			paths := make([]string, count)
			for i := range count {
				file := fmt.Sprintf("file-%04d.yaml", i)
				paths[i] = file
				testutil.WriteFile(
					t,
					roots[1],
					file,
					fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm-%d}\n", i),
				)
				for _, suffix := range []string{"direct", "shared-reader"} {
					id := testutil.KustomizationID("apps", fmt.Sprintf("owner-%04d-%s", i, suffix))
					index.Readers[file] = append(
						index.Readers[file],
						manifest.FileReader{Reader: id, Source: manifest.BootstrapSourceID},
					)
					want[id] = InputChange{File: file}
				}
			}
			for side, root := range roots {
				testutil.WriteFile(
					t,
					root,
					"unchanged.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unchanged}\n",
				)
				inputs[side] = &store.SourceArtifact{
					Key:       fmt.Sprintf("side-%d", side),
					LocalPath: root,
					FS:        os.DirFS(root),
				}
			}
			unrelated := testutil.KustomizationID("apps", "unrelated")
			index.Readers["unchanged.yaml"] = []manifest.FileReader{
				{Reader: unrelated, Source: manifest.BootstrapSourceID},
			}
			changed, err := Detect(ctx, inputs[0], inputs[1], work.Scope())
			assert.NoError(t, err)
			assert.Diff(t, changed.Paths(), paths)
			original := maps.Clone(changed.inputs)
			for id, input := range original {
				input.Unbound = false
				want[id] = input
			}
			projected, err := changed.WithReaders(ctx,
				[2]*manifest.DiscoveryIndex{index, index},
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				work.Scope(),
			)
			assert.NoError(t, err)
			assert.Diff(t, projected.inputs, want)
			assert.Diff(t, changed.inputs, original)
			got := map[string]manifest.WorkCounts{}
			for _, record := range work.Scope().Snapshot(0).Normalize() {
				got[record.Key.Input] = record.Counts
			}
			assert.Diff(t, got, map[string]manifest.WorkCounts{
				"side-0":  {FileEntries: 2},
				"side-1":  {FileEntries: uint64(count + 2)},
				"readers": {FileEntries: uint64(count), ReaderEdges: uint64(2 * count)},
			})
			assertAbsentInputFiles(t, roots[0], paths)
			t.Logf("physical changes=%d changed owners=%d file visits=%d reader edges=%d bootstrap entries=4",
				count, 2*count, count, 2*count,
			)
		})
	}
}

func assertAbsentInputFiles(t *testing.T, root string, paths []string) {
	t.Helper()
	for _, file := range paths {
		if _, err := os.Stat(filepath.Join(root, file)); !os.IsNotExist(err) {
			t.Fatalf("changed file unexpectedly exists in BASE: %s: %v", file, err)
		}
	}
}
