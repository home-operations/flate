package selection

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_SourceCoordinates(t *testing.T) {
	t.Parallel()
	for _, origin := range []string{"/workspace/selected", "/workspace", "/mounted"} {
		t.Run(origin, func(t *testing.T) {
			t.Parallel()
			root := "/workspace/selected"
			if origin == "/mounted" {
				root = "/mounted/selected"
			}
			file := "input.txt"
			if origin != root {
				file = "selected/input.txt"
			}
			var sources [2]*store.SourceArtifact
			for side := range sources {
				sources[side] = &store.SourceArtifact{
					LocalPath: "/mounted", LocalRoot: origin, InputAlias: true,
					FS: fstest.MapFS{file: {Data: fmt.Appendf(nil, "%d", side)}},
				}
			}
			changes, err := change.Detect(t.Context(), sources[0], sources[1], manifest.WorkScope{})
			assert.NoError(t, err)
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, sources[1])
			toIndex, err := filepath.Rel(root, origin)
			assert.NoError(t, err)
			facts := &solverFacts{
				sources: map[manifest.NamedResource]SourceFacts{
					manifest.BootstrapSourceID: {
						Source:  manifest.BootstrapSourceID,
						Context: SourceContext{Root: root},
						Delta:   changes,
						Input:   sources[1],
						Origin:  origin,
						ToIndex: toIndex,
						Files:   []SourceFile{{Path: file, Indexed: "input.txt"}},
						Paired:  true,
					},
				},
			}
			c := solverClosure(t, st, facts)
			c.context.Root = root
			coordinates2 := c.coordinates(
				new(manifest.Production{
					Source: manifest.BootstrapSourceID,
					File:   file,
					Path:   filepath.Join(sources[1].LocalPath, file),
				}),
			)
			relative, indexed := coordinates2.relative, coordinates2.indexed
			assert.Equal(t, relative, file)
			assert.Equal(t, indexed, "input.txt")
			assert.Equal(t, c.fileChanged(manifest.BootstrapSourceID, "input.txt"), true)
			assert.Diff(t, slices.Collect(c.sourceFiles(manifest.BootstrapSourceID)), []string{"input.txt"})
		})
	}
}
