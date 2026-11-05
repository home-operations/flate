package reach

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:tparallel // The second cold request compares bytes from the completed first request.
func TestRenderTrees_ReachScale(t *testing.T) {
	t.Parallel()
	var cases []reachCase
	for _, tc := range reachCases() {
		if tc.fixture == "plex" {
			cases = append(cases, tc)
		}
	}
	controls, worlds := prepareReachWorlds(t, cases)
	for i := range worlds {
		world := &worlds[i]
		for side, root := range world.roots {
			world.roots[side] = reachScaleRoot(t, root, &world.spec)
		}
		controls[world.spec.name].root = world.roots[0]
	}
	cfg := reachConfig()
	oracle := reachOracle{cfg: &cfg, controls: controls, worlds: worlds}
	oracle.render(t)
	cfg.RecordWork = true
	for pass, name := range []string{"control", "repeat"} {
		t.Run(name, func(t *testing.T) {
			for i := range worlds {
				t.Run(worlds[i].spec.name, func(t *testing.T) {
					t.Parallel()
					encoded, _ := runReachWorld(t, &worlds[i], &cfg)
					if pass == 0 {
						worlds[i].first = encoded
					} else {
						assert.Equal(t, encoded, worlds[i].first)
					}
				})
			}
		})
	}
}

func reachScaleRoot(t *testing.T, root string, tc *reachCase) string {
	t.Helper()
	excluded := make(map[string]bool)
	for _, id := range tc.admissions {
		name, kustomization := strings.CutPrefix(id, "Kustomization.kustomize.toolkit.fluxcd.io/")
		if kustomization && !slices.Contains(tc.builders, id) {
			excluded["kubernetes/apps/"+name+"/app"] = true
		}
	}
	files := make(map[string]string)
	assert.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		relative, relativeErr := filepath.Rel(root, path)
		if relativeErr != nil {
			return relativeErr
		}
		relative = filepath.ToSlash(relative)
		text := string(body)
		if excluded[filepath.ToSlash(filepath.Dir(relative))] {
			if filepath.Base(relative) != "kustomization.yaml" {
				return nil
			}
			text = "resources: []\n"
		}
		if relative == "kubernetes/apps/excluded.yaml" {
			text = strings.ReplaceAll(text, "  suspend: true\n", "")
		}
		files[relative] = text
		return nil
	}))
	return testutil.WriteTree(t, files)
}
