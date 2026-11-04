package scope_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2EEntryPrerequisiteDiffSidesName(t *testing.T, direction *struct {
	name     string
	current  string
	original string
	value    string
},
) {
	t.Helper()

	e2EEntryPrerequisiteDiffSidesNameLoop(t, struct {
		direction **struct {
			name     string
			current  string
			original string
			value    string
		}
	}{direction: &direction})
}

func e2EEntryPrerequisiteDiffSidesNameLoop(t *testing.T, loopInputs struct {
	direction **struct {
		name     string
		current  string
		original string
		value    string
	}
},
) {
	t.Helper()

	for _, command := range []string{"build", "diff"} {
		args := []string{
			command,
			"all",
			"--path",
			filepath.Join((*loopInputs.direction).current, "entry"),
			"--path-orig",
			filepath.Join((*loopInputs.direction).original, "entry"),
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
		}
		if command == "diff" {
			args = append(args, "-o", "json")
		}
		requireCLIOKResult1 := harness.RequireCLIOK(t, args...)
		out, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
		requireCLIOKResult2 := harness.RequireCLIOK(t, args...)
		again, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
		assert.Equal(t, again, out)
		if command == "build" {
			docs, err := testutil.SplitDocs([]byte(out))
			assert.NoError(t, err)
			found := map[string]bool{}
			e2EEntryPrerequisiteDiffSidesNameLoopLoop(t, struct {
				docs       *[]map[string]any
				loopInputs *struct {
					direction **struct {
						name     string
						current  string
						original string
						value    string
					}
				}
				found *map[string]bool
			}{docs: &docs, loopInputs: &loopInputs, found: &found})
			assert.FatalIf(t, len(found) != 2, "changed-only entry/prerequisite output missing: %s", out)
			continue
		}
		var result struct {
			Changes []struct {
				Kind   string `json:"Kind"`
				Name   string `json:"Name"`
				Status string `json:"Status"`
				Parent struct {
					Name string `json:"Name"`
				} `json:"Parent"`
			} `json:"Changes"`
		}
		assert.NoError(t, json.Unmarshal([]byte(out), &result))
		found := map[string]bool{}
		for _, change := range result.Changes {
			if change.Kind == manifest.KindConfigMap &&
				(change.Name == "entry-output" || change.Name == "prerequisite-output") {
				assert.Equal(t, change.Status, "changed")
				if change.Name == "prerequisite-output" {
					assert.Equal(t, change.Parent.Name, "sibling")
				}
				found[change.Name] = true
			}
		}
		assert.FatalIf(t, len(found) != 2, "diff lost entry/prerequisite output: %s", out)
	}
}

func e2EEntryPrerequisiteDiffSidesNameLoopLoop(t *testing.T, loopInputs struct {
	docs       *[]map[string]any
	loopInputs *struct {
		direction **struct {
			name     string
			current  string
			original string
			value    string
		}
	}
	found *map[string]bool
},
) {
	t.Helper()

	for _, doc := range *loopInputs.docs {
		metadata := manifest.DocMetadata(doc)
		name := metadata.Name
		if name == "entry-output" || name == "prerequisite-output" {
			assert.Equal(
				t,
				doc["data"].(map[string]any)["value"],
				any((*loopInputs.loopInputs.direction).value),
			)
			(*loopInputs.found)[name] = true
		}
	}
}
