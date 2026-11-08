package kustomize

import (
	"io/fs"
	"os"
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
)

func testRenderInputOriginsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	files := map[string]string{
		"base/cm.yaml":            cmYAML("base"),
		"base/kustomization.yaml": existingKustomization("./cm.yaml"),
		"app/kustomization.yaml": existingKustomization(
			"../base",
			"./local.yaml",
		),
		"app/local.yaml":            cmYAML("local"),
		"generated/a space+@é.yaml": cmYAML("exotic"),
	}
	root := testutil.WriteTree(t, files)
	lower := memoryTree(t, files)
	spec := ksDoc(map[string]any{"namePrefix": "merged-"})
	want, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     (*testCaseInput),
				Spec:        parsedKS(t, spec),
			},
			sourcecache.RetryConfig{},
		),
	)
	assert.NoError(t, err)
	cache := newTestTreeCache(t, t.TempDir())
	source := &store.SourceArtifact{FS: lower, Key: "pin+rules+options+auth", LocalPath: root}
	for _, state := range []string{"cold", "warm", "host-absent"} {
		if state == "host-absent" {
			{
				err := os.RemoveAll(root)
				assert.NoError(t, err)
			}
		}
		got, _, err := testutil.RenderMaps(
			Render(
				t.Context(),
				cache,
				RenderRequest{Source: source, SubPath: (*testCaseInput), Spec: parsedKS(t, spec)},
				sourcecache.RetryConfig{},
			),
		)
		assert.FatalIf(
			t,
			err != nil || !reflect.DeepEqual(got, want),
			"%s memory output differs: %s, %v",
			state,
			got,
			err,
		)
	}
	if (*testCaseInput) == "app" {
		got, err := fs.ReadFile(lower, "app/kustomization.yaml")
		assert.FatalIf(
			t,
			err != nil || string(got) != files["app/kustomization.yaml"],
			"%s",
			"generated manifest mutated source",
		)
	}
}
