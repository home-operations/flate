package helm

import (
	"errors"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testTemplateCacheSubchartPersistenceCase(
	t *testing.T,
	testCaseInput *testTemplateCacheSubchartPersistenceCaseInputs,
) {
	t.Helper()
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"

	files := fstest.MapFS{}
	assert.NoError(
		t,
		fs.WalkDir(os.DirFS((*testCaseInput.dir)), ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(os.DirFS((*testCaseInput.dir)), name)
			files[name] = &fstest.MapFile{Data: data}
			return err
		}),
	)
	if (*testCaseInput.field) == "template" {
		files["charts/child/templates/cm.yaml"].Data = []byte(
			strings.ReplaceAll(
				string(files["charts/child/templates/cm.yaml"].Data),
				"marker:",
				"edited-template:",
			),
		)
	} else {
		files["charts/child/values.yaml"].Data = []byte("enabled: true\nmarker: edited-default\n")
	}
	updated := acquireChartFS(t, cachetest.New(t, t.TempDir(), cache.New), chartFixtureFS((*testCaseInput.hr), files))
	nextStore := store.New()
	nextStore.AddObject(&manifest.OCIRepository{Name: "podinfo", Namespace: "apps"})
	nextStore.SetArtifact((*testCaseInput.hr).ChartSource(), &store.SourceArtifact{
		Kind: manifest.KindOCIRepository, FS: updated, Digest: digest, Revision: "6.15.0@" + digest,
	})
	next := NewClient((*testCaseInput.owner))
	nextSources := NewSources(nextStore)
	nextKey := (*testCaseInput.keyFor)(t, next, nextSources)
	{
		assert.FatalIf(
			t,
			nextKey.Content == testCaseInput.key.Content,
			"%s",
			"edited child reused the original completed-output key",
		)
	}
	_, _, _, err := (*testCaseInput.owner).Blob(t.Context(), nextKey, cache.Policy{MaxAge: cache.Forever}, nil)
	{
		assert.FatalIf(
			t,
			!errors.Is(err, errdef.ErrNotFound),
			"edited child lookup: got %v, want not found",
			err,
		)
	}
	out, receipt10, err := testutil.RenderMaps(
		next.Template(t.Context(), (*testCaseInput.hr), nil, RenderOptions{Options: Options{}, Sources: nextSources}),
	)
	if err == nil {
		{
			recordErr := next.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: (*testCaseInput.hr).Named(), Receipt: receipt10, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			reflect.DeepEqual(out, (*testCaseInput.cold)) || hasRenderField(out, "witness", "persisted-subchart-hit") ||
				!hasRenderField(out, "edited-template", "original") &&
					!hasRenderField(
						out,
						"marker",
						"edited-default",
					),
			"%s",
			"edited child reused stale output or omitted its new content",
		)
	}
	assert.Diff(t, (*testCaseInput.readBlob)(t, (*testCaseInput.owner), nextKey), out)
	assert.Diff(t, (*testCaseInput.readBlob)(t, (*testCaseInput.owner), (*testCaseInput.key)), (*testCaseInput.marked))
	t.Log(
		"reacquired child content invalidated the key and rendered fresh documents with fixed parent/release/OCI identity",
	)
}
