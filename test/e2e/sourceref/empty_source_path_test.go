package sourceref_test

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2ESourceRefEmptyPathMatchesRootName(
	t *testing.T,
	fixture *func(t *testing.T, field string) (string, *store.Store, *discovery.Result),
	path *struct {
		name  string
		field string
	},
	capturedInputs struct {
		want     **discovery.Result
		wantDocs *[]map[string]any
	},
) {
	t.Helper()

	root, st, res := (*fixture)(t, path.field)
	parent := testutil.KustomizationID("flux-system", "root")
	child := testutil.KustomizationID("apps", "child")
	record, _ := res.Index.Lookup(child)
	assert.FatalIf(
		t,
		st.GetObject(child) == nil || record.Parent != parent,
		"source-root child not discovered or owned by root: parent=%v",
		record.Parent,
	)
	owners, _ := res.Index.OwnersOfFile("cm.yaml")
	assert.FatalIf(
		t,
		!slices.Equal(owners, []manifest.NamedResource{parent}),
		"source-root ConfigMap owners = %v; want [%v]",
		owners,
		parent,
	)
	//nolint:gocritic // The discovery iterator yields immutable records by value.
	for id, record := range res.Index.Files() {
		wantRecord, ok := (*capturedInputs.want).Index.Lookup(id)
		assert.FatalIf(t, !ok, "unexpected discovered resource %s", id)
		assert.Equal(t, record.File, wantRecord.File)
		assert.Equal(t, record.Parent, wantRecord.Parent)
	}
	for _, concurrency := range []string{"2", "8"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			args := []string{
				"--path",
				filepath.Join(root, "flux"),
				"--concurrency",
				concurrency,
				"--cache-dir",
				t.TempDir(),
			}
			owned := harness.RunCLIStdout(t, append([]string{"build", "ks", "root"}, args...)...)
			assert.FatalIf(
				t,
				!strings.Contains(owned, "name: root-owned") || !strings.Contains(owned, "value: source-root") ||
					!strings.Contains(
						owned,
						"namespace: apps",
					),
				"root's rendered objects missing or untransformed:\n%s",
				owned,
			)
			out := harness.RunCLIStdout(t, append([]string{"build", "all"}, args...)...)
			assert.FatalIf(
				t,
				!strings.Contains(out, "name: leaf-owned"),
				"child's rendered objects missing:\n%s",
				out,
			)
			docs, err := testutil.DecodeDocs(strings.NewReader(out))
			assert.NoError(t, err)
			for _, doc := range docs {
				name, _ := manifest.DocMetadata(doc)
				if manifest.DocKind(doc) == manifest.KindKustomization && name == "root" {
					spec := doc["spec"].(map[string]any)
					if path.name == "explicit" {
						assert.Equal(t, spec["path"], "./")
					} else if _, present := spec["path"]; present {
						t.Fatalf("omitted path was added to the root declaration: %s", out)
					}
					delete(spec, "path")
				}
			}
			assert.Diff(t, docs, (*capturedInputs.wantDocs))
		})
	}
}
