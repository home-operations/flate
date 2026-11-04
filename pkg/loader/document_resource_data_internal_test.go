package loader

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_MixedKustomizationDataFile(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mixed", "mixed_producer", "duplicate_record", "duplicate_store_object"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexMixedKustomizationDataFileCase(
				t,
				&mode,
			)
		},
		)
	}
}

func testFinalizeIndexMixedKustomizationDataFileCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/ks.yaml",
		testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps}\n"),
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources: [bundle.yaml]\n")
	bundle := testutil.KustomizationYAML("child", "apps", "  {path: ./child}\n")
	if (*testCaseInput) == "mixed_producer" {
		bundle += "---\napiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer}\nspec: {target: {name: values}}\n"
	} else {
		bundle += `---
apiVersion: v1
kind: ConfigMap
metadata: {name: values, namespace: apps}
data: {value: fixture}
`
	}
	testutil.WriteFile(t, root, "apps/bundle.yaml", bundle)
	if strings.HasPrefix((*testCaseInput), "duplicate") {
		testutil.WriteFile(
			t,
			root,
			"flux/shadow.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {value: shadow}\n",
		)
	}
	l := loaderFixture(store.New(), nil)
	var existing *manifest.ConfigMap
	if (*testCaseInput) == "duplicate_store_object" {
		existing = &manifest.ConfigMap{
			Name:      "values",
			Namespace: "apps",
			Data:      map[string]any{"value": "shadow"},
		}
		l.Store.AddObject(existing)
	}
	l.PreferExisting = true
	l.SourceRoot = root
	for _, dir := range []string{"flux", "apps"} {
		{
			_, err := loadForTest(t.Context(), t, l, filepath.Join(root, dir))
			assert.NoError(t, err)
		}
	}
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	idx := l.Index
	owner := testutil.KustomizationID("flux-system", "owner")
	child := testutil.KustomizationID("apps", "child")
	values := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
	assert.FatalIf(
		t,
		existing != nil && l.Store.GetObject(values) != existing,
		"%s",
		"pre-existing data object was replaced",
	)
	{
		record, _ := l.Index.Lookup(child)
		assert.FatalIf(t, record.Parent != owner, "child parent = %s, want %s", record.Parent, owner)
	}
	if (*testCaseInput) == "mixed_producer" {
		{
			declarations := testutil.Declarations(
				l.Store.Productions(secretID("apps", "values"), idx),
			)
			assert.FatalIf(t, len(
				declarations,
			) == 0, "%s", "mixed-file raw producer is missing")
		}
	} else if record, _ := l.Index.Lookup(values); !slices.Contains(testutil.Owners(record.Productions), owner) {
		t.Fatalf("mixed-file data renderers = %v, want %s", testutil.Owners(record.Productions), owner)
	}
}
