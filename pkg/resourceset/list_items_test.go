package resourceset_test

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_ListItems(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{Name: "list", Namespace: "default", ResourcesTemplate: `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: first}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: second}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: first}
- apiVersion: v1
  kind: ConfigMap
  metadata:
    name: excluded
    annotations: {fluxcd.controlplane.io/reconcile: disabled}`}
	docs, _, err := renderProviders(rs, nil, nil)
	{
		assert.NoError(t, err)
	}
	names := make([]string, 0, len(docs))
	for _, doc := range docs {
		name, ns := manifest.DocMetadata(doc.Document)
		names = append(names, name)
		{
			assert.ErrorIf(t, ns != "", "namespace = %q", ns)
		}
	}
	assert.Diff(t, names, []string{"first", "second"})
	for _, tc := range []struct {
		name, template string
		count          int
		wantErr        bool
	}{
		{"ordinary missing name", "apiVersion: v1\nkind: ConfigMap", 0, false},
		{"ordinary missing kind", "apiVersion: v1\nmetadata: {name: excluded}", 0, false},
		{"ordinary missing apiVersion", "kind: ConfigMap\nmetadata: {name: excluded}", 0, false},
		{"mixed case directive", "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: kUsToMiZaTiOn\nmetadata: {name: excluded}", 0, false},
		{"non directive group", "apiVersion: another.example/v1\nkind: Kustomization\nmetadata: {name: kept}", 1, false},
		{"items without suffix", "items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: item}}]", 1, false},
		{"suffix without items", "apiVersion: example.test/v1\nkind: WidgetList\nmetadata: {name: ordinary}", 1, false},
		{"typed incomplete items", "apiVersion: v1\nkind: ConfigMapList\nitems: [{metadata: {name: kindless}}, {apiVersion: v1, kind: ConfigMap}]", 2, false},
		{"nested list stays wrapped", "items: [{kind: List, items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]}]", 1, false},
		{"directive item survives", "items: [{apiVersion: kustomize.config.k8s.io/v1beta1, kind: Kustomization, metadata: {name: directive}}]", 1, false},
		{"invalid YAML", "metadata: [unterminated", 0, true},
		{"null member", "items: [null]", 0, true},
		{"scalar member", "items: [plain]", 0, true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testRenderListItemsCase(t, &tc) },
		)
	}
}

func testRenderListItemsCase(t *testing.T, testCaseInput *struct {
	name     string
	template string
	count    int
	wantErr  bool
},
) {
	t.Helper()

	candidate := &manifest.ResourceSet{
		Name:              "boundary",
		Namespace:         "apps",
		ResourcesTemplate: testCaseInput.template,
	}
	got, _, err := renderProviders(candidate, nil, nil)
	if testCaseInput.wantErr {
		{
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
				"error = %v, want classified malformed list error",
				err,
			)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, len(got), testCaseInput.count)
	if testCaseInput.name == "typed incomplete items" && len(got) == 2 {
		assert.Equal(t, manifest.DocKind(got[0].Document), "")
		assert.Equal(t, manifest.DocAPIVersion(got[0].Document), "")
		name, _ := manifest.DocMetadata(got[1].Document)
		assert.Equal(t, name, "")
	}
	if testCaseInput.name == "nested list stays wrapped" && len(got) == 1 {
		assert.Equal(t, manifest.DocKind(got[0].Document), "List")
		assert.Equal(t, len(got[0].Document["items"].([]any)), 1)
	}
}
