package helm

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/cli-runtime/pkg/resource"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestTemplate_EmittedLists(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input string
		wantError   bool
	}{
		{"generic", `apiVersion: v1
kind: List
items:
- {apiVersion: v1, kind: ConfigMap, metadata: {name: a}}
- {apiVersion: v1, kind: Secret, metadata: {name: b}}
`, false},
		{"typed both absent", "apiVersion: v1\nkind: ConfigMapList\nitems: [{metadata: {name: bare}}]\n", false},
		{"typed secret", "apiVersion: v1\nkind: SecretList\nitems: [{metadata: {name: secret}}]\n", false},
		{"missing kind", "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, metadata: {name: partial}}]\n", true},
		{"missing version", "apiVersion: v1\nkind: ConfigMapList\nitems: [{kind: ConfigMap, metadata: {name: partial}}]\n", true},
		{"nested", `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: List
  items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]
- {apiVersion: v1, kind: ConfigMap, metadata: {name: sibling}}
`, true},
		{"empty", "apiVersion: v1\nkind: ConfigMapList\nitems: []\n", false},
		{"null items", "apiVersion: v1\nkind: ConfigMapList\nitems: null\n", false},
		{"null member", "apiVersion: v1\nkind: ConfigMapList\nitems: [null]\n", false},
		{"scalar member", "apiVersion: v1\nkind: ConfigMapList\nitems: [scalar]\n", true},
		{"object items", "apiVersion: v1\nkind: ConfigMapList\nitems: {metadata: {name: bad}}\n", true},
		{"scalar items", "apiVersion: v1\nkind: ConfigMapList\nitems: scalar\n", true},
		{"custom kind", "apiVersion: example.test/v1\nkind: Bundle\nmetadata: {name: bundle}\nitems: [{apiVersion: v1, kind: ConfigMap, metadata: {name: child}}]\n", false},
		{"scalar AllowList", "apiVersion: example.test/v1\nkind: AllowList\nmetadata: {name: acl}\nitems: [scalar]\n", true},
		{"ordinary AllowList", "apiVersion: example.test/v1\nkind: AllowList\nmetadata: {name: acl}\nspec: {items: [scalar]}\n", false},
		{"constructed scalars", `apiVersion: v1
kind: List
items:
- apiVersion: example.test/v1
  kind: Example
  metadata: {name: nested-keys}
  spec:
    mapping: {true: value}
    string: 'yes'
    date: '2001-01-01'
    number: 9007199254740993
`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testTemplateEmittedListsCase(t, &tc)
		},
		)
	}
}

func TestNormalizeLists_OwnershipAndOrder(t *testing.T) {
	t.Parallel()
	input := []byte(`apiVersion: v1
kind: ConfigMap
metadata: {name: before}
---
apiVersion: v1
kind: ConfigMapList
items: []
---
apiVersion: v1
kind: ConfigMapList
items:
- metadata: {name: expanded}
  data: {key: value}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: after}
`)
	docs, err := manifest.DecodeDocs(bytes.NewReader(input), manifest.RequiredDocuments)
	assert.NoError(t, err)
	snapshot, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	out := normalizeLists(docs)
	names := make([]string, 0, len(out))
	for _, doc := range out {
		metadata := manifest.DocMetadata(doc.Document)
		name := metadata.Name
		names = append(names, name)
	}
	assert.Diff(t, names, []string{"before", "expanded", "after"})
	ordinals := make([]int, len(out))
	for i, doc := range out {
		ordinals[i] = doc.Ordinal
	}
	assert.Diff(t, ordinals, []int{0, 2, 3})
	cached, err := manifest.DecodeRender(snapshot)
	assert.NoError(t, err)
	replayed := normalizeLists(cached)
	assert.Diff(t, replayed, out)

	out[1].Document["data"].(map[string]any)["key"] = "changed"
	unchanged, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	assert.Diff(t, unchanged, snapshot)
	out[0].Document["retained"] = true
	assert.Equal(t, docs[0].Document["retained"], any(true))
	out[2].Document["retained"] = true
	assert.Equal(t, docs[3].Document["retained"], any(true))
}

//nolint:paralleltest // allocation measurements require serial execution.
func TestNormalizeLists_NoItems(t *testing.T) {
	docs := testutil.DocumentOccurrences(
		[]map[string]any{{"kind": "AllowList", "spec": map[string]any{"items": []any{"value"}}}, {"kind": "ConfigMap"}},
	)
	out := normalizeLists(docs)
	assert.Equal(t, &out[0] == &docs[0], true)
	allocations := testing.AllocsPerRun(100, func() {
		out = normalizeLists(docs)
	})
	assert.Equal(t, allocations, float64(0))
}

func TestTemplate_PostRenderedLists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/list.yaml", `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: List
  items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]
- {apiVersion: v1, kind: ConfigMap, metadata: {name: sibling}}
`)
	hr := newHR()
	hr.PostRenderers = []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{}}}
	out, _, err := NewClient(
		cachetest.New(t, "", sourcecache.New),
	).Template(t.Context(), hr, nil, RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
	assert.NoError(t, err)
	names := make([]string, 0, len(out))
	for _, doc := range out {
		metadata2 := manifest.DocMetadata(doc.Document)
		name := metadata2.Name
		names = append(names, name)
	}
	assert.Diff(t, names, []string{"nested", "sibling"})
}

func TestNormalizeLists_RetainsFailedOutcome(t *testing.T) {
	t.Parallel()
	docs, err := manifest.DecodeDocs(strings.NewReader(`apiVersion: v1
kind: ConfigMap
metadata: {name: ordinary}
---
apiVersion: v1
kind: ConfigMapList
items:
- {apiVersion: v1, kind: ConfigMap, metadata: {name: valid}}
- {apiVersion: v1, metadata: {name: invalid}}
`), manifest.RequiredDocuments)
	assert.NoError(t, err)
	before, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	out := normalizeLists(docs)
	failure := manifest.DocumentFailure(out)
	assert.FatalIf(t, len(out) != 2 || out[0].Err != nil || out[1].Ordinal != 1 || out[1].Document == nil ||
		!errors.Is(failure, manifest.ErrInput) ||
		!errors.Is(failure, manifest.ErrFlux), "invalid stream lost its outcome: %#v, %v", out, failure)
	after, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	assert.Diff(t, after, before)
}

func TestTemplate_RetainsFailedStream(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"mychart/templates/output.yaml",
		"null\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: invalid}\ndata: {key: first, key: duplicate}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling}\n",
	)
	client := NewClient(cachetest.New(t, "", sourcecache.New))
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	var diagnostic string
	for _, phase := range []string{"cold", "repeat"} {
		docs, receipt, err := client.Template(t.Context(), newHR(), nil, input)
		assert.NoError(t, err)
		assert.FatalIf(t, len(docs) != 2 || docs[0].Ordinal != 0 || docs[0].Err == nil || docs[0].Document != nil ||
			docs[1].Ordinal != 1 ||
			manifest.DocIdentity(docs[1].Document).Name != "sibling" ||
			receipt != nil, "%s lost outcomes or cached subset: %+v, %v", phase, docs, receipt)
		if diagnostic == "" {
			diagnostic = manifest.DocumentFailure(docs).Error()
		}
		assert.Equal(t, manifest.DocumentFailure(docs).Error(), diagnostic)
	}
}

func testTemplateEmittedListsCase(t *testing.T, testCaseInput *struct {
	name      string
	input     string
	wantError bool
},
) {
	t.Helper()

	infos, oracleErr := resource.NewLocalBuilder().
		ContinueOnError().
		Unstructured().
		Flatten().
		Stream(strings.NewReader(testCaseInput.input), "").
		Do().
		Infos()
	assert.FatalIf(
		t,
		(oracleErr != nil) != testCaseInput.wantError,
		"upstream error = %v, wantError = %t",
		oracleErr,
		testCaseInput.wantError,
	)
	want := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		want = append(want, info.Object.(*unstructured.Unstructured).Object)
	}
	if testCaseInput.name == "ordinary AllowList" {
		var err error
		want, err = testutil.SplitDocs([]byte(testCaseInput.input))
		assert.NoError(t, err)
	}
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/list.yaml", testCaseInput.input)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	docs, _, err := testutil.RenderMaps(
		cli.Template(
			t.Context(),
			newHR(),
			nil,
			RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)},
		),
	)
	if oracleErr != nil {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
			!strings.Contains(
				err.Error(),
				oracleErr.Error(),
			), "Template error = %v, want upstream: %v", err, oracleErr)
		assert.FatalIf(t, docs != nil, "failed producer returned partial output: %#v", docs)
		return
	}
	assert.NoError(t, err)
	assert.Diff(t, docs, want)
}
