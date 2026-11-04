package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOrchestrator_Render_ResourceSetEffectiveOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		competitor bool
	}{
		{name: "both-producers", competitor: true},
		{name: "sole-producer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOrchestratorRenderResourceSetEffectiveOwnershipCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestOrchestrator_Render_ResourceSetDocumentBoundary(t *testing.T) {
	t.Parallel()
	source, err := filepath.Abs("../../testdata/resourceset-documents")
	assert.NoError(t, err)
	path := t.TempDir()
	testutil.CopyTree(t, source, path)
	testOrchestratorRenderResourceSetDocumentBoundarySegment(t, &path)
}

func testOrchestratorRenderResourceSetEffectiveOwnershipCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		competitor bool
	},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, root, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system",
		"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [rs.yaml]\n")
	resources := `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: aaa, namespace: flux-system}
spec:
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: foo}
      data: {marker: aaa-omitted}
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: foo, namespace: flux-system}
      data: {marker: aaa-explicit}
`
	if testCaseInput.competitor {
		resources += `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: zzz, namespace: flux-system}
spec:
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: foo, namespace: flux-system}
      data: {marker: zzz-winner}
`
	}
	testutil.WriteFile(t, root, "apps/rs.yaml", resources)
	want, err := testutil.DecodeDocs(strings.NewReader(`apiVersion: v1
kind: ConfigMap
metadata: {name: foo}
data: {marker: aaa-omitted}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: foo, namespace: flux-system}
data: {marker: aaa-explicit}
`))
	assert.NoError(t, err)
	if testCaseInput.competitor {
		want = []map[string]any{
			{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "foo", "namespace": "flux-system"},
				"data":       map[string]any{"marker": "zzz-winner"},
			},
		}
	}
	owner := "aaa"
	if testCaseInput.competitor {
		owner = "zzz"
	}
	for _, doc := range want {
		doc["metadata"].(map[string]any)["labels"] = map[string]any{
			"resourceset.fluxcd.controlplane.io/name":      owner,
			"resourceset.fluxcd.controlplane.io/namespace": "flux-system",
		}
	}
	testOrchestratorRenderResourceSetEffectiveOwnershipCaseTail(
		t,
		&testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs{
			root:          &root,
			testCaseInput: &testCaseInput,
			want:          &want,
		},
	)
}

func testOrchestratorRenderResourceSetEffectiveOwnershipCaseTail(
	t *testing.T,
	tailCaseInput *testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs,
) {
	t.Helper()
	testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailSegment(t, tailCaseInput)
}

type testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs struct {
	root *string

	testCaseInput **struct {
		name string

		competitor bool
	}

	want *[]map[string]any
}

func testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailSegment(
	t *testing.T,
	tailCaseInput *testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs,
) {
	t.Helper()
	var previous map[manifest.NamedResource][]map[string]any
	for range 2 {
		renderedSingleton2, err := RenderTrees(
			t.Context(),
			[]Tree{{Path: (*tailCaseInput.root), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
			Config{Concurrency: 2},
		)
		assert.FatalIf(t, len(renderedSingleton2) != 1, "missing render: %v", err)
		result := renderedSingleton2[0].Result
		assert.NoError(t, err)
		assert.Equal(t, len(result.Failed), 0)
		var got []map[string]any
		for parent, docs := range outputByParent(result.Manifests) {
			for _, doc := range docs {
				if manifest.DocKind(doc) == manifest.KindConfigMap {
					assert.Equal(t, parent, testutil.KustomizationID("flux-system", "apps"))
					got = append(got, doc)
				}
				tailCaseInput.assertLosingMarkers(t, doc)
			}
		}
		assert.Diff(t, got, (*tailCaseInput.want))
		if !(*tailCaseInput.testCaseInput).competitor && len(got) == 2 {
			_, present := got[0]["metadata"].(map[string]any)["namespace"]
			assert.Equal(t, present, false)
		}
		if previous != nil {
			assert.Diff(t, outputByParent(result.Manifests), previous)
		}
		previous = outputByParent(result.Manifests)
	}
}

func testOrchestratorRenderResourceSetDocumentBoundarySegment(
	t *testing.T,
	path *string,
) {
	t.Helper()
	var previous map[manifest.NamedResource][]map[string]any
	for range 2 {
		renderedSingleton3, err := RenderTrees(
			t.Context(),
			[]Tree{{Path: (*path), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
			Config{Concurrency: 2},
		)
		assert.FatalIf(t, len(renderedSingleton3) != 1, "missing render: %v", err)
		o := renderedSingleton3[0].Orchestrator
		result := renderedSingleton3[0].Result
		assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux) || len(result.Failed) != 1,
			"required malformed output = %v, failures=%v", err, result.Failed)

		effective := manifest.NamedResource{Group: "apps", Kind: "Deployment", Namespace: "flux-system", Name: "raw"}
		{
			child := o.Store().GetObject(effective)
			assert.FatalIf(t, child == nil, "missing effective child %s", effective)
		}

		counts := map[string]int{}
		for _, docs := range outputByParent(result.Manifests) {
			for _, doc := range docs {
				recordResourceSetDocumentBoundary(t, doc, counts)
			}
		}
		for key, want := range map[string]int{"raw": 1, "duplicate": 2, "nameless-json": 1, "nameless-list": 1, "kindless-list": 1, "wrapper": 2, "directive": 2, "explicit": 1, "sanitized": 1, "nested": 0, "item-directive": 1, "child-result": 1, "rejected-yaml": 0, "rejected-directive": 0, "child": 0} {
			assert.ErrorIf(t, counts[key] != want,
				"%s count=%d, want %d; all=%v", key, counts[key], want, counts)
		}
		if previous != nil {
			assert.Diff(t, outputByParent(result.Manifests), previous)
		}
		previous = outputByParent(result.Manifests)
	}
}

func (s *testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs) assertLosingMarkers(
	t *testing.T,
	doc map[string]any,
) {
	t.Helper()
	if (*s.testCaseInput).competitor && manifest.DocKind(doc) == manifest.KindConfigMap {
		encoded, err := json.Marshal(doc)
		assert.NoError(t, err)
		assert.ErrorIf(t, strings.Contains(string(encoded), "aaa-omitted") ||
			strings.Contains(
				string(encoded),
				"aaa-explicit",
			), "losing marker in output: %s", encoded)
	}
}

func recordResourceSetDocumentBoundary(t *testing.T, doc map[string]any, counts map[string]int) {
	t.Helper()
	metadata := manifest.DocMetadata(doc)
	name, ns := metadata.Name, metadata.Namespace
	if name == "raw" || name == "duplicate" {
		assert.Equal(t, ns, "")
	}
	if name == "explicit" {
		assert.Equal(t, ns, "other")
	}
	if name == "sanitized" {
		assert.Diff(t, doc["data"], map[string]any{})
	}
	if name == "kindless-list" {
		assert.Equal(t, manifest.DocKind(doc), "")
		assert.Equal(t, manifest.DocAPIVersion(doc), "")
	}
	counts[name]++
	if data, ok := doc["data"].(map[string]any); ok {
		counts[fmt.Sprint(data["value"])]++
	}
	if spec, ok := doc["spec"].(map[string]any); ok {
		counts[fmt.Sprint(spec["value"])]++
	}
	if manifest.DocKind(doc) == "List" {
		counts["wrapper"]++
	}
	if manifest.DocAPIVersion(doc) == "kustomize.config.k8s.io/v1beta1" {
		counts["directive"]++
	}
}
