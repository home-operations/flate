package manifest

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestObjectDocument_Ownership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, version, spec string
	}{
		{KindConfigMap, "v1", ""},
		{KindKustomization, FluxKustomizeDomain + "/v1", "path: ./old\npostBuild: {substitute: {releaseVersion: v1}}"},
		{KindHelmRelease, HelmReleaseDomain + "/v2", "chart: {spec: {chart: authored, sourceRef: {kind: GitRepository, name: source}}}\nvalues: {nested: {integer: 9007199254740993, fraction: 1.25}}"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			testObjectDocumentOwnershipCase(t, &tc)
		},
		)
	}
}

func TestExternalArtifact_RenderDocument(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		ref    *meta.NamespacedObjectKindReference
		inputs map[string]*apiextensionsv1.JSON
	}{
		{name: "absent"},
		{name: "local", ref: &meta.NamespacedObjectKindReference{Kind: "ArtifactGenerator", Name: "generator"}},
		{name: "qualified", ref: &meta.NamespacedObjectKindReference{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "sources"}},
		{name: "exported_inputs", inputs: map[string]*apiextensionsv1.JSON{
			"nested": {Raw: []byte(`{"list":[true,null,{"integer":9007199254740993}]}`)},
			"empty":  nil,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cr := sourcev1.ExternalArtifact{
				Name:        "output",
				Namespace:   "apps",
				Labels:      map[string]string{"capture": "value"},
				Annotations: map[string]string{"annotation": "value"},
				Spec:        sourcev1.ExternalArtifactSpec{SourceRef: tc.ref},
				Status:      sourcev1.ExternalArtifactStatus{ExportedInputs: tc.inputs},
			}
			want, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cr)
			assert.NoError(t, err)
			delete(want, "status")
			if tc.inputs != nil {
				want["status"] = map[string]any{"exportedInputs": map[string]any{
					"nested": map[string]any{
						"list": []any{true, nil, map[string]any{"integer": int64(9007199254740993)}},
					},
					"empty": nil,
				}}
			}
			delete(want["metadata"].(map[string]any), "creationTimestamp")
			want["kind"], want["apiVersion"] = KindExternalArtifact, sourceAPIVersion
			object := newExternalArtifact(cr)
			got, err := object.RenderDocument()
			assert.NoError(t, err)
			assert.Diff(t, got, want)
			parsed, err := ParseDoc(got, ParseDocOptions{})
			assert.NoError(t, err)
			again, err := parsed.Object.(*ExternalArtifact).RenderDocument()
			assert.NoError(t, err)
			assert.Diff(t, again, want)
			got["metadata"].(map[string]any)["labels"].(map[string]any)["capture"] = "changed"
			assert.Equal(t, object.Labels["capture"], "value")
		})
	}
}

func testObjectDocumentOwnershipCase(t *testing.T, testCaseInput *struct {
	kind    string
	version string
	spec    string
},
) {
	t.Helper()

	docs, err := splitDocMaps(
		[]byte(
			"apiVersion: " + testCaseInput.version + "\nkind: " + testCaseInput.kind + "\nmetadata: {name: original, namespace: authored, labels: {}, annotations: {}}\nspec: {}\n",
		),
	)
	assert.NoError(t, err)
	specs, err := splitDocMaps([]byte(testCaseInput.spec))
	assert.NoError(t, err)
	if testCaseInput.spec != "" {
		docs[0]["spec"] = specs[0]
	}
	parsed, err := ParseDoc(docs[0], ParseDocOptions{})
	obj := parsed.Object
	assert.NoError(t, err)
	if h, ok := obj.(*HelmRelease); ok {
		h.Chart.Name = "resolved"
		h.Values = map[string]any{"merged": true}
	}
	renderer, ok := obj.(documentRenderer)
	if !ok {
		id := obj.Named()
		id.Name, id.Namespace = "captured", ""
		want := map[string]any{
			"apiVersion": "", "kind": KindConfigMap,
			"metadata": map[string]any{"name": "captured", "namespace": ""},
		}
		doc, projectionErr := ObjectDocument(id, obj)
		assert.NoError(t, projectionErr)
		assert.Diff(t, doc, want)
		again, projectionErr := ObjectDocument(id, obj)
		assert.NoError(t, projectionErr)
		assert.Diff(t, again, want)
		doc["status"] = map[string]any{"observedGeneration": int64(1)}
		doc["metadata"].(map[string]any)["name"] = "overlay"
		doc["metadata"].(map[string]any)["generation"] = int64(1)
		assert.Diff(t, again, want)
		assert.Equal(t, obj.Named(), NamedResource{Kind: KindConfigMap, Name: "original", Namespace: "authored"})
		return
	}
	rendered, err := renderer.RenderDocument()
	assert.NoError(t, err)
	before := DeepCopyMap(rendered)
	id := obj.Named()
	id.Name, id.Namespace = "captured", ""
	doc, err := ObjectDocument(id, obj)
	assert.NoError(t, err)
	assert.Equal(t, DocIdentity(doc), id)
	assert.Equal(t, doc["apiVersion"].(string), testCaseInput.version)
	metadata := doc["metadata"].(map[string]any)
	assert.Equal(t, metadata["name"], "captured")
	assert.Equal(t, metadata["namespace"], "")
	for _, field := range []string{"labels", "annotations"} {
		{
			_, present := metadata[field]
			assert.FatalIf(t, present, "absent %s projected", field)
		}
	}
	metadata["generation"] = int64(1)
	doc["status"] = map[string]any{"observedGeneration": int64(1)}
	metadata["name"] = "overlay"
	again, err := ObjectDocument(obj.Named(), obj)
	assert.NoError(t, err)
	assert.Diff(t, again, before)
	{
		_, present := again["metadata"].(map[string]any)["generation"]
		assert.FatalIf(t, present, "%s", "generation overlay mutated desired document")
	}
	unchanged, err := renderer.RenderDocument()
	assert.NoError(t, err)
	{
		diff := cmp.Diff(before, unchanged)
		assert.FatalIf(t, diff != "", "readiness overlay changed generator document (-want +got):\n%s", diff)
	}
	var changed BaseManifest
	switch o := obj.(type) {
	case *Kustomization:
		clone := o.Clone()
		clone.Path = "./new"
		changed = clone
	case *HelmRelease:
		spec := doc["spec"].(map[string]any)
		want, toUnstructuredErr := runtime.DefaultUnstructuredConverter.ToUnstructured(
			&helmv2.HelmRelease{Spec: o.HelmReleaseSpec},
		)
		assert.NoError(t, toUnstructuredErr)
		{
			diff := cmp.Diff(want["spec"], spec)
			assert.FatalIf(t, diff != "", "desired spec differs from upstream (-want +got):\n%s", diff)
		}
		values := spec["values"].(map[string]any)["nested"].(map[string]any)
		assert.Equal(t, values["integer"].(int64), int64(9007199254740993))
		assert.Equal(t, values["fraction"], 1.25)
		assert.Equal(t, spec["chart"].(map[string]any)["spec"].(map[string]any)["chart"], "authored")
		{
			_, present := spec["chartRef"]
			assert.FatalIf(t, present, "%s", "omitted chartRef projected")
		}
		clone := o.Clone()
		clone.HelmReleaseSpec.Chart.Spec.Chart = "new"
		changed = clone
	}
	changed = changed.WithNamespace("effective")
	changedDoc, err := ObjectDocument(changed.Named(), changed)
	assert.NoError(t, err)
	assert.Equal(t, changedDoc["metadata"].(map[string]any)["namespace"], "effective")
	assert.FatalIf(
		t,
		cmp.Equal(before["spec"], changedDoc["spec"]),
		"%s",
		"clone projection retained old desired spec",
	)
	old, err := renderer.RenderDocument()
	assert.NoError(t, err)
	{
		diff := cmp.Diff(before, old)
		assert.FatalIf(t, diff != "", "clone changed original document (-want +got):\n%s", diff)
	}
	nsCopy := obj.WithNamespace("copy")
	copyDoc, err := ObjectDocument(nsCopy.Named(), nsCopy)
	assert.NoError(t, err)
	assert.Equal(t, copyDoc["metadata"].(map[string]any)["namespace"], "copy")
	{
		diff := cmp.Diff(before["spec"], copyDoc["spec"])
		assert.FatalIf(t, diff != "", "namespace copy changed desired spec (-want +got):\n%s", diff)
	}
}
