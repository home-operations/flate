package manifest

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/home-operations/flate/internal/assert"
)

func TestObjectDocument_Ownership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, version, spec string
	}{
		{KindKustomization, FluxKustomizeDomain + "/v1", "path: ./old\npostBuild: {substitute: {releaseVersion: v1}}"},
		{KindHelmRelease, HelmReleaseDomain + "/v2", "chart: {spec: {chart: authored, sourceRef: {kind: GitRepository, name: source}}}\nvalues: {nested: {integer: 9007199254740993, fraction: 1.25}}"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			docs, err := SplitDocs([]byte("apiVersion: " + tc.version + "\nkind: " + tc.kind + "\nmetadata: {name: original, namespace: authored, labels: {}, annotations: {}}\nspec: {}\n"))
			assert.NoError(t, err)
			specs, err := SplitDocs([]byte(tc.spec))
			assert.NoError(t, err)
			docs[0]["spec"] = specs[0]
			obj, _, _, err := ParseDoc(docs[0], ParseDocOptions{})
			assert.NoError(t, err)
			if h, ok := obj.(*HelmRelease); ok {
				h.Chart.Name = "resolved"
				h.Values = map[string]any{"merged": true}
			}
			renderer := obj.(documentRenderer)
			rendered, err := renderer.RenderDocument()
			assert.NoError(t, err)
			before := DeepCopyMap(rendered)
			id := obj.Named()
			id.Name, id.Namespace = "captured", ""
			doc, err := ObjectDocument(id, obj)
			assert.NoError(t, err)
			meta := doc["metadata"].(map[string]any)
			assert.Equal(t, meta["name"], "captured")
			assert.Equal(t, meta["namespace"], "")
			for _, field := range []string{"labels", "annotations"} {
				if _, present := meta[field]; present {
					t.Fatalf("absent %s projected", field)
				}
			}
			meta["generation"] = int64(1)
			doc["status"] = map[string]any{"observedGeneration": int64(1)}
			meta["name"] = "overlay"
			again, err := ObjectDocument(obj.Named(), obj)
			assert.NoError(t, err)
			if _, present := again["metadata"].(map[string]any)["generation"]; present {
				t.Fatal("generation overlay mutated desired document")
			}
			unchanged, err := renderer.RenderDocument()
			assert.NoError(t, err)
			if diff := cmp.Diff(before, unchanged); diff != "" {
				t.Fatalf("readiness overlay changed generator document (-want +got):\n%s", diff)
			}
			var changed BaseManifest
			switch o := obj.(type) {
			case *Kustomization:
				clone := o.Clone()
				clone.Path = "./new"
				changed = clone
			case *HelmRelease:
				spec := doc["spec"].(map[string]any)
				want, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&helmv2.HelmRelease{Spec: o.HelmReleaseSpec})
				assert.NoError(t, err)
				if diff := cmp.Diff(want["spec"], spec); diff != "" {
					t.Fatalf("desired spec differs from upstream (-want +got):\n%s", diff)
				}
				values := spec["values"].(map[string]any)["nested"].(map[string]any)
				assert.Equal(t, values["integer"].(int64), int64(9007199254740993))
				assert.Equal(t, values["fraction"], 1.25)
				assert.Equal(t, spec["chart"].(map[string]any)["spec"].(map[string]any)["chart"], "authored")
				if _, present := spec["chartRef"]; present {
					t.Fatal("omitted chartRef projected")
				}
				clone := o.Clone()
				clone.HelmReleaseSpec.Chart.Spec.Chart = "new"
				changed = clone
			}
			changed = changed.WithNamespace("effective")
			changedDoc, err := ObjectDocument(changed.Named(), changed)
			assert.NoError(t, err)
			assert.Equal(t, changedDoc["metadata"].(map[string]any)["namespace"], "effective")
			if cmp.Equal(before["spec"], changedDoc["spec"]) {
				t.Fatal("clone projection retained old desired spec")
			}
			old, err := renderer.RenderDocument()
			assert.NoError(t, err)
			if diff := cmp.Diff(before, old); diff != "" {
				t.Fatalf("clone changed original document (-want +got):\n%s", diff)
			}
			nsCopy := obj.WithNamespace("copy")
			copyDoc, err := ObjectDocument(nsCopy.Named(), nsCopy)
			assert.NoError(t, err)
			assert.Equal(t, copyDoc["metadata"].(map[string]any)["namespace"], "copy")
			if diff := cmp.Diff(before["spec"], copyDoc["spec"]); diff != "" {
				t.Fatalf("namespace copy changed desired spec (-want +got):\n%s", diff)
			}
		})
	}
}
