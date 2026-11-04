package manifest

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/home-operations/flate/internal/assert"
)

func TestRenderDocument_SequentialClone(t *testing.T) {
	t.Parallel()
	ks, err := parseKustomization(mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps}
spec: {path: ./p0}
`))
	assert.NoError(t, err)
	first, err := ks.RenderDocument()
	assert.NoError(t, err)
	snapshot := DeepCopyMap(first)
	clone := ks.Clone()
	clone.Path = "./p1"
	second, err := clone.RenderDocument()
	assert.NoError(t, err)
	assert.Equal(t, second["spec"].(map[string]any)["path"], any("./p1"))
	assert.Diff(t, first, snapshot)
	repeated, err := ks.RenderDocument()
	assert.NoError(t, err)
	assert.Diff(t, repeated, snapshot)
}

//nolint:paralleltest // The test replaces a package variable shared by other tests.
func TestRenderDocument_BufferOwnership(t *testing.T) {
	factory := kustomizationEncoders.New
	t.Cleanup(func() { kustomizationEncoders = sync.Pool{New: factory} })
	var encoder *kustomizationEncoder
	kustomizationEncoders = sync.Pool{New: func() any {
		encoder = factory().(*kustomizationEncoder)
		return encoder
	}}
	ks, err := parseKustomization(mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps}
spec: {path: ./p0}
`))
	assert.NoError(t, err)
	_, err = ks.RenderDocument()
	assert.NoError(t, err)
	{
		assert.FatalIf(t, encoder == nil || encoder.buffer.Len() == 0, "%s", "render did not use an encoder buffer")
	}
	published := ks.projection.Load().raw
	snapshot := bytes.Clone(published)
	encoder.buffer.Bytes()[0] ^= 1
	assert.Diff(t, published, snapshot)
}

func TestRenderDocument_TypedSpec(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, spec string }{
		{"absent", "{}"},
		{"null fields", "{targetNamespace: null, namePrefix: null, nameSuffix: null, patches: null, images: null, components: null, buildMetadata: null}"},
		{"empty fields", "{targetNamespace: '', namePrefix: '', nameSuffix: '', patches: [], images: [], components: [], buildMetadata: []}"},
		{"zero elements", "{patches: [null], images: [null], components: [null], buildMetadata: [null]}"},
		{"metadata values", "{commonMetadata: {labels: {app: null, team: typed}, annotations: {note: null, owner: typed}}}"},
		{"generator-only fields", "{patchesStrategicMerge: [{metadata: {name: ignored}}], patchesJson6902: not-a-list}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(
				t,
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: apps}\nspec: "+tc.spec+"\n",
			)
			raw, err := json.Marshal(doc)
			assert.NoError(t, err)
			var cr kustomizev1.Kustomization
			assert.NoError(t, json.Unmarshal(raw, &cr))
			want, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cr)
			assert.NoError(t, err)
			ks, err := parseKustomization(doc)
			assert.NoError(t, err)
			for range 2 {
				got, err := ks.RenderDocument()
				assert.NoError(t, err)
				assert.Diff(t, got["spec"], want["spec"])
			}
		})
	}
}
