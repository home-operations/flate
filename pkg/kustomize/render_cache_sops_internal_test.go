package kustomize

import (
	"bytes"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRender_SOPSDiagnosticsOnReplay(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": "resources: [secret.yaml]\n",
		"secret.yaml":        "apiVersion: v1\nkind: Secret\nmetadata: {name: input, namespace: apps}\nstringData: {VALUE: '" + cipher + "', PLAIN: readable}\nsops: {mac: encrypted}\n",
	})
	storage := t.TempDir()
	cache := newTestTreeCache(t, storage)
	spec := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
	req := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	var previous []byte
	for i := range 3 {
		if i == 2 {
			cache = newTestTreeCache(t, storage)
		}
		docs, receipt, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
		assert.NoError(t, err)
		assert.NoError(
			t,
			cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: spec.Named(), Receipt: receipt, Records: emptyRenderInventory},
			),
		)
		_, raw, hit := readTestRenderFrame(t, cache, receipt.Value().(renderFrame).header.Key)
		assert.FatalIf(
			t,
			!hit || len(raw) != 1 || raw[0]["stringData"].(map[string]any)["VALUE"] != cipher,
			"%s",
			"raw replay frame lost admission input",
		)

		assert.NoError(t, Shape(spec, docs, false))
		assert.Equal(t, len(docs[0].Warnings), 1)
		assert.Diff(t, docs[0].Warnings[0].Detail, []string{"VALUE"})
		assert.Diff(t, docs[0].Document["stringData"], map[string]any{"PLAIN": "readable"})
		encoded, err := manifest.EncodeRender(docs)
		assert.NoError(t, err)
		if i != 0 {
			assert.Equal(t, bytes.Equal(previous, encoded), true)
		}
		previous = encoded
	}
}
