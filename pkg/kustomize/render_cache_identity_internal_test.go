package kustomize

import (
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_CacheIdentityIsolated(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("fresh"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	key := renderKey(&RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "."}, root, nil)
	publishTestFrame(
		t,
		cache.owner,
		sourcecache.BlobKey{Content: struct {
			Render string `json:"Render"`
		}{key}},
		testRenderFrame(t, new(readSetSnapshot{}), []byte("ORDINARY\n")),
	)
	out, receipt20, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt20},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(out, "name", "fresh"),
			"ordinary raw identity reused cached output: %s, %v",
			out,
			err,
		)
	}
	_, stored, hit := readTestRenderFrame(t, cache, key)
	{
		assert.FatalIf(t, !hit || !reflect.DeepEqual(stored, out), "completed render = %s, %v", stored, hit)
	}
	bare := req
	bare.Spec = nil
	{
		assert.FatalIf(
			t,
			renderKey(&bare, root, nil) == renderKey(&req, root, nil),
			"%s",
			"bare and owned cache identities collide",
		)
	}
	first, _, err := testutil.RenderMaps(Render(t.Context(), cache, bare, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(t, err != nil || hasRenderField(first, manifest.FluxKustomizeDomain+"/name", "demo") ||
			!hasRenderField(first, "name", "fresh"), "bare render acquired owner metadata: %s, %v", first, err)
	}
	warm, _, err := testutil.RenderMaps(Render(t.Context(), cache, bare, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(t, err != nil || !reflect.DeepEqual(first, warm), "bare replay changed: %s, %v", warm, err)
	}
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("changed"))
	updated, _, err := testutil.RenderMaps(Render(t.Context(), cache, bare, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(updated, "name", "changed"),
			"bare cache missed input invalidation: %s, %v",
			updated,
			err,
		)
	}
	req.Spec = req.Spec.Clone()
	req.Spec.NamePrefix = "owned-"
	owned, _, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(t, err != nil || hasRenderField(owned, manifest.FluxKustomizeDomain+"/name", "demo") ||
			!hasRenderField(owned, "name", "owned-changed"), "owned render reused bare output: %s, %v", owned, err)
	}
}
