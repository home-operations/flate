package kustomize

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRender_UnidentifiedContent(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("first")},
	)
	cache := newTestTreeCache(t, t.TempDir())
	request := func() RenderRequest {
		source := localSource(t, root)
		source.Key, source.Revision, source.Digest = "family", "revision", "digest"
		return RenderRequest{Source: source, Label: root, SubPath: ".", Spec: demoSpec}
	}
	first, _, err := testutil.RenderMaps(Render(t.Context(), cache, request(), sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	{
		assert.FatalIf(t, !hasRenderField(first, "name", "first"), "%s", "missing first render")
	}
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("second"))
	second, _, err := testutil.RenderMaps(Render(t.Context(), cache, request(), sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			!hasRenderField(second, "name", "second"),
			"%s",
			"descriptor metadata pinned unidentified mutable bytes",
		)
	}
}
