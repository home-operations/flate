package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestSplitDocs(t *testing.T) {
	t.Parallel()
	data := []byte(`
apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: b, namespace: ns}
`)
	docs, err := splitDocMaps(data)
	{
		assert.FatalIf(t, err != nil, "SplitDocs: %v", err)
	}
	{
		assert.FatalIf(t, len(docs) != 2, "expected 2 docs, got %d", len(docs))
	}
	names := []string{
		docs[0]["metadata"].(map[string]any)["name"].(string),
		docs[1]["metadata"].(map[string]any)["name"].(string),
	}
	assert.Diff(t, names, []string{"a", "b"})
}

// TestSplitDocs_AliasAsMapKey pins the home-ops pattern of using a
// YAML anchor as a mapping key (e.g. `&app sonarr` in metadata.name,
// referenced later as `*app :` to share the literal across the doc).
// Sigs/Flux YAML parsers resolve the alias to its underlying scalar
// before assigning into the map; flate's manual node walk used to
// reject any non-scalar key kind. See m00nwtchr/homelab-cluster
// apps/media/sonarr/app/helmrelease.yaml.
func TestSplitDocs_AliasAsMapKey(t *testing.T) {
	t.Parallel()
	data := []byte(`
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: &app sonarr
spec:
  values:
    controllers:
      *app :
        replicas: 2
`)
	docs, err := splitDocMaps(data)
	{
		assert.FatalIf(t, err != nil, "SplitDocs: %v", err)
	}
	{
		assert.FatalIf(t, len(docs) != 1, "expected 1 doc, got %d", len(docs))
	}
	controllers, _ := docs[0]["spec"].(map[string]any)["values"].(map[string]any)["controllers"].(map[string]any)
	{
		_, ok := controllers["sonarr"]
		assert.ErrorIf(t, !ok, "alias key not resolved to scalar; controllers=%v", controllers)
	}
}
