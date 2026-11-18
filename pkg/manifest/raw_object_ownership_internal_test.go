package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestRawObject_DoesNotAliasParsedDoc(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "foo"},
		"spec":       map[string]any{"key": "original"},
	}
	obj, err := parseRawObject(doc)
	assert.FatalIf(t, err != nil, "parseRawObject: %v", err)
	obj.Spec["key"] = "mutated"
	got := doc["spec"].(map[string]any)["key"]
	assert.ErrorIf(t, got != "original", "RawObject.Spec aliases parsed doc; got %q in source after mutation", got)
	clone := *obj
	clone.Spec = DeepCopyMap(obj.Spec)
	clone.Spec["key"] = "clone-only"
	assert.ErrorIf(t, obj.Spec["key"] != "mutated", "copied spec aliases original: copy mutation propagated to source")
}
