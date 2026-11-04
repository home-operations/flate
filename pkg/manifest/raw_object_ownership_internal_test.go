package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

// TestRawObject_DoesNotAliasParsedDoc pins the deep-copy fix: a
// RawObject.Spec must not alias the loader's parsed YAML map.
// Previously r.Spec = doc["spec"].(map[string]any) shared the map
// pointer; mutating r.Spec corrupted the original doc the loader
// reused across passes.
func TestRawObject_DoesNotAliasParsedDoc(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "foo"},
		"spec":       map[string]any{"key": "original"},
	}
	obj, err := parseRawObject(doc)
	{
		assert.FatalIf(t, err != nil, "parseRawObject: %v", err)
	}
	// Mutate the RawObject's spec. The original doc must NOT change.
	obj.Spec["key"] = "mutated"
	got := doc["spec"].(map[string]any)["key"]
	{
		assert.ErrorIf(t, got != "original", "RawObject.Spec aliases parsed doc; got %q in source after mutation", got)
	}
	// Clone() produces an independent copy too.
	clone := obj.Clone()
	clone.Spec["key"] = "clone-only"
	{
		assert.ErrorIf(t, obj.Spec["key"] != "mutated", "Clone aliases original: clone mutation propagated to source")
	}
}
