package values

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestReplaceValueAtPath_TypeCoercion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		path string
		val  string
		want any
	}{
		{"int", "replicaCount", "3", float64(3)},
		{"bool", "enabled", "true", true},
		{"null", "extra", "null", nil},
		{"nested map", "image.repository", "nginx", "nginx"},
		{"quoted string forces string", "tag", `"123"`, "123"},
		{"list index", "ports[0]", "8080", float64(8080)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := map[string]any{}
			err := replaceValueAtPath(got, tc.path, tc.val)
			{
				assert.FatalIf(t, err != nil, "replaceValueAtPath: %v", err)
			}
			leaf := walkPath(t, got, tc.path)
			{
				assert.ErrorIf(
					t,
					!equalish(leaf, tc.want),
					"path %q: got %v (%T), want %v (%T)",
					tc.path,
					leaf,
					leaf,
					tc.want,
					tc.want,
				)
			}
		})
	}
}

func walkPath(t *testing.T, m map[string]any, path string) any {
	t.Helper()
	cur := any(m)
	parts := strings.SplitSeq(path, ".")
	for p := range parts {
		if key, _, ok := strings.Cut(p, "["); ok {
			if cm, ok := cur.(map[string]any); ok {
				cur = cm[key]
			}
			// Strip "[0]" → 0 and index.
			cur = cur.([]any)[0]
			continue
		}
		if cm, ok := cur.(map[string]any); ok {
			cur = cm[p]
		}
	}
	return cur
}

func equalish(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// strvals.ParseInto returns int64 for integers; allow comparison
	// against float64 literals in test cases.
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(float64); ok {
			return float64(av) == bv
		}
	case int:
		if bv, ok := b.(float64); ok {
			return float64(av) == bv
		}
	}
	return a == b
}
