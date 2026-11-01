package resourcesetinput

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestPermute_UpstreamIDs(t *testing.T) {
	t.Parallel()
	got, err := Permute([]Group{
		{Name: "provider-1", Inputs: []map[string]any{{"key1": "value1"}, {"key1": "value2"}}},
		{Name: "provider-2", Inputs: []map[string]any{{"key2": "value1"}, {"key2": "value2"}}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, got, []map[string]any{
		{"id": "2075461889", "provider_1": map[string]any{"key1": "value1"}, "provider_2": map[string]any{"key2": "value1"}},
		{"id": "2075527426", "provider_1": map[string]any{"key1": "value1"}, "provider_2": map[string]any{"key2": "value2"}},
		{"id": "2076379394", "provider_1": map[string]any{"key1": "value2"}, "provider_2": map[string]any{"key2": "value1"}},
		{"id": "2076444931", "provider_1": map[string]any{"key1": "value2"}, "provider_2": map[string]any{"key2": "value2"}},
	})
}

func TestNormalizeKeyForTemplate_UpstreamKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ key, want string }{
		{"My_ResourceSet", "my_resourceset"},
		{"My-ResourceSet", "my_resourceset"},
		{"My@ResourceSet", "my_resourceset"},
		{"My ResourceSet", "my_resourceset"},
		{"__", ""},
		{"A💻B", "ab"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, normalizeKeyForTemplate(tc.key), tc.want)
		})
	}
}
