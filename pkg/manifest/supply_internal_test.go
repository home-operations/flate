package manifest

import (
	"encoding/base64"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestData_WithSupplied(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                 string
		repository, supplied Data
	}{
		{"ConfigMap", &ConfigMap{Data: map[string]any{"empty": "", "real": "repository", "malformed": []any{"bad"}, "shadowed-real": "repository", "shadowed-empty": ""}}, &ConfigMap{Data: map[string]any{"empty": "supplied", "real": "supplied", "unreadable": "filled", "absent": "added", "malformed": "hidden", "shadowed-real": "supplied", "shadowed-empty": "supplied"}}},
		{"Secret", &Secret{Data: map[string]any{"real": base64.StdEncoding.EncodeToString([]byte("repository")), "malformed": "%%%", "unrequested": "%%%", "shadowed-real": "%%%", "shadowed-empty": "%%%"}, StringData: map[string]any{"empty": "", "shadowed-real": "repository", "shadowed-empty": ""}}, &Secret{StringData: map[string]any{"empty": "supplied", "real": "supplied", "unreadable": "filled", "absent": "added", "malformed": "hidden", "shadowed-real": "supplied", "shadowed-empty": "supplied"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testDataWithSuppliedCase1(t, &tc)
		})
	}
}

func testDataWithSuppliedCase1(t *testing.T, tc *struct {
	name       string
	repository Data
	supplied   Data
},
) {
	t.Helper()

	before := tc.repository.Snapshot()
	supplyBefore := tc.supplied.Snapshot()
	result1 := tc.repository.WithSupplied(tc.supplied)
	data, keys := result1.Data, result1.Overridden
	assert.Diff(t, keys, []string{"empty", "real", "shadowed-empty", "shadowed-real"})
	for key, want := range map[string]string{"empty": "", "real": "repository", "unreadable": "filled", "absent": "added", "shadowed-real": "repository", "shadowed-empty": ""} {
		result6, err := data.Value(key)
		got, found := result6.Value, result6.Found
		assert.NoError(t, err)
		assert.FatalIf(t, !found || got != want,
			"%s = %q, found %t; want %q", key, got, found, want)
	}
	if _, err := data.Value("malformed"); err == nil {
		t.Fatal("supply concealed malformed repository data")
	}
	if secret, ok := data.(*Secret); ok {
		if _, duplicate := secret.Data["unreadable"]; duplicate {
			t.Fatal("filled logical key retained a shadowed data representation")
		}
	}
	assert.Diff(t, tc.repository.Snapshot(), before)
	assert.Diff(t, tc.supplied.Snapshot(), supplyBefore)
	result3 := tc.repository.WithSupplied(
		&ConfigMap{Data: map[string]any{"empty": "supplied", "real": "supplied", "malformed": "hidden"}},
	)
	unchanged, overridden := result3.Data, result3.Overridden
	assert.FatalIf(t, unchanged != tc.repository, "%s", "no-op fill changed object identity")

	assert.Diff(t, overridden, []string{"empty", "real"})
	result4 := data.WithSupplied(tc.supplied)
	again := result4.Data
	assert.FatalIf(t, again != data, "%s", "repeated fill changed object identity")

	result5 := tc.repository.WithSupplied(&ConfigMap{Data: map[string]any{"absent": "added"}})
	partial, overridden := result5.Data, result5.Overridden
	assert.Equal(t, len(overridden), 0)
	for key, want := range map[string]string{"real": "repository", "absent": "added"} {
		result7, err := partial.Value(key)
		value, found := result7.Value, result7.Found
		assert.NoError(t, err)
		assert.Equal(t, found, true)
		assert.Equal(t, value, want)
	}
	assert.Diff(t, tc.repository.Snapshot(), before)
}
