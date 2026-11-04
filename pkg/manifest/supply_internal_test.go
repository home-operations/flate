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
		{"ConfigMap", &ConfigMap{Data: map[string]any{"empty": "", "real": "repository", "wiped": ValuePlaceholder("wiped"), "malformed": []any{"bad"}, "shadowed-real": "repository", "shadowed-empty": ""}}, &ConfigMap{Data: map[string]any{"empty": "supplied", "real": "supplied", "wiped": "filled", "absent": "added", "malformed": "hidden", "shadowed-real": "supplied", "shadowed-empty": "supplied"}}},
		{"Secret", &Secret{Data: map[string]any{"real": base64.StdEncoding.EncodeToString([]byte("repository")), "wiped": base64.StdEncoding.EncodeToString([]byte(ValuePlaceholder("wiped"))), "malformed": "%%%", "unrequested": "%%%", "shadowed-real": "%%%", "shadowed-empty": "%%%"}, StringData: map[string]any{"empty": "", "wiped": ValuePlaceholder("wiped"), "shadowed-real": "repository", "shadowed-empty": ""}}, &Secret{StringData: map[string]any{"empty": "supplied", "real": "supplied", "wiped": "filled", "absent": "added", "malformed": "hidden", "shadowed-real": "supplied", "shadowed-empty": "supplied"}}},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testDataWithSuppliedCase(t, &tc) },
		)
	}
}

func testDataWithSuppliedCase(t *testing.T, testCaseInput *struct {
	name       string
	repository Data
	supplied   Data
},
) {
	t.Helper()

	before := testCaseInput.repository.Snapshot()
	supplyBefore := testCaseInput.supplied.Snapshot()
	supply := testCaseInput.repository.WithSupplied(testCaseInput.supplied)
	data, keys := supply.Data, supply.Overridden
	assert.Diff(t, keys, []string{"empty", "real", "shadowed-empty", "shadowed-real"})
	for key, want := range map[string]string{"empty": "", "real": "repository", "wiped": "filled", "absent": "added", "shadowed-real": "repository", "shadowed-empty": ""} {
		valueResult, err := data.Value(key)
		got, found := valueResult.Value, valueResult.Found
		assert.NoError(t, err)
		assert.FatalIf(t, !found || got != want, "%s = %q, found %t; want %q", key, got, found, want)
	}
	{
		_, err := data.Value("malformed")
		assert.FatalIf(t, err == nil, "%s", "supply concealed malformed repository data")
	}
	if secret, ok := data.(*Secret); ok {
		{
			_, duplicate := secret.Data["wiped"]
			assert.FatalIf(t, duplicate, "%s", "filled logical key retained a shadowed data representation")
		}
	}
	assert.Diff(t, testCaseInput.repository.Snapshot(), before)
	assert.Diff(t, testCaseInput.supplied.Snapshot(), supplyBefore)
	supply2 := testCaseInput.repository.WithSupplied(
		&ConfigMap{Data: map[string]any{"empty": "supplied", "real": "supplied", "malformed": "hidden"}},
	)
	unchanged, overridden := supply2.Data, supply2.Overridden
	assert.FatalIf(t, unchanged != testCaseInput.repository, "%s", "no-op fill changed object identity")
	assert.Diff(t, overridden, []string{"empty", "real"})
	supply3 := data.WithSupplied(testCaseInput.supplied)
	again := supply3.Data
	assert.FatalIf(t, again != data, "%s", "repeated fill changed object identity")
	supply4 := testCaseInput.repository.WithSupplied(
		&ConfigMap{Data: map[string]any{"absent": "added"}},
	)
	partial, overridden := supply4.Data, supply4.Overridden
	assert.Equal(t, len(overridden), 0)
	for key, want := range map[string]string{"real": "repository", "absent": "added", "wiped": ValuePlaceholder("wiped")} {
		valueResult3, err := partial.Value(key)
		value, found := valueResult3.Value, valueResult3.Found
		assert.NoError(t, err)
		assert.Equal(t, found, true)
		assert.Equal(t, value, want)
	}
	assert.Diff(t, testCaseInput.repository.Snapshot(), before)
}
