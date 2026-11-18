package manifest

import (
	"encoding/base64"
	"slices"
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

func TestData_WithSuppliedEncryptedReplacement(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:opaque,iv:x,tag:y,type:str]"
	for _, repository := range []Data{
		&ConfigMap{Data: map[string]any{"HIDDEN": cipher, "CLEAR": "repository"}},
		&Secret{Data: map[string]any{"HIDDEN": cipher}, StringData: map[string]any{"CLEAR": "repository"}},
	} {
		before := repository.Snapshot()
		supplied := &ConfigMap{Data: map[string]any{"HIDDEN": "supplied", "CLEAR": "collision", "ENCRYPTED": cipher}}
		result := repository.WithSupplied(supplied)
		assert.Diff(t, result.Overridden, []string{"CLEAR"})
		value, err := result.Data.Value("HIDDEN")
		assert.NoError(t, err)
		assert.Equal(t, value, DataValue{Value: "supplied", Found: true})
		value, err = result.Data.Value("ENCRYPTED")
		assert.NoError(t, err)
		assert.Equal(t, value, DataValue{Found: true, Encrypted: true})
		assert.Equal(t, slices.Contains(slices.Collect(result.Data.Keys()), "ENCRYPTED"), true)
		value, err = repository.Value("HIDDEN")
		assert.NoError(t, err)
		assert.Equal(t, value, DataValue{Found: true, Encrypted: true})
		assert.Diff(t, repository.Snapshot(), before)
	}
}

func TestData_WithSuppliedEncryptedMembership(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:supplied,iv:x,tag:y,type:str]"
	const repositoryCipher = "ENC[AES256_GCM,data:repository,iv:x,tag:y,type:str]"
	encoded := base64.StdEncoding.EncodeToString([]byte(cipher))
	for _, supplied := range []Data{
		newConfigMap(NamedResource{}, map[string]any{"KEY": cipher}),
		newSecret(NamedResource{}, map[string]any{"KEY": encoded}, nil),
		newSecret(NamedResource{}, map[string]any{"KEY": base64.StdEncoding.EncodeToString([]byte("[sops]"))}, nil),
	} {
		for _, repository := range []Data{newConfigMap(NamedResource{}, nil), newSecret(NamedResource{}, nil, nil)} {
			before := repository.Snapshot()
			input := supplied.Snapshot()
			result := repository.WithSupplied(supplied)
			assert.Equal(t, result.Data.HasKeys(), true)
			assert.Diff(t, slices.Collect(result.Data.Keys()), []string{"KEY"})
			value, err := result.Data.Value("KEY")
			assert.NoError(t, err)
			assert.Equal(t, value, DataValue{Found: true, Encrypted: true})
			assert.Diff(t, repository.Snapshot(), before)
			assert.Diff(t, supplied.Snapshot(), input)
			repeated := result.Data.WithSupplied(supplied)
			assert.Equal(t, repeated.Data == result.Data, true)
			replacement := result.Data.WithSupplied(newConfigMap(NamedResource{}, map[string]any{"KEY": "readable"}))
			value, err = replacement.Data.Value("KEY")
			assert.NoError(t, err)
			assert.Equal(t, value, DataValue{Value: "readable", Found: true})
			assert.Diff(t, slices.Collect(replacement.Data.Keys()), []string{"KEY"})
			collision := replacement.Data.WithSupplied(supplied)
			assert.Equal(t, collision.Data == replacement.Data, true)
			assert.Diff(t, collision.Overridden, []string{"KEY"})
		}
	}
	repository := newSecret(NamedResource{}, map[string]any{"KEY": repositoryCipher}, nil)
	before := repository.Snapshot()
	result := repository.WithSupplied(newConfigMap(NamedResource{}, map[string]any{"KEY": cipher}))
	assert.Equal(t, result.Data == repository, true)
	assert.Diff(t, result.Data.Snapshot(), before)
}
