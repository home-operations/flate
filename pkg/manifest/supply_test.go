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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := tc.repository.Snapshot()
			supplyBefore := tc.supplied.Snapshot()
			data, keys := tc.repository.WithSupplied(tc.supplied)
			assert.Diff(t, keys, []string{"empty", "real", "shadowed-empty", "shadowed-real"})
			for key, want := range map[string]string{"empty": "", "real": "repository", "wiped": "filled", "absent": "added", "shadowed-real": "repository", "shadowed-empty": ""} {
				got, found, err := data.Value(key)
				assert.NoError(t, err)
				if !found || got != want {
					t.Fatalf("%s = %q, found %t; want %q", key, got, found, want)
				}
			}
			if _, _, err := data.Value("malformed"); err == nil {
				t.Fatal("supply concealed malformed repository data")
			}
			if secret, ok := data.(*Secret); ok {
				if _, duplicate := secret.Data["wiped"]; duplicate {
					t.Fatal("filled logical key retained a shadowed data representation")
				}
			}
			assert.Diff(t, tc.repository.Snapshot(), before)
			assert.Diff(t, tc.supplied.Snapshot(), supplyBefore)
			unchanged, overridden := tc.repository.WithSupplied(&ConfigMap{Data: map[string]any{"empty": "supplied", "real": "supplied", "malformed": "hidden"}})
			if unchanged != tc.repository {
				t.Fatal("no-op fill changed object identity")
			}
			assert.Diff(t, overridden, []string{"empty", "real"})
			again, _ := data.WithSupplied(tc.supplied)
			if again != data {
				t.Fatal("repeated fill changed object identity")
			}
			partial, overridden := tc.repository.WithSupplied(&ConfigMap{Data: map[string]any{"absent": "added"}})
			assert.Equal(t, len(overridden), 0)
			for key, want := range map[string]string{"real": "repository", "absent": "added", "wiped": ValuePlaceholder("wiped")} {
				value, found, err := partial.Value(key)
				assert.NoError(t, err)
				assert.Equal(t, found, true)
				assert.Equal(t, value, want)
			}
			assert.Diff(t, tc.repository.Snapshot(), before)
		})
	}
}
