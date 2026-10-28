package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestNewSupplied_SnapshotAndReplacement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		object func(map[string]any) manifest.BaseManifest
	}{
		{"Secret", func(data map[string]any) manifest.BaseManifest {
			return &manifest.Secret{Name: "input", Namespace: "apps", StringData: data}
		}},
		{"ConfigMap", func(data map[string]any) manifest.BaseManifest {
			return &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: data}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := tc.object(map[string]any{"old": "old"})
			payload := []byte("owned")
			data := map[string]any{"value": payload}
			last := tc.object(data)
			supplied, err := NewSupplied([]SuppliedObject{{Object: first, Path: "first"}, {Object: last, Path: "last"}})
			assert.NoError(t, err)
			payload[0] = 'x'
			data["extra"] = "late"
			s := New()
			s.SetSupplied(supplied)
			facts := s.ReferenceFacts(first.Named(), nil)
			if facts.Data != nil || facts.Accepted || facts.SupplyPath != "last" || s.GetObject(first.Named()) != nil {
				t.Fatalf("canonical publication: %+v", facts)
			}
			value, found, err := facts.Supplied.Value("value")
			assert.NoError(t, err)
			if value != "owned" || !found {
				t.Fatalf("snapshot value = %q, found %t", value, found)
			}
			for _, key := range []string{"old", "extra"} {
				if _, found, _ := facts.Supplied.Value(key); found {
					t.Fatalf("snapshot contains %s", key)
				}
			}
		})
	}
}

func TestNewSupplied_Validation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		object manifest.BaseManifest
	}{
		{"nil", nil}, {"typed nil Secret", (*manifest.Secret)(nil)}, {"typed nil ConfigMap", (*manifest.ConfigMap)(nil)},
		{"unsupported type", &manifest.Kustomization{Name: "input", Namespace: "apps"}},
		{"missing namespace", &manifest.ConfigMap{Name: "missing-namespace"}},
		{"invalid base64", &manifest.Secret{Name: "input", Namespace: "apps", Data: map[string]any{"bad": "payload%%%"}}},
		{"nonscalar", &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"bad": []any{"payload"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSupplied([]SuppliedObject{{Object: tc.object, Path: "input.yaml"}, {Object: &manifest.Secret{Name: "input", Namespace: "apps"}}})
			if !errors.Is(err, manifest.ErrInput) || strings.Contains(err.Error(), "payload") || !strings.Contains(err.Error(), "input.yaml") {
				t.Fatalf("validation = %v", err)
			}
		})
	}
}
