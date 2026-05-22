package selector

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestMetadata_Matches(t *testing.T) {
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Labels: map[string]string{"env": "prod"},
	}
	cases := []struct {
		name string
		sel  Metadata
		want bool
	}{
		{"empty matches all", Metadata{}, true},
		{"name match", Metadata{Name: "apps"}, true},
		{"name mismatch", Metadata{Name: "other"}, false},
		{"label match", Metadata{Labels: map[string]string{"env": "prod"}}, true},
		{"label mismatch", Metadata{Labels: map[string]string{"env": "dev"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.sel.Matches(ks); got != tc.want {
				t.Errorf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestMetadata_NilSafe(t *testing.T) {
	s := Metadata{}
	if s.Matches(nil) {
		t.Errorf("nil should never match")
	}
}
