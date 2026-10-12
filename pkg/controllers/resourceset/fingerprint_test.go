package resourceset

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestResourceSetFingerprint_SelectorInputs(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "matrix", Namespace: "apps",
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind:     manifest.KindResourceSetInputProvider,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}},
		}},
	}
	a := staticRSIP("a", "apps", map[string]string{"role": "db"}, map[string]string{"id": "one", "value": "first"})
	b := staticRSIP("b", "apps", map[string]string{"role": "db"}, map[string]string{"id": "two", "value": "second"})
	s := store.New()
	s.AddObject(b)
	s.AddObject(a)
	fp := resourceSetFingerprint(rs, s)
	if fp == "" {
		t.Fatal("empty fingerprint")
	}
	other := store.New()
	other.AddObject(a)
	other.AddObject(b)
	assert.Equal(t, resourceSetFingerprint(rs, other), fp)
	assert.Diff(t, resolvedInputDigests(rs, s), []inputDigest{
		{ID: a.Named().String(), Inputs: manifest.Fingerprint([]map[string]any{{"id": "one", "value": "first"}})},
		{ID: b.Named().String(), Inputs: manifest.Fingerprint([]map[string]any{{"id": "two", "value": "second"}})},
	})
	for _, tc := range []struct {
		name     string
		provider *manifest.ResourceSetInputProvider
		changed  bool
	}{
		{"nonmatching label", staticRSIP("c", "apps", map[string]string{"role": "web"}, nil), false},
		{"other namespace", staticRSIP("c", "other", map[string]string{"role": "db"}, nil), false},
		{"new match", staticRSIP("c", "apps", map[string]string{"role": "db"}, map[string]string{"value": "third"}), true},
		{"changed inputs", staticRSIP("a", "apps", map[string]string{"role": "db"}, map[string]string{"value": "updated"}), true},
		{"removed match", staticRSIP("a", "apps", map[string]string{"role": "web"}, map[string]string{"id": "one", "value": "first"}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.New()
			st.AddObject(a)
			st.AddObject(b)
			st.AddObject(tc.provider)
			assert.Equal(t, resourceSetFingerprint(rs, st) != fp, tc.changed)
		})
	}
}
