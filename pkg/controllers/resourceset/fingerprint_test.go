package resourceset

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/base"
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
	fp := replayKey(resourceSetContent(rs, s))
	if fp == "" {
		t.Fatal("empty fingerprint")
	}
	other := store.New()
	other.AddObject(a)
	other.AddObject(b)
	assert.Equal(t, replayKey(resourceSetContent(rs, other)), fp)
	aContent, err := a.Content()
	assert.NoError(t, err)
	bContent, err := b.Content()
	assert.NoError(t, err)
	assert.Diff(t, resourceSetContent(rs, s), rs.Content([]any{aContent, bContent}))

	for _, tc := range []struct {
		name   string
		ref    fluxopv1.InputProviderReference
		inputs []any
	}{
		{"Flux named", fluxopv1.InputProviderReference{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Name: "a"}, []any{aContent}},
		{"foreign named", fluxopv1.InputProviderReference{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Name: "a"}, nil},
		{"Flux selector", fluxopv1.InputProviderReference{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}, []any{aContent, bContent}},
		{"foreign selector", fluxopv1.InputProviderReference{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			referenced := *rs
			referenced.InputsFrom = []fluxopv1.InputProviderReference{tc.ref}
			assert.Diff(t, resourceSetContent(&referenced, s), referenced.Content(tc.inputs))
		})
	}

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
			assert.Equal(t, replayKey(resourceSetContent(rs, st)) != fp, tc.changed)
		})
	}
}

func replayKey(content any) string {
	c := base.New(store.New(), nil, "test")
	fp, _, _ := c.FingerprintDedup(manifest.NamedResource{}, content, func([]map[string]any) {})
	return fp
}
