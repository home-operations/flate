package resourceset

import (
	"encoding/json"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

func TestResourceSetFingerprint_SelectorInputs(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "matrix", Namespace: "apps",
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind:     manifest.KindResourceSetInputProvider,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}},
		}},
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.provider.name >>'}\ndata: {value: '<< inputs.value >>'}\n",
	}
	a := staticRSIP("a", "apps", map[string]string{"role": "db"}, map[string]string{"id": "one", "value": "first"})
	b := staticRSIP("b", "apps", map[string]string{"role": "db"}, map[string]string{"id": "two", "value": "second"})
	s := store.New()
	s.AddObject(b)
	s.AddObject(a)
	resolve := resourceset.StoreResolver(s)
	content := resourceSetContent(rs, resolve)
	fp := replayKey(content)
	docs, err := resourceset.Render(rs, resolve)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 2)
	encoded, err := json.Marshal(docs)
	assert.NoError(t, err)
	if fp == "" {
		t.Fatal("empty fingerprint")
	}
	other := store.New()
	other.AddObject(a)
	other.AddObject(b)
	otherResolve := resourceset.StoreResolver(other)
	assert.Equal(t, replayKey(resourceSetContent(rs, otherResolve)), fp)
	otherDocs, err := resourceset.Render(rs, otherResolve)
	assert.NoError(t, err)
	otherEncoded, err := json.Marshal(otherDocs)
	assert.NoError(t, err)
	assert.Equal(t, string(otherEncoded), string(encoded))
	aContent, err := a.Content()
	assert.NoError(t, err)
	bContent, err := b.Content()
	assert.NoError(t, err)
	assert.Diff(t, content, rs.Content([]any{aContent, bContent}))

	invalid := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}
	for _, tc := range []struct {
		name   string
		refs   []fluxopv1.InputProviderReference
		inputs []any
	}{
		{"Flux named", []fluxopv1.InputProviderReference{{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Name: "a"}}, []any{aContent}},
		{"foreign named", []fluxopv1.InputProviderReference{{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Name: "a"}}, nil},
		{"Flux selector", []fluxopv1.InputProviderReference{{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}}, []any{aContent, bContent}},
		{"foreign selector", []fluxopv1.InputProviderReference{{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}}, nil},
		{"expressions", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: metav1.LabelSelectorOpIn, Values: []string{"db"}}}}}}, []any{aContent, bContent}},
		{"empty selector", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{}}}, []any{aContent, bContent}},
		{"named invalid selector", []fluxopv1.InputProviderReference{{Name: "a", Selector: invalid}}, []any{aContent}},
		{"nil selector", []fluxopv1.InputProviderReference{{}}, nil},
		{"foreign kind", []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSet, Selector: &metav1.LabelSelector{}}}, nil},
		{"omitted reference fields", []fluxopv1.InputProviderReference{{Selector: rs.InputsFrom[0].Selector}}, []any{aContent, bContent}},
		{"overlap", []fluxopv1.InputProviderReference{{Name: "a"}, rs.InputsFrom[0], rs.InputsFrom[0]}, []any{aContent, bContent}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			referenced := *rs
			referenced.InputsFrom = tc.refs
			resolve := resourceset.StoreResolver(s)
			assert.Diff(t, resourceSetContent(&referenced, resolve), referenced.Content(tc.inputs))
			got, err := resourceset.Render(&referenced, resolve)
			assert.NoError(t, err)
			var want []map[string]any
			if len(tc.inputs) != 0 {
				want = docs[:len(tc.inputs)]
			}
			assert.Diff(t, got, want)
		})
	}

	for _, tc := range []struct {
		name     string
		provider *manifest.ResourceSetInputProvider
		changed  bool
		want     map[string]string
	}{
		{"nonmatching label", staticRSIP("c", "apps", map[string]string{"role": "web"}, nil), false, map[string]string{"a": "first", "b": "second"}},
		{"other namespace", staticRSIP("c", "other", map[string]string{"role": "db"}, nil), false, map[string]string{"a": "first", "b": "second"}},
		{"new match", staticRSIP("c", "apps", map[string]string{"role": "db"}, map[string]string{"value": "third"}), true, map[string]string{"a": "first", "b": "second", "c": "third"}},
		{"changed inputs", staticRSIP("a", "apps", map[string]string{"role": "db"}, map[string]string{"value": "updated"}), true, map[string]string{"a": "updated", "b": "second"}},
		{"removed match", staticRSIP("a", "apps", map[string]string{"role": "web"}, map[string]string{"id": "one", "value": "first"}), true, map[string]string{"b": "second"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.AddObject(a)
			st.AddObject(b)
			st.AddObject(tc.provider)
			resolve := resourceset.StoreResolver(st)
			assert.Equal(t, replayKey(resourceSetContent(rs, resolve)) != fp, tc.changed)
			got, err := resourceset.Render(rs, resolve)
			assert.NoError(t, err)
			values := make(map[string]string)
			for _, doc := range got {
				values[doc["metadata"].(map[string]any)["name"].(string)] = doc["data"].(map[string]any)["value"].(string)
			}
			assert.Diff(t, values, tc.want)
		})
	}
}

func replayKey(content any) string {
	c := base.New(store.New(), nil, "test")
	fp, _, _ := c.FingerprintDedup(manifest.NamedResource{}, content)
	return fp
}
