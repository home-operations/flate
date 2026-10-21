package resourceset

import (
	"encoding/json"
	"errors"
	"slices"
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
	docs, fp, err := renderWithAdmission(rs, resolve, "")
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
	otherDocs, otherFP, err := renderWithAdmission(rs, otherResolve, "")
	assert.Equal(t, otherFP, fp)
	assert.NoError(t, err)
	otherEncoded, err := json.Marshal(otherDocs)
	assert.NoError(t, err)
	assert.Equal(t, string(otherEncoded), string(encoded))

	for _, field := range []string{"name", "namespace", "provider type", "provider identity"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			candidate := *rs
			providers := []*manifest.ResourceSetInputProvider{a, b}
			changed := *a
			providers[0] = &changed
			switch field {
			case "name":
				candidate.Name = "other"
			case "namespace":
				candidate.Namespace = "other"
			case "provider identity":
				providers[0].Name = "renamed"
			case "provider type":
				providers[0].Type = fluxopv1.InputProviderGitHubBranch
				providers[0].Status.ExportedInputs = []fluxopv1.ResourceSetInput{a.DefaultValues}
			}
			calls := 0
			got, key, err := renderWithAdmission(&candidate, func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
				calls++
				return providers, nil
			}, fp)
			assert.NoError(t, err)
			assert.Equal(t, calls, len(candidate.InputsFrom))
			assert.Equal(t, len(got), 2)
			if key == fp {
				t.Fatal("effective content change replayed")
			}
		})
	}

	t.Run("resolver error cannot replay", func(t *testing.T) {
		t.Parallel()
		failure := errors.New("captured provider unavailable")
		docs, key, err := renderWithAdmission(rs, func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			return nil, failure
		}, fp)
		if !errors.Is(err, failure) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || key != "" || docs != nil {
			t.Fatalf("resolver error became a hit: docs=%v key=%q error=%v", docs, key, err)
		}
	})
	t.Run("inline order", func(t *testing.T) {
		t.Parallel()
		candidate := &manifest.ResourceSet{Name: "inline", Namespace: "apps", Inputs: []fluxopv1.ResourceSetInput{
			{"name": {Raw: []byte(`"first"`)}}, {"name": {Raw: []byte(`"second"`)}},
		}, ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.name >>'}"}
		docs, key, err := renderWithAdmission(candidate, nil, "")
		assert.NoError(t, err)
		first, _ := manifest.DocMetadata(docs[0])
		assert.Equal(t, first, "first")
		candidate.Inputs[0], candidate.Inputs[1] = candidate.Inputs[1], candidate.Inputs[0]
		docs, current, err := renderWithAdmission(candidate, nil, key)
		assert.NoError(t, err)
		if current == key {
			t.Fatal("inline order replayed")
		}
		first, _ = manifest.DocMetadata(docs[0])
		assert.Equal(t, first, "second")
	})

	t.Run("resolver delivery order", func(t *testing.T) {
		t.Parallel()
		reversed := []*manifest.ResourceSetInputProvider{b, nil, a, a}
		calls := 0
		got, key, err := renderWithAdmission(rs, func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			calls++
			return reversed, nil
		}, "")
		assert.NoError(t, err)
		assert.Equal(t, calls, 1)
		assert.Equal(t, key, fp)
		assert.Diff(t, got, docs)
		assert.Equal(t, reversed[0].Named(), b.Named())
		overlapped := *rs
		overlapped.InputsFrom = slices.Clone(rs.InputsFrom)
		overlapped.InputsFrom = append(overlapped.InputsFrom, fluxopv1.InputProviderReference{Name: "a"})
		calls = 0
		got, key, err = renderWithAdmission(&overlapped, func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			calls++
			return reversed, nil
		}, "")
		assert.NoError(t, err)
		assert.Equal(t, calls, 2)
		assert.Diff(t, got, docs)
		hit, replayKey, err := renderWithAdmission(&overlapped, func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			return []*manifest.ResourceSetInputProvider{a, b, nil}, nil
		}, key)
		assert.NoError(t, err)
		assert.Equal(t, replayKey, key)
		if hit != nil {
			t.Fatalf("equal canonical input returned documents: %v", hit)
		}
	})

	invalid := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}
	for _, tc := range []struct {
		name   string
		refs   []fluxopv1.InputProviderReference
		inputs []any
	}{
		{"Flux named", []fluxopv1.InputProviderReference{{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Name: "a"}}, []any{a}},
		{"foreign named", []fluxopv1.InputProviderReference{{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Name: "a"}}, nil},
		{"Flux selector", []fluxopv1.InputProviderReference{{APIVersion: fluxopv1.GroupVersion.String(), Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}}, []any{a, b}},
		{"foreign selector", []fluxopv1.InputProviderReference{{APIVersion: "other.example/v1", Kind: manifest.KindResourceSetInputProvider, Selector: rs.InputsFrom[0].Selector}}, nil},
		{"expressions", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: metav1.LabelSelectorOpIn, Values: []string{"db"}}}}}}, []any{a, b}},
		{"empty selector", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{}}}, []any{a, b}},
		{"named invalid selector", []fluxopv1.InputProviderReference{{Name: "a", Selector: invalid}}, []any{a}},
		{"nil selector", []fluxopv1.InputProviderReference{{}}, nil},
		{"foreign kind", []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSet, Selector: &metav1.LabelSelector{}}}, nil},
		{"omitted reference fields", []fluxopv1.InputProviderReference{{Selector: rs.InputsFrom[0].Selector}}, []any{a, b}},
		{"overlap", []fluxopv1.InputProviderReference{{Name: "a"}, rs.InputsFrom[0], rs.InputsFrom[0]}, []any{a, b}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			referenced := *rs
			referenced.InputsFrom = tc.refs
			resolve := resourceset.StoreResolver(s)
			got, _, err := renderWithAdmission(&referenced, resolve, "")

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
			got, key, err := renderWithAdmission(rs, resolve, "")
			assert.Equal(t, key != fp, tc.changed)
			assert.NoError(t, err)
			values := make(map[string]string)
			for _, doc := range got {
				values[doc["metadata"].(map[string]any)["name"].(string)] = doc["data"].(map[string]any)["value"].(string)
			}
			assert.Diff(t, values, tc.want)
		})
	}
}

func TestResourceSetFingerprint_CapturedInputs(t *testing.T) {
	t.Parallel()
	s := store.New()
	rs := selectorFixture("reader", "blue")
	rs.ResourcesTemplate = "<< $_ := set inputs.nested \"value\" \"template\" >>apiVersion: v1\nkind: ConfigMap\nmetadata: {name: captured}\ndata: {value: '<< inputs.value >>'}"
	s.AddObject(rs)
	p := staticRSIP("provider", "ns", map[string]string{"color": "blue"}, map[string]string{"value": "spec"})
	p.Status.ExportedInputs = []fluxopv1.ResourceSetInput{{"value": {Raw: []byte(`"status-a"`)}, "nested": {Raw: []byte(`{"value":"stored"}`)}}}
	s.AddObject(p)
	resolve := resourceset.StoreResolver(s)
	docs, fingerprint, err := renderWithAdmission(rs, resolve, "")
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"].(string), "status-a")
	assert.Equal(t, string(p.Status.ExportedInputs[0]["nested"].Raw), `{"value":"stored"}`)
	hit, key, err := renderWithAdmission(rs, resolve, fingerprint)
	assert.NoError(t, err)
	assert.Equal(t, key, fingerprint)
	assert.Equal(t, len(hit), 0)
	fresh := *p
	fresh.Status.ExportedInputs = []fluxopv1.ResourceSetInput{{"value": {Raw: []byte(`"status-b"`)}, "nested": {Raw: []byte(`{"value":"stored"}`)}}}
	s.AddObject(&fresh)
	docs, current, err := renderWithAdmission(rs, resolve, fingerprint)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"].(string), "status-b")
	if current == fingerprint {
		t.Fatal("successor did not capture the changed exported inputs")
	}
}

func renderWithAdmission(rs *manifest.ResourceSet, resolve resourceset.ProviderResolver, previous string) ([]map[string]any, string, error) {
	s := store.New()
	s.AddObject(rs)
	s.SetArtifact(rs.Named(), &store.Emission{Fingerprint: previous})
	c := base.New(s, nil, "test")
	var fp string
	docs, err := resourceset.Render(rs, resolve, func(content any) (bool, error) {
		var hit bool
		var err error
		fp, hit, err = c.FingerprintDedup(rs.Named(), content)
		return hit, err
	})
	return docs, fp, err
}
