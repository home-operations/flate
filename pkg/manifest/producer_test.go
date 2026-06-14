package manifest

import (
	"reflect"
	"testing"
)

// secret is a small helper so each case reads as identity + declared keys.
func secret(ns, name string, keys ...string) ProducerTarget {
	return ProducerTarget{
		NamedResource: NamedResource{Kind: KindSecret, Namespace: ns, Name: name},
		DeclaredKeys:  keys,
	}
}

func TestProducerTargets(t *testing.T) {
	cases := []struct {
		name string
		raw  *RawObject
		want []ProducerTarget
	}{
		{
			name: "ExternalSecret explicit target.name",
			raw: &RawObject{Kind: "ExternalSecret", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"target": map[string]any{"name": "app-values"}}},
			want: []ProducerTarget{secret("default", "app-values")},
		},
		{
			name: "ExternalSecret no target falls back to own name",
			raw:  &RawObject{Kind: "ExternalSecret", Namespace: "staging", Name: "my-secret", Spec: map[string]any{}},
			want: []ProducerTarget{secret("staging", "my-secret")},
		},
		{
			name: "ExternalSecret template.data declares the output keys",
			raw: &RawObject{Kind: "ExternalSecret", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"target": map[string]any{"template": map[string]any{
					"data": map[string]any{"PASSWORD": "{{ .pw }}", "USERNAME": "{{ .user }}"}}}}},
			want: []ProducerTarget{secret("default", "app-creds", "PASSWORD", "USERNAME")},
		},
		{
			name: "ExternalSecret without template declares data[].secretKey",
			raw: &RawObject{Kind: "ExternalSecret", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"data": []any{
					map[string]any{"secretKey": "TOKEN"},
					map[string]any{"secretKey": "API_KEY"},
				}}},
			want: []ProducerTarget{secret("default", "app-creds", "API_KEY", "TOKEN")},
		},
		{
			name: "ExternalSecret dataFrom-only declares nothing",
			raw: &RawObject{Kind: "ExternalSecret", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"dataFrom": []any{map[string]any{"extract": map[string]any{"key": "vault/app"}}}}},
			want: []ProducerTarget{secret("default", "app-creds")},
		},
		{
			name: "SealedSecret template.metadata.name + encryptedData keys",
			raw: &RawObject{Kind: "SealedSecret", Namespace: "prod", Name: "sealed",
				Spec: map[string]any{
					"template":      map[string]any{"metadata": map[string]any{"name": "sealed-db"}},
					"encryptedData": map[string]any{"password": "AgB...", "username": "AgC..."}}},
			want: []ProducerTarget{secret("prod", "sealed-db", "password", "username")},
		},
		{
			name: "SealedSecret no template falls back to own name, no keys",
			raw:  &RawObject{Kind: "SealedSecret", Namespace: "prod", Name: "sealed-db", Spec: map[string]any{}},
			want: []ProducerTarget{secret("prod", "sealed-db")},
		},
		{
			name: "ObjectBucketClaim produces a Secret and a ConfigMap named after the claim",
			raw:  &RawObject{Kind: "ObjectBucketClaim", Namespace: "default", Name: "netbox-obc"},
			want: []ProducerTarget{
				secret("default", "netbox-obc"),
				{NamedResource: NamedResource{Kind: KindConfigMap, Namespace: "default", Name: "netbox-obc"}},
			},
		},
		{
			name: "non-producer kind",
			raw:  &RawObject{Kind: "Certificate", Namespace: "default", Name: "tls"},
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ProducerTargets(c.raw); !reflect.DeepEqual(got, c.want) {
				t.Errorf("targets = %#v, want %#v", got, c.want)
			}
		})
	}
}

func TestProducerIndex(t *testing.T) {
	target := NamedResource{Kind: KindSecret, Namespace: "default", Name: "app-values"}
	producer := NamedResource{Kind: "ExternalSecret", Namespace: "default", Name: "app-creds"}

	var idx ProducerIndex
	if _, ok := idx.Producer(target); ok {
		t.Error("empty index reported a producer")
	}
	idx.Record(target, producer)
	got, ok := idx.Producer(target)
	if !ok || got != producer {
		t.Errorf("Producer = (%v, %v), want (%v, true)", got, ok, producer)
	}
	if _, ok := idx.Producer(NamedResource{Kind: KindSecret, Namespace: "other", Name: "app-values"}); ok {
		t.Error("matched a target in a different namespace")
	}

	// Nil-safe: a nil index records nothing and finds nothing.
	var nilIdx *ProducerIndex
	nilIdx.Record(target, producer)
	if _, ok := nilIdx.Producer(target); ok {
		t.Error("nil index reported a producer")
	}
}
