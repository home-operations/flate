package manifest

import (
	"slices"
	"testing"
)

func TestProducerTargets(t *testing.T) {
	cases := []struct {
		name string
		raw  *RawObject
		want []NamedResource
	}{
		{
			name: "ExternalSecret explicit target.name",
			raw: &RawObject{Kind: "ExternalSecret", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"target": map[string]any{"name": "app-values"}}},
			want: []NamedResource{{Kind: KindSecret, Namespace: "default", Name: "app-values"}},
		},
		{
			name: "ExternalSecret no target falls back to own name",
			raw:  &RawObject{Kind: "ExternalSecret", Namespace: "staging", Name: "my-secret", Spec: map[string]any{}},
			want: []NamedResource{{Kind: KindSecret, Namespace: "staging", Name: "my-secret"}},
		},
		{
			name: "SealedSecret template.metadata.name",
			raw: &RawObject{Kind: "SealedSecret", Namespace: "prod", Name: "sealed",
				Spec: map[string]any{"template": map[string]any{"metadata": map[string]any{"name": "sealed-db"}}}},
			want: []NamedResource{{Kind: KindSecret, Namespace: "prod", Name: "sealed-db"}},
		},
		{
			name: "SealedSecret no template falls back to own name",
			raw:  &RawObject{Kind: "SealedSecret", Namespace: "prod", Name: "sealed-db", Spec: map[string]any{}},
			want: []NamedResource{{Kind: KindSecret, Namespace: "prod", Name: "sealed-db"}},
		},
		{
			name: "ObjectBucketClaim produces a Secret and a ConfigMap named after the claim",
			raw:  &RawObject{Kind: "ObjectBucketClaim", Namespace: "default", Name: "netbox-obc"},
			want: []NamedResource{
				{Kind: KindSecret, Namespace: "default", Name: "netbox-obc"},
				{Kind: KindConfigMap, Namespace: "default", Name: "netbox-obc"},
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
			if got := ProducerTargets(c.raw); !slices.Equal(got, c.want) {
				t.Errorf("targets = %v, want %v", got, c.want)
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
