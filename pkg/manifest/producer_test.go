package manifest

import "testing"

func TestProducerTargetSecret(t *testing.T) {
	cases := []struct {
		name     string
		raw      *RawObject
		wantOK   bool
		wantName string
		wantNS   string
	}{
		{
			name: "ExternalSecret explicit target.name",
			raw: &RawObject{Kind: "ExternalSecret", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"target": map[string]any{"name": "app-values"}}},
			wantOK: true, wantName: "app-values", wantNS: "default",
		},
		{
			name:   "ExternalSecret no target falls back to own name",
			raw:    &RawObject{Kind: "ExternalSecret", Namespace: "staging", Name: "my-secret", Spec: map[string]any{}},
			wantOK: true, wantName: "my-secret", wantNS: "staging",
		},
		{
			name: "SealedSecret template.metadata.name",
			raw: &RawObject{Kind: "SealedSecret", Namespace: "prod", Name: "sealed",
				Spec: map[string]any{"template": map[string]any{"metadata": map[string]any{"name": "sealed-db"}}}},
			wantOK: true, wantName: "sealed-db", wantNS: "prod",
		},
		{
			name:   "SealedSecret no template falls back to own name",
			raw:    &RawObject{Kind: "SealedSecret", Namespace: "prod", Name: "sealed-db", Spec: map[string]any{}},
			wantOK: true, wantName: "sealed-db", wantNS: "prod",
		},
		{
			name:   "non-producer kind",
			raw:    &RawObject{Kind: "Certificate", Namespace: "default", Name: "tls"},
			wantOK: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := ProducerTargetSecret(c.raw)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if !c.wantOK {
				return
			}
			want := NamedResource{Kind: KindSecret, Namespace: c.wantNS, Name: c.wantName}
			if got != want {
				t.Errorf("target = %v, want %v", got, want)
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
