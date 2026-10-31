package manifest

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func BenchmarkReferences_InputProviders(b *testing.B) {
	for _, tc := range []struct {
		name string
		ref  fluxopv1.InputProviderReference
	}{
		{"named", fluxopv1.InputProviderReference{Name: "images"}},
		{"selector", fluxopv1.InputProviderReference{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "images"}}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			set := &ResourceSet{Name: "matrix", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{tc.ref}}
			count := 0
			b.ReportAllocs()
			for b.Loop() {
				for range References(set, ReferenceRelations) {
					count++
				}
			}
			if count == 0 {
				b.Fatal("reference iterator returned no inputs")
			}
		})
	}
}
