package resourceset_test

import (
	"fmt"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkRender_Matrix(b *testing.B) {
	for _, strategy := range []string{fluxopv1.InputStrategyFlatten, fluxopv1.InputStrategyPermute} {
		b.Run(strategy, func(b *testing.B) {
			rs := &manifest.ResourceSet{Name: "apps", Namespace: "default", InputStrategy: &fluxopv1.InputStrategySpec{Name: strategy}, Resources: []*apix.JSON{{Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"<< inputs.name >>"}}`)}}}
			if strategy == fluxopv1.InputStrategyPermute {
				rs.Resources[0].Raw = []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"<< inputs.apps.name >>"}}`)
			}
			for i := range 10 {
				rs.Inputs = append(rs.Inputs, fluxopv1.ResourceSetInput{"name": {Raw: []byte(fmt.Sprintf(`"app-%d"`, i))}})
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := resourceset.Render(rs, nil, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("resourcesTemplate", func(b *testing.B) {
		rs := &manifest.ResourceSet{Name: "apps", Namespace: "default", ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.name >>'}\n---\napiVersion: v1\nkind: Service\nmetadata: {name: '<< inputs.name >>'}\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata: {name: '<< inputs.name >>'}"}
		for i := range 50 {
			rs.Inputs = append(rs.Inputs, fluxopv1.ResourceSetInput{"name": {Raw: []byte(fmt.Sprintf(`"app-%d"`, i))}})
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := resourceset.Render(rs, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkStoreResolver_SelectorCandidates(b *testing.B) {
	for _, count := range []int{1, 1000} {
		b.Run(fmt.Sprintf("candidates=%d", count), func(b *testing.B) {
			st := store.New()
			for i := range count {
				role := "db"
				if i%2 != 0 {
					role = "web"
				}
				st.AddObject(&manifest.ResourceSetInputProvider{Name: fmt.Sprintf("provider-%04d", i), Namespace: "apps", Labels: map[string]string{"role": role}})
			}
			ref := fluxopv1.InputProviderReference{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}}
			id := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps"}
			b.ReportAllocs()
			for b.Loop() {
				resolve := resourceset.StoreResolver(st)
				for range 2 {
					got, err := resolve(ref, id)
					if err != nil || len(got) != (count+1)/2 {
						b.Fatalf("matches=%d error=%v, want %d", len(got), err, (count+1)/2)
					}
				}
			}
		})
	}
}
