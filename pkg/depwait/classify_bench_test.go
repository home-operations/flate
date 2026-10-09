package depwait

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkClassify_Dependency(b *testing.B) {
	for _, state := range []string{"absent CRD", "present CRD", "ready ConfigMap", "pending Kustomization"} {
		b.Run(state, func(b *testing.B) {
			s := store.New()
			id := manifest.NamedResource{Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}
			switch state {
			case "present CRD":
				s.AddObject(&manifest.RawObject{Kind: id.Kind, Name: id.Name})
			case "ready ConfigMap":
				id.Kind = manifest.KindConfigMap
				id.Namespace = "ns"
				s.AddObject(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace})
			case "pending Kustomization":
				id.Kind = manifest.KindKustomization
				id.Namespace = "ns"
				s.UpdateStatus(id, store.StatusPending, "")
			}
			w := &Waiter{Store: s}
			dep := manifest.DependencyRef{NamedResource: id}
			for b.Loop() {
				w.Classify(dep, drainCascade)
			}
		})
	}
}
