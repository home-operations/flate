package depwait

import (
	"testing"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkClassify_Dependency(b *testing.B) {
	for _, state := range []string{"absent CRD", "present CRD", "ready ConfigMap", "pending Kustomization", "ready Kustomization", "CEL dep only", "CEL same identity", "CEL distinct self"} {
		b.Run(state, func(b *testing.B) {
			s := store.New()
			id := manifest.NamedResource{Group: apix.GroupName, Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}
			switch state {
			case "present CRD":
				s.AddObject(&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name})
			case "ready ConfigMap":
				id.Group = ""
				id.Kind = manifest.KindConfigMap
				id.Namespace = "ns"
				s.AddObject(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace})
			case "pending Kustomization":
				id.Group = manifest.FluxKustomizeDomain
				id.Kind = manifest.KindKustomization
				id.Namespace = "ns"
				s.UpdateStatus(id, store.StatusPending, "", "")
			case "ready Kustomization", "CEL dep only", "CEL same identity", "CEL distinct self":
				id = manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "dependency", Namespace: "ns"}
				s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
				s.UpdateStatus(id, store.StatusReady, "", "")
			}
			w := &Waiter{Store: s}
			dep := manifest.DependencyRef{NamedResource: id}
			switch state {
			case "CEL dep only", "CEL same identity", "CEL distinct self":
				dep.ReadyExpr = `dep.metadata.name == "dependency" && dep.status.conditions.exists(c, c.type == "Ready" && c.status == "True")`
				if state != "CEL dep only" {
					w.Parent = id
					if state == "CEL distinct self" {
						w.Parent.Name = "self"
						s.AddObject(&manifest.Kustomization{Name: w.Parent.Name, Namespace: id.Namespace})
						s.UpdateStatus(w.Parent, store.StatusReady, "", "")
					}
					dep.ReadyExpr += ` && self.status.conditions.exists(c, c.type == "Ready" && c.status == "True")`
				}
				w.Classify(b.Context(), dep, manifest.DrainCascade)
			}
			for b.Loop() {
				w.Classify(b.Context(), dep, manifest.DrainCascade)
			}
		})
	}
}
