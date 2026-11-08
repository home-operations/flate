package loader

import (
	"fmt"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkSelfProduceBuilder_ArtifactJoin(b *testing.B) {
	obj := &manifest.RawObject{
		APIVersion: "source.extensions.fluxcd.io/v1beta1",
		Kind:       "ArtifactGenerator",
		Name:       "generator",
		Namespace:  "apps",
		Spec: map[string]any{
			"pathPattern": "@repo/{app}",
			"artifacts":   []any{map[string]any{"name": "app-{app}"}},
		},
	}
	consumers := make([]parsedObject, 100)
	for i := range consumers {
		consumers[i].Obj = &manifest.Kustomization{
			Namespace: "apps",
			Name:      fmt.Sprintf("consumer-%d", i),
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindExternalArtifact,
				Name: fmt.Sprintf("app-%d", i),
			},
		}
	}
	declaration := producerDeclaration{
		obj:      obj,
		identity: obj.Named(),
		authored: obj.Named(),
		visit:    graphVisit{source: manifest.BootstrapSourceID, coordinate: "generator.yaml"},
	}
	reader := documentReader{cache: map[string]document{"consumers.yaml": {objects: consumers}}}
	scope := []graphVisit{{file: "consumers.yaml", readable: true}}
	s := store.New()
	b.ReportAllocs()
	for b.Loop() {
		builder := selfProduceBuilder{
			idx:    &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			store:  s,
			reader: reader,
		}
		builder.joinProductions([]producerDeclaration{declaration}, scope)
	}
}
