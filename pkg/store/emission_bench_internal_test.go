package store

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkRecordEmitted_BootstrapFeedback(b *testing.B) {
	seed := &manifest.Kustomization{Name: "generator", Namespace: "apps"}
	middle := &manifest.Kustomization{Name: "middle", Namespace: "apps"}
	applier := &manifest.Kustomization{Name: "sync", Namespace: "apps"}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		seed.Named(): {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
	}}
	b.ReportAllocs()
	for b.Loop() {
		s := New()
		s.AddObject(seed)
		s.SetDiscoveryIndex(index)
		s.RecordEmitted(seed.Named(), &Emission{Children: []Child{{Obj: middle}}, Complete: true}, nil)
		s.RecordEmitted(middle.Named(), &Emission{Children: []Child{{Obj: applier}}, Complete: true}, nil)
		s.RecordEmitted(applier.Named(), &Emission{Children: []Child{{Obj: seed}}, Complete: true}, nil)
	}
}
