package kustomization

import (
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkCollectDeps(b *testing.B) {
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "target"}
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []manifest.DependencyRef{{NamedResource: id}}}
	st := store.New()
	c := New(st, nil, nil, false)
	c.SetFilter(change.NewFilter(change.NewSet([]string{"target.yaml"}), map[manifest.NamedResource]string{id: "target.yaml"}, "", st))
	b.ReportAllocs()
	for b.Loop() {
		c.collectDeps(ks)
	}
}
