package resourceset

import (
	"fmt"
	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"testing"
)

func BenchmarkCollectDeps_Scope(b *testing.B) {
	for _, kind := range []string{manifest.KindCustomResourceDefinition, manifest.KindConfigMap, "Widget"} {
		for _, hasParent := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/parent=%t", kind, hasParent), func(b *testing.B) {
				c := New(store.New(), task.NewBounded(2), true)
				opts := base.Options{}
				if hasParent {
					opts.ParentOf = func(manifest.NamedResource) (manifest.NamedResource, bool) {
						return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "parent"}, true
					}
				}
				c.Configure(Options{Options: opts})
				rs := &manifest.ResourceSet{Name: "app", Namespace: "ns", DependsOn: []fluxopv1.Dependency{{Kind: kind, Name: "dependency"}}}
				for b.Loop() {
					c.collectDeps(rs)
				}
			})
		}
	}
}
