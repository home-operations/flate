package resourceset

import (
	"fmt"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func BenchmarkCollectDeps_Scope(b *testing.B) {
	for _, target := range []struct{ api, kind string }{{"apiextensions.k8s.io/v1", manifest.KindCustomResourceDefinition}, {"v1", manifest.KindConfigMap}, {"example.com/v1", "Widget"}} {
		for _, hasParent := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/parent=%t", target.kind, hasParent), func(b *testing.B) {
				c := New(store.New(), task.NewBounded(2), true)
				opts := base.Options{}
				if hasParent {
					opts.ParentOf = func(manifest.NamedResource) (manifest.NamedResource, bool) {
						return testutil.KustomizationID("ns", "parent"), true
					}
				}
				c.Configure(Options{Options: opts})
				rs := &manifest.ResourceSet{Name: "app", Namespace: "ns", DependsOn: []fluxopv1.Dependency{{APIVersion: target.api, Kind: target.kind, Name: "dependency"}}}
				manifest.KindOf(&manifest.RawObject{APIVersion: target.api, Kind: target.kind}).Namespaced()
				for b.Loop() {
					c.collectDeps(rs)
				}
			})
		}
	}
}
