package helmrelease

import (
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkCollectHRDeps(b *testing.B) {
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "target"}
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []manifest.DependencyRef{{NamedResource: id}}}
	st := store.New()
	c := &Controller{Controller: base.New(st, nil, "helmrelease")}
	c.SetFilter(change.NewFilter(change.NewSet([]string{"target.yaml"}), map[manifest.NamedResource]string{id: "target.yaml"}, "", st))
	b.ReportAllocs()
	for b.Loop() {
		c.collectHRDeps(hr)
	}
}
