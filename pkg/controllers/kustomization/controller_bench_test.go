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

func BenchmarkSubstitutionProvider_Lookup(b *testing.B) {
	for _, supplied := range []bool{false, true} {
		name := "repository"
		if supplied {
			name = "supplied"
		}
		b.Run(name, func(b *testing.B) {
			st := store.New()
			cm := &manifest.ConfigMap{Name: "external", Namespace: "apps"}
			c := New(st, nil, nil, false)
			opts := Options{}
			if supplied {
				opts.SubstituteFrom = map[manifest.NamedResource]manifest.BaseManifest{cm.Named(): cm}
			} else {
				st.AddObject(cm)
			}
			c.Configure(opts)
			b.ReportAllocs()
			for b.Loop() {
				if c.substitutionProvider.ConfigMap("apps", "external") != cm {
					b.Fatal("lookup failed")
				}
			}
		})
	}
}
