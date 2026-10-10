package base_test

import (
	"context"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkDispatchNodeReady(b *testing.B) {
	st := store.New()
	obj := &manifest.HelmRelease{Name: "app", Namespace: "apps"}
	st.AddObject(obj)
	st.UpdateStatus(obj.Named(), store.StatusReady, "")
	c := base.New(st, nil, "benchmark")
	suspended := func(*manifest.HelmRelease) bool { return false }
	reconcile := func(context.Context, *manifest.HelmRelease, manifest.DrainLevel) error { return nil }
	b.ReportAllocs()
	for b.Loop() {
		c.DispatchNode(b.Context(), obj.Named(), manifest.DrainNone, base.Dispatch[*manifest.HelmRelease]{
			Suspended: suspended, Reconcile: reconcile,
		})
	}
}

func BenchmarkRequire_Dependency(b *testing.B) {
	for _, state := range []store.Status{store.StatusReady, store.StatusPending, store.StatusFailed} {
		b.Run(string(state), func(b *testing.B) {
			st := store.New()
			obj := &manifest.Kustomization{Name: "dep", Namespace: "apps"}
			st.AddObject(obj)
			st.UpdateStatus(obj.Named(), state, "reason")
			c := base.New(st, nil, "benchmark")
			deps := []manifest.DependencyRef{{NamedResource: obj.Named()}}
			id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "consumer"}
			b.ReportAllocs()
			for b.Loop() {
				_ = c.Require(b.Context(), id, deps, manifest.DrainNone)
			}
		})
	}
}
