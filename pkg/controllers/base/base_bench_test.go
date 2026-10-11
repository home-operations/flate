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

func BenchmarkEmit_Children(b *testing.B) {
	for _, kind := range []string{manifest.KindKustomization, manifest.KindResourceSet} {
		b.Run(kind, func(b *testing.B) {
			st := store.New()
			c := base.New(st, nil, "benchmark")
			parent := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "parent"}
			docs := []map[string]any{
				{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config", "namespace": "apps"}, "data": map[string]any{"key": "value"}},
				{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "child", "namespace": "apps"}, "spec": map[string]any{"path": "./child", "sourceRef": map[string]any{"kind": "GitRepository", "name": "repo"}}},
			}
			b.ReportAllocs()
			for b.Loop() {
				c.Emit(parent, docs, false, true)
			}
		})
	}
}
