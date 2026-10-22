package base_test

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkDispatchNodeReady(b *testing.B) {
	st := store.New()
	obj := &manifest.HelmRelease{Name: "app", Namespace: "apps"}
	st.AddObject(obj)
	st.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	c := base.New(st, nil)
	reconcile := func(context.Context, *manifest.HelmRelease, manifest.DrainLevel) error { return nil }
	b.ReportAllocs()
	for b.Loop() {
		execution := controllertest.NewExecution(c, obj.Named())
		execution.Dispatch(b.Context(), manifest.DrainNone, reconcile)
		execution.Close()
		for range st.Sweep() {
		}
	}
}

func BenchmarkRequire_Dependency(b *testing.B) {
	for _, state := range []store.Status{store.StatusReady, store.StatusPending, store.StatusFailed} {
		b.Run(string(state), func(b *testing.B) {
			st := store.New()
			obj := &manifest.Kustomization{Name: "dep", Namespace: "apps"}
			st.AddObject(obj)
			st.UpdateStatus(obj.Named(), store.StatusInfo{Status: state, Reason: "", Message: "reason"})
			c := base.New(st, nil)
			deps := []manifest.DependencyRef{{NamedResource: obj.Named()}}
			id := testutil.HelmReleaseID("apps", "consumer")
			execution := controllertest.NewExecution(c, id)
			defer execution.Close()
			b.ReportAllocs()
			for b.Loop() {
				_ = execution.Require(b.Context(), deps, manifest.DrainNone)
			}
		})
	}
}

func BenchmarkEmit_Children(b *testing.B) {
	for _, kind := range []manifest.KindInfo{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet}} {
		b.Run(kind.Kind, func(b *testing.B) {
			st := store.New()
			c := base.New(st, nil)
			parent := manifest.NamedResource{Group: kind.Group, Kind: kind.Kind, Namespace: "apps", Name: "parent"}
			docs := []map[string]any{
				{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config", "namespace": "apps"}, "data": map[string]any{"key": "value"}},
				{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "child", "namespace": "apps"}, "spec": map[string]any{"path": "./child", "sourceRef": map[string]any{"kind": "GitRepository", "name": "repo"}}},
			}
			b.ReportAllocs()
			for b.Loop() {
				owner := &manifest.RawObject{APIVersion: parent.KindInfo().APIVersion(), Kind: parent.Kind, Namespace: parent.Namespace, Name: parent.Name}
				c.Store.AddObject(owner)
				children, visible := c.ParseChildren(owner, docs, manifest.ParseDocOptions{DefaultNamespace: parent.Namespace})
				_, next := st.GetEmission(parent)
				next.Children, next.Manifests, next.Complete = children, visible, true
				c.Emit(parent, &next)
				next.Finish()
			}
		})
	}
}
