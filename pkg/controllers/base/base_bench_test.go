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
	reconcile := func(context.Context, *manifest.HelmRelease) error { return nil }
	b.ReportAllocs()
	for b.Loop() {
		c.DispatchNode(b.Context(), obj.Named(), 0, suspended, reconcile)
	}
}
