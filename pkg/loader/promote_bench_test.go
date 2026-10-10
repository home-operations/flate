package loader

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkPromoteAvailable(b *testing.B) {
	st := store.New()
	cm := &manifest.ConfigMap{Name: "values", Namespace: "apps"}
	st.AddObject(cm)
	idx := loaderFixture(st, nil)
	idx.Index.Records[cm.Named()] = manifest.DiscoveryRecord{Deferred: true, Path: "values.yaml"}
	b.ReportAllocs()
	for b.Loop() {
		if !idx.Promote(cm.Named(), true, func(manifest.BaseManifest) bool { return true }) {
			b.Fatal("available ConfigMap was not found")
		}
	}
}
