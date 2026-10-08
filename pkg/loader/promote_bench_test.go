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
	idx := NewExistenceIndex()
	idx.Record(cm.Named(), "values.yaml")
	b.ReportAllocs()
	for b.Loop() {
		if !idx.Promote(st, cm.Named(), true, func(manifest.BaseManifest) bool { return true }) {
			b.Fatal("available ConfigMap was not found")
		}
	}
}
