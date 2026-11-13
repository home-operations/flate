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
	idx.Index.Records[cm.Named()] = manifest.DiscoveryRecord{Authored: cm.Named(), Path: "values.yaml"}
	b.ReportAllocs()
	for b.Loop() {
		if promoted, err := promoteForTest(b.Context(),
			b,
			idx.Store,
			cm.Named(),
			PromoteOptions{
				Record: productionForTest(new(idx.Index.Records[cm.Named()])),
			},
		); err != nil ||
			!promoted {
			b.Fatal("available ConfigMap was not found")
		}
	}
}
