package loader

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkBuildParentIndex(b *testing.B) {
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	st := store.New()
	st.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace, Path: "apps"})
	files := make(map[manifest.NamedResource]string, 1000)
	for i := range 1000 {
		hr := &manifest.HelmRelease{Name: fmt.Sprintf("app-%d", i), Namespace: "apps"}
		files[hr.Named()] = fmt.Sprintf("apps/app-%d/release.yaml", i)
	}
	l := loaderFixture(st, files)
	b.ReportAllocs()
	for b.Loop() {
		l.FinalizeIndex("", nil)
	}
}
