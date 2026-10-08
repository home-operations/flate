package loader

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkBuildParentIndex(b *testing.B) {
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	prefixes := []KSPathPrefix{{ID: parent, Prefix: "apps/"}}
	files := make(map[manifest.NamedResource]string, 1000)
	for i := range 1000 {
		hr := &manifest.HelmRelease{Name: fmt.Sprintf("app-%d", i), Namespace: "apps"}
		files[hr.Named()] = fmt.Sprintf("apps/app-%d/release.yaml", i)
	}
	b.ReportAllocs()
	for b.Loop() {
		BuildParentIndexFromPrefixes(prefixes, files, manifest.KindHelmRelease)
	}
}
