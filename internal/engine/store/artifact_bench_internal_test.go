package store

import "testing"

func BenchmarkSourceArtifact_RootCoordinates(b *testing.B) {
	for _, tc := range []struct {
		name string
		path func(string, string) (string, bool)
		root string
	}{
		{name: "index", path: (&SourceArtifact{LocalPath: "/repo/nested"}).IndexPath, root: "/repo"},
		{name: "source", path: (&SourceArtifact{LocalPath: "/repo"}).SourcePath, root: "/repo/nested"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tc.path(tc.root, ".")
			}
		})
	}
}
