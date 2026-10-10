package git

import (
	"os"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func BenchmarkResolveLocal(b *testing.B) {
	root, _, _, _ := localFixture(b)
	for _, name := range []string{"HEAD", "Cold", "Warm"} {
		b.Run(name, func(b *testing.B) {
			cacheRoot := b.TempDir()
			cache := source.NewCache(cacheroot.New(cacheRoot))
			tag := "v1.0.0"
			if name == "HEAD" {
				tag = "v2.0.0"
			}
			repositories := []*manifest.GitRepository{localSource(manifest.GitRepositoryRef{Tag: tag})}
			if _, err := ResolveLocal(b.Context(), root, repositories, cache); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if name == "Cold" {
					b.StopTimer()
					if err := os.RemoveAll(cacheRoot); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				if _, err := ResolveLocal(b.Context(), root, repositories, cache); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
