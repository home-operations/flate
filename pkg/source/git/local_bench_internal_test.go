package git

import (
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkResolveLocal(b *testing.B) {
	fixture := localFixture(b)

	location := localLocation(b, fixture.root)
	for _, name := range []string{"HEAD", "Cold", "Warm"} {
		b.Run(name, func(b *testing.B) {
			tag := "v1.0.0"
			if name == "HEAD" {
				tag = "v2.0.0"
			}
			repositories := []*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{Tag: tag})}
			{
				_, err := ResolveLocal(b.Context(), location, repositories, LocalOptions{Revision: ""})
				assert.NoError(b, err)
			}

			b.ReportAllocs()
			for b.Loop() {
				{
					_, err := ResolveLocal(b.Context(), location, repositories, LocalOptions{Revision: ""})
					assert.NoError(b, err)
				}
			}
		})
	}
}
