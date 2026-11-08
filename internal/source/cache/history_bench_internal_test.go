package cache

import (
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkProducersForPaths(b *testing.B) {
	for _, count := range []int{0, 32, 1024} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			cache := cachetest.New(b, b.TempDir(), New)
			cache.persistBlob = true
			source := manifest.SourceIdentity{Root: "/input"}
			for i := range count {
				name := strconv.Itoa(i)
				owner := manifest.NamedResource{
					Group: manifest.HelmReleaseDomain,
					Kind:  manifest.KindHelmRelease,
					Name:  name,
				}
				err := RecordInventory(
					b.Context(),
					cache,
					AcceptedInventory[*strings.Reader]{
						Key:        name,
						Acceptance: name,
						History: RenderHistory{
							Owner:   owner,
							Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/" + name, Subtree: true}},
						},
						Content: strings.NewReader(name),
					},
				)
				assert.NoError(b, err)
			}
			query := []manifest.SourceEvidence{{Source: source, Path: "apps/0/cm.yaml"}}
			b.ReportAllocs()
			for b.Loop() {
				owners, err := cache.ProducersForPaths(b.Context(), query)
				assert.FatalIf(b, err != nil || len(owners) != min(count, 1), "owners=%v error=%v", owners, err)
			}
		})
	}
}
