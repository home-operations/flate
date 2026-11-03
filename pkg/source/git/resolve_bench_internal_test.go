package git

import (
	"fmt"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
)

func BenchmarkResolveSemver_MatchingTags(b *testing.B) {
	_, repo, a, _ := localFixture(b)
	for i := range 100 {
		ref := plumbing.NewHashReference(plumbing.NewTagReferenceName(fmt.Sprintf("v1.0.%d", i)), a)
		{
			err := repo.Storer.SetReference(ref)
			assert.NoError(b, err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		{
			_, err := resolveSemver(repo, "<2.0.0")
			assert.NoError(b, err)
		}
	}
}
