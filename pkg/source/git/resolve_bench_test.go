package git

import (
	"fmt"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
)

func BenchmarkResolveSemver_MatchingTags(b *testing.B) {
	_, repo, a, _ := localFixture(b)
	for i := range 100 {
		ref := plumbing.NewHashReference(plumbing.NewTagReferenceName(fmt.Sprintf("v1.0.%d", i)), a)
		if err := repo.Storer.SetReference(ref); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := resolveSemver(repo, "<2.0.0"); err != nil {
			b.Fatal(err)
		}
	}
}
