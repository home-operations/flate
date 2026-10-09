package git

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

func TestGitCacheKey_RulesVersion(t *testing.T) {
	repo := &manifest.GitRepository{}
	const ref = "branch:main"
	const prefix = ref + "#opts:"
	v1 := gitCacheKey(repo, ref, sourceignore.RulesVersion)
	assert.Equal(t, sourceignore.RulesVersion, "sourceignore-v1")
	assert.Equal(t, v1, gitCacheKey(repo, ref, sourceignore.RulesVersion))
	assert.Equal(t, strings.HasPrefix(v1, prefix), true)
	assert.Equal(t, len(v1), len(prefix)+16)
	legacyHash, err := source.CacheKeyHash(json.RawMessage(`{"ref":"branch:main"}`), 8)
	if err != nil {
		t.Fatal(err)
	}
	legacy := prefix + legacyHash
	for _, tc := range []struct {
		name string
		got  string
		want string
	}{
		{name: "different version", got: gitCacheKey(repo, ref, "sourceignore-v2"), want: v1},
		{name: "legacy payload", got: v1, want: legacy},
		{name: "empty version still present", got: gitCacheKey(repo, ref, ""), want: legacy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.got == tc.want, false)
			assert.Equal(t, strings.HasPrefix(tc.got, prefix), true)
		})
	}
}

func TestGitCacheKey_Options(t *testing.T) {
	const ref = "branch:main"
	base := gitCacheKey(&manifest.GitRepository{}, ref, sourceignore.RulesVersion)
	for _, tc := range []struct {
		name     string
		repo     *manifest.GitRepository
		ref      string
		wantSame bool
	}{
		{name: "nil ignore", repo: &manifest.GitRepository{}, ref: ref, wantSame: true},
		{name: "empty ignore", repo: &manifest.GitRepository{Ignore: new("")}, ref: ref, wantSame: true},
		{name: "ignore", repo: &manifest.GitRepository{Ignore: new("*.tmp\n")}, ref: ref},
		{name: "empty sparse checkout", repo: &manifest.GitRepository{SparseCheckout: []string{}}, ref: ref, wantSame: true},
		{name: "sparse checkout", repo: &manifest.GitRepository{SparseCheckout: []string{"app"}}, ref: ref},
		{name: "submodules", repo: &manifest.GitRepository{RecurseSubmodules: true}, ref: ref},
		{name: "ref", repo: &manifest.GitRepository{}, ref: "branch:other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := gitCacheKey(tc.repo, tc.ref, sourceignore.RulesVersion)
			assert.Equal(t, got == base, tc.wantSame)
		})
	}
}
