package git

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
	assert.Equal(t, len(v1), len(prefix)+64)
	const expected = prefix + "cc5cc365bec71d99"
	for _, tc := range []struct {
		name string
		got  string
		want string
	}{
		{name: "different version", got: gitCacheKey(repo, ref, "sourceignore-v2"), want: v1},
		{name: "expected payload", got: v1, want: expected},
		{name: "empty version still present", got: gitCacheKey(repo, ref, ""), want: expected},
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
		wantKey  string
	}{
		{name: "nil ignore", repo: &manifest.GitRepository{}, ref: ref, wantSame: true, wantKey: "branch:main#opts:e6aeca4a3a0612cabc841da5bc5328f2a38cd56692d1b387dda6d38da3e9790c"},
		{name: "empty ignore", repo: &manifest.GitRepository{Ignore: new("")}, ref: ref, wantSame: true, wantKey: "branch:main#opts:e6aeca4a3a0612cabc841da5bc5328f2a38cd56692d1b387dda6d38da3e9790c"},
		{name: "ignore", repo: &manifest.GitRepository{Ignore: new("*.tmp\n")}, ref: ref, wantKey: "branch:main#opts:ba94a0ef059f7d30207b779f8eaa4e38ad286e7d1b855c993b1a93ce6109eafa"},
		{name: "empty sparse checkout", repo: &manifest.GitRepository{SparseCheckout: []string{}}, ref: ref, wantSame: true, wantKey: "branch:main#opts:e6aeca4a3a0612cabc841da5bc5328f2a38cd56692d1b387dda6d38da3e9790c"},
		{name: "sparse checkout", repo: &manifest.GitRepository{SparseCheckout: []string{"app"}}, ref: ref, wantKey: "branch:main#opts:34e1afc670c5e76e8721aeccfde56c5d2a3c14b0899fa8b723ba4f200e499061"},
		{name: "submodules", repo: &manifest.GitRepository{RecurseSubmodules: true}, ref: ref, wantKey: "branch:main#opts:d9e26b4d6bb6493dbaf6ce8b7317c7762782b31c39ccc1f17735d3570a729cf9"},
		{name: "escaping", repo: &manifest.GitRepository{Ignore: new("<&é>"), SparseCheckout: []string{"前/"}, RecurseSubmodules: true}, ref: "branch:图", wantKey: "branch:图#opts:2f79a805391d9673226b8474946f2b162daa1c118bf7cc9f3c411bea239c0b72"},
		{name: "ref", repo: &manifest.GitRepository{}, ref: "branch:other", wantKey: "branch:other#opts:94e404972580b767e0554260347b071a973162a85197795156516076e19931af"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := gitCacheKey(tc.repo, tc.ref, sourceignore.RulesVersion)
			assert.Equal(t, got, tc.wantKey)
			assert.Equal(t, got == base, tc.wantSame)
		})
	}
}
