package oci

import (
	"encoding/json"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

func TestOCICacheKey_RulesVersion(t *testing.T) {
	repo := &manifest.OCIRepository{}
	ref := manifest.OCIRepositoryRef{Tag: "v1"}
	const prefix = "v1#opts:"
	v1 := ociCacheKey(repo, ref, "", "sourceignore-v1")
	assert.Equal(t, sourceignoreRulesVersion, "sourceignore-v1")
	assert.Equal(t, v1, ociCacheKey(repo, ref, "", "sourceignore-v1"))
	assert.Equal(t, strings.HasPrefix(v1, prefix), true)
	assert.Equal(t, len(v1), len(prefix)+16)
	assert.Equal(t, ociResolveCacheKey(repo, ref), resolveCachePrefix+v1)
	legacyHash, err := source.CacheKeyHash(json.RawMessage(`{"ref":"v1","layerOperation":"extract"}`), 8)
	if err != nil {
		t.Fatal(err)
	}
	legacy := prefix + legacyHash
	for _, tc := range []struct {
		name string
		got  string
		want string
	}{
		{name: "different version", got: ociCacheKey(repo, ref, "", "sourceignore-v2"), want: v1},
		{name: "legacy payload", got: v1, want: legacy},
		{name: "empty version still present", got: ociCacheKey(repo, ref, "", ""), want: legacy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.got == tc.want, false)
			assert.Equal(t, strings.HasPrefix(tc.got, prefix), true)
		})
	}
}

func TestOCICacheKey_Options(t *testing.T) {
	ref := manifest.OCIRepositoryRef{Tag: "v1"}
	base := ociCacheKey(&manifest.OCIRepository{}, ref, "", sourceignoreRulesVersion)
	for _, tc := range []struct {
		name     string
		repo     *manifest.OCIRepository
		ref      manifest.OCIRepositoryRef
		resolved string
		wantSame bool
	}{
		{name: "nil ignore", repo: &manifest.OCIRepository{}, ref: ref, wantSame: true},
		{name: "empty ignore", repo: &manifest.OCIRepository{Ignore: new("")}, ref: ref, wantSame: true},
		{name: "ignore", repo: &manifest.OCIRepository{Ignore: new("*.tmp\n")}, ref: ref},
		{name: "empty layer selector", repo: &manifest.OCIRepository{LayerSelector: &sourcev1.OCILayerSelector{}}, ref: ref, wantSame: true},
		{name: "explicit extract", repo: &manifest.OCIRepository{LayerSelector: &sourcev1.OCILayerSelector{Operation: manifest.OCILayerOperationExtract}}, ref: ref, wantSame: true},
		{name: "copy", repo: &manifest.OCIRepository{LayerSelector: &sourcev1.OCILayerSelector{Operation: manifest.OCILayerOperationCopy}}, ref: ref},
		{name: "media type", repo: &manifest.OCIRepository{LayerSelector: &sourcev1.OCILayerSelector{MediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"}}, ref: ref},
		{name: "tag", repo: &manifest.OCIRepository{}, ref: manifest.OCIRepositoryRef{Tag: "v2"}},
		{name: "digest", repo: &manifest.OCIRepository{}, ref: manifest.OCIRepositoryRef{Digest: fullDigest}},
		{name: "semver", repo: &manifest.OCIRepository{}, ref: manifest.OCIRepositoryRef{SemVer: ">=1.0.0"}},
		{name: "latest", repo: &manifest.OCIRepository{}},
		{name: "resolved digest", repo: &manifest.OCIRepository{}, ref: ref, resolved: fullDigest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ociCacheKey(tc.repo, tc.ref, tc.resolved, sourceignoreRulesVersion)
			assert.Equal(t, got == base, tc.wantSame)
		})
	}
}
