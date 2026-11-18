package loader

import (
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func claimsForTest(s *store.Store, root string, candidates []*manifest.Kustomization) []manifest.KSClaim {
	index := Claims(s, root, candidates)
	var claims []manifest.KSClaim
	for prefix, ids := range index.Claims {
		for _, id := range ids {
			claims = append(claims, manifest.KSClaim{ID: id, Prefix: prefix})
		}
	}
	slices.SortFunc(claims, func(a, b manifest.KSClaim) int {
		if order := len(b.Prefix) - len(a.Prefix); order != 0 {
			return order
		}
		if order := a.ID.Compare(b.ID); order != 0 {
			return order
		}
		return strings.Compare(a.Prefix, b.Prefix)
	})
	return claims
}

func claimParentForTest(
	claims []manifest.KSClaim,
	file string,
	self manifest.NamedResource,
) (manifest.NamedResource, bool) {
	index := manifest.DiscoveryIndex{Claims: map[string][]manifest.NamedResource{}}
	for _, claim := range claims {
		index.Claims[claim.Prefix] = append(index.Claims[claim.Prefix], claim.ID)
	}
	for owners := range index.ClaimOwners(file, manifest.NamedResource{}, self, nil) {
		return owners[0], true
	}
	return manifest.NamedResource{}, false
}
