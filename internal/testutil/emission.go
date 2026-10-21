package testutil

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// ProductionEvidence supplies fixture ownership without authored coordinates.
func ProductionEvidence(ids []manifest.NamedResource) []manifest.Production {
	var out []manifest.Production
	for _, id := range ids {
		out = append(out, manifest.Production{Owner: id})
	}
	return out
}

// ProductionOwners returns sorted unique owners from frozen discovery evidence.
func ProductionOwners(r manifest.DiscoveryRecord) []manifest.NamedResource {
	var ids []manifest.NamedResource
	for _, p := range r.Productions {
		if p.Owner != (manifest.NamedResource{}) && !slices.Contains(ids, p.Owner) {
			ids = append(ids, p.Owner)
		}
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return ids
}

// Owners returns sorted unique owners from borrowed production evidence.
func Owners(productions []manifest.Production) []manifest.NamedResource {
	var ids []manifest.NamedResource
	for _, p := range productions {
		if p.Owner != (manifest.NamedResource{}) && !slices.Contains(ids, p.Owner) {
			ids = append(ids, p.Owner)
		}
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return ids
}

// Declarations returns sorted unique declarations from borrowed production evidence.
func Declarations(productions []manifest.Production) []manifest.NamedResource {
	var ids []manifest.NamedResource
	for _, p := range productions {
		if p.Declaration != (manifest.NamedResource{}) && !slices.Contains(ids, p.Declaration) {
			ids = append(ids, p.Declaration)
		}
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return ids
}
