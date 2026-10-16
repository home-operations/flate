package loader

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func promoteForTest(st *store.Store, id manifest.NamedResource, r manifest.DiscoveryRecord, opts PromoteOptions) bool {
	return Promote(st, id, manifest.Production{Authored: r.Authored, Path: r.Path, File: r.File}, opts)
}
