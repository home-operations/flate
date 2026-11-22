package filtertest

import (
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// CheckoutProvider admits supplied checkout changes before the fixture publishes material.
func CheckoutProvider(
	st *store.Store,
	artifactProvider func(manifest.NamedResource, *store.ReadSet, *manifest.DiscoveryIndex) store.SourceProvider,
) func(manifest.NamedResource, *store.ReadSet, *manifest.DiscoveryIndex) store.SourceProvider {
	bootstrap := manifest.BootstrapSourceID
	if st.GetArtifact(bootstrap, store.ArtifactContent) != nil || st.GetObject(bootstrap) != nil {
		return artifactProvider
	}
	return func(id manifest.NamedResource, reads *store.ReadSet, index *manifest.DiscoveryIndex) store.SourceProvider {
		provider := artifactProvider(id, reads, index)
		if id == bootstrap && provider == store.ProviderAbsent {
			return store.ProviderInput
		}
		return provider
	}
}
