// Package helmchart resolves HelmRepository charts through the shared acquisition cache.
package helmchart

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type ociFetcher interface {
	FetchChart(context.Context, *manifest.OCIRepository, *store.ReadSet, cache.Policy) (*store.SourceArtifact, error)
}

// Fetcher resolves a HelmChart into an immutable source artifact.
type Fetcher struct {
	oci     ociFetcher   // OCI-backed charts (a synthesized OCIRepository)
	cache   *cache.Cache // content-addressed store for HTTP chart tarballs
	network *source.Network

	indexCache                 sync.Map                // map[string]*repo.IndexFile
	indexLocks                 *keylock.KeyMap[string] // serialize parsed memo publication
	indexDecodes               chan struct{}
	disableChartDigestTracking bool
}

// Config fixes acquisition and chart identity policy before reconciliation.
type Config struct {
	// Cache remains caller-owned and MUST outlive acquired artifacts.
	Cache *cache.Cache
	// Network is the required non-nil invocation owner, borrowed by this fetcher.
	Network *source.Network
	// Concurrency is the invocation's task budget, from which index decoding borrows a smaller share.
	Concurrency int
	// DisableChartDigestTracking makes exact chart versions immutable cache pins.
	DisableChartDigestTracking bool
}

// New constructs a fetcher borrowing the caller's cache and required non-nil Network.
func New(oci ociFetcher, cfg Config) *Fetcher {
	return &Fetcher{oci: oci, cache: cfg.Cache,
		network: cfg.Network, disableChartDigestTracking: cfg.DisableChartDigestTracking,
		indexLocks: keylock.New[string](), indexDecodes: make(chan struct{}, max(1, cfg.Concurrency/128))}
}

// Fetch implements source.TypedFetcher[*manifest.HelmChartSource]. It
// resolves the backing HelmRepository, then pulls the chart: OCI repos via
// the OCI fetcher (a synthesized OCIRepository), HTTP repos by streaming chart tarballs.
type operation struct {
	*Fetcher
	reads   *store.ReadSet
	secrets source.SecretGetter
}

// Fetch acquires content using one execution's captured credentials and declarations.
func (f *Fetcher) Fetch(ctx context.Context, hc *manifest.HelmChartSource, reads *store.ReadSet, policy cache.Policy) (*store.SourceArtifact, error) {
	operation := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads)}
	return operation.fetchSource(ctx, hc, policy)
}

func (f *operation) fetchSource(ctx context.Context, hc *manifest.HelmChartSource, policy cache.Policy) (*store.SourceArtifact, error) {
	r, _ := f.reads.Get[*manifest.HelmRepository](manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: hc.Namespace, Name: hc.SourceRef.Name})
	if r == nil {
		return nil, fmt.Errorf("%w: %s backing HelmChart %s", manifest.ErrObjectNotFound,
			source.QualifiedName("HelmRepository", hc.Namespace, hc.SourceRef.Name), hc.Named().String())
	}
	policy.MaxAge = r.SourceInterval()
	if f.disableChartDigestTracking && hc.ExactVersion() {
		policy.MaxAge = cache.Forever
	}
	if r.IsOCI() {
		syn := hc.OCIRepository(r)
		art, err := f.oci.FetchChart(ctx, syn, f.reads, policy)
		if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryAmbient != nil {
			retry := missing.RetryAmbient
			wrapped := *missing
			wrapped.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
				art, err := retry(ctx)
				if art != nil {
					art.Kind = manifest.KindHelmChart
				}
				return art, err
			}
			return nil, &wrapped
		}
		if err != nil {
			return nil, err
		}
		if art != nil {
			art.Kind = manifest.KindHelmChart
		}
		return art, nil
	}
	prepared, err := f.prepareHTTP(r)
	if err != nil {
		if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && prepared != nil {
			missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
				return f.fetchHTTPChart(ctx, prepared, hc, policy)
			}
		}
		return nil, err
	}
	return f.fetchHTTPChart(ctx, prepared, hc, policy)
}
