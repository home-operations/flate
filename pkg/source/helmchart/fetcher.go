// Package helmchart resolves HelmRepository charts through the shared acquisition cache.
package helmchart

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type ociFetcher interface {
	FetchChart(context.Context, *manifest.OCIRepository, *store.ReadSet, *cache.Policy) (*store.SourceArtifact, error)
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
	const indexDecodeShare = 128
	return &Fetcher{
		oci: oci, cache: cfg.Cache,
		network: cfg.Network, disableChartDigestTracking: cfg.DisableChartDigestTracking,
		indexLocks: keylock.New[string](), indexDecodes: make(chan struct{}, max(1, cfg.Concurrency/indexDecodeShare)),
	}
}

// Fetch implements source.TypedFetcher[*manifest.HelmChartSource]. It
// resolves the backing HelmRepository, then pulls the chart: OCI repos via
// the OCI fetcher (a synthesized OCIRepository), HTTP repos by streaming chart tarballs.
type operation struct {
	*Fetcher

	work      manifest.AcquisitionWork
	indexWork manifest.AcquisitionWork

	reads   *store.ReadSet
	secrets source.SecretGetter
}

// Fetch acquires content using one execution's captured credentials and declarations.
func (f *Fetcher) Fetch(
	ctx context.Context,
	hc *manifest.HelmChartSource,
	reads *store.ReadSet,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	innerOperation := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads)}
	return innerOperation.fetchSource(ctx, hc, &policy)
}

func (f *operation) fetchSource(
	ctx context.Context,
	hc *manifest.HelmChartSource,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	f.work.Scope = f.reads.Work().Scope().Source(hc.Named())
	if policy.Work != nil {
		f.work.Scope = policy.Work.Attribution().Source(hc.Named())
	}
	policy.Work = &f.work
	f.indexWork.Scope = f.work.Scope
	policy.HistoryWork = &f.indexWork
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmRepository,
		Namespace: hc.Namespace,
		Name:      hc.SourceRef.Name,
	}
	r, _ := f.reads.Get[*manifest.HelmRepository](id)
	if r == nil {
		return nil, manifest.NewError(
			manifest.ErrObjectNotFound,
			nil,
			fmt.Sprintf("%s not found", manifest.DiagnosticFor(id, nil).Label()),
		)
	}
	policy.MaxAge = r.SourceInterval()
	if f.disableChartDigestTracking && hc.ExactVersion() {
		policy.MaxAge = cache.Forever
	}
	if r.IsOCI() {
		return f.fetchOCIChart(ctx, hc, r, &policy)
	}

	prepared, err := f.prepareHTTP(r)
	if err != nil {
		if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && prepared != nil {
			timeout := r.OperationTimeout()
			policy.Timeout = &timeout
			missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
				local := struct {
					httpRepository

					work, indexWork manifest.AcquisitionWork
				}{httpRepository: *prepared}
				local.work.Scope = f.work.Scope
				local.indexWork.Scope = local.work.Scope
				policy := policy
				policy.Work, policy.HistoryWork = &local.work, &local.indexWork
				return source.AcquireMachine(ctx, &policy, func(ctx context.Context) error {
					if localCredentialsErr := local.localCredentials(ctx); localCredentialsErr != nil {
						return localCredentialsErr
					}
					credentials := [2]string{local.username, local.password}
					local.material = local.resolved.Identity(credentials)
					tr, httpTransportErr := f.network.HTTPTransport(
						source.HTTPTransportConfig{
							Material:           local.resolved,
							Credentials:        credentials,
							DisableCompression: true,
						},
					)
					local.client = &http.Client{Transport: tr}
					return httpTransportErr
				}, func(ctx context.Context, policyValue *cache.Policy) (*store.SourceArtifact, error) {
					policy := *policyValue

					return f.fetchHTTPChart(ctx, &local.httpRepository, hc, &policy)
				})
			}
		}
		return nil, err
	}
	return f.fetchHTTPChart(ctx, prepared, hc, &policy)
}

func (f *operation) fetchOCIChart(
	ctx context.Context,
	hc *manifest.HelmChartSource,
	r *manifest.HelmRepository,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	syn := hc.OCIRepository(r)
	art, err := f.oci.FetchChart(ctx, syn, f.reads, &policy)
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryAmbient != nil {
		retry := missing.RetryAmbient
		wrapped := *missing
		wrapped.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
			retryArt, retryErr := retry(ctx)
			if retryArt != nil {
				retryArt.Kind = manifest.KindHelmChart
			}
			return retryArt, retryErr
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
