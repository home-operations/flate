// Package helmchart resolves HelmRepository charts through the shared acquisition cache.
package helmchart

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/core/keylock"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type ociFetcher interface {
	FetchChart(context.Context, *manifest.OCIRepository, *store.ReadSet, *cache.Policy) (source.PreparedFetch, error)
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
) (source.PreparedFetch, error) {
	policy := *policyValue
	innerOperation := operation{
		Fetcher: f,
		reads:   reads,
		secrets: source.SecretLookup(reads),
	}
	return innerOperation.fetchSource(ctx, hc, &policy)
}

func (f *operation) fetchSource(
	ctx context.Context,
	hc *manifest.HelmChartSource,
	policyValue *cache.Policy,
) (source.PreparedFetch, error) {
	policy := *policyValue
	if err := ctx.Err(); err != nil {
		return source.PreparedFetch{}, fmt.Errorf("prepare helm chart: %w", err)
	}

	var dependency manifest.DependencyRef
	for ref := range manifest.References(hc, manifest.ReferenceAll) {
		if ref.Gate&manifest.GateInputs != 0 {
			dependency = ref.DependencyRef
			break
		}
	}
	id := dependency.NamedResource
	r, _ := f.reads.Get[*manifest.HelmRepository](id)
	if r == nil {
		return source.PreparedFetch{Inputs: []manifest.DependencyRef{dependency}}, manifest.NewError(
			manifest.ErrObjectNotFound,
			nil,
			fmt.Sprintf("%s not found", manifest.DiagnosticFor(id, nil).Label()),
		)
	}
	return f.fetchRepository(ctx, hc, r, &policy)
}

func (f *operation) fetchRepository(
	ctx context.Context,
	hc *manifest.HelmChartSource,
	r *manifest.HelmRepository,
	policyValue *cache.Policy,
) (source.PreparedFetch, error) {
	policy := *policyValue
	f.work.Scope = f.reads.Work().Scope().Source(hc.Named())
	if policy.Work != nil {
		f.work.Scope = policy.Work.Attribution().Source(hc.Named())
	}
	policy.Work = &f.work
	f.indexWork.Scope = f.work.Scope
	policy.HistoryWork = &f.indexWork

	policy.MaxAge = r.SourceInterval()
	if f.disableChartDigestTracking && hc.ExactVersion() {
		policy.MaxAge = cache.Forever
	}
	if r.IsOCI() {
		return f.fetchOCIChart(ctx, hc, r, &policy)
	}

	return f.fetchHTTPSource(hc, r, &policy)
}

func (f *operation) fetchHTTPSource(
	hc *manifest.HelmChartSource,
	r *manifest.HelmRepository,
	policyValue *cache.Policy,
) (source.PreparedFetch, error) {
	policy := *policyValue
	prepared, err := f.prepareHTTP(r)
	if err != nil {
		if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && prepared != nil {
			timeout := r.OperationTimeout()
			policy.Timeout = &timeout
			missing.RetryAmbient = f.machineHTTP(hc, prepared, &policy)
		}
		return source.PreparedFetch{}, err
	}
	return source.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}
			if err := f.connectHTTP(prepared); err != nil {
				return nil, err
			}
			return f.fetchHTTPChart(ctx, prepared, hc, &policy)
		},
	}, nil
}

func (f *operation) machineHTTP(
	hc *manifest.HelmChartSource,
	prepared *httpRepository,
	policyValue *cache.Policy,
) func(context.Context) (source.PreparedFetch, error) {
	policy := *policyValue
	return func(ctx context.Context) (source.PreparedFetch, error) {
		local := struct {
			httpRepository

			work, indexWork manifest.AcquisitionWork
		}{httpRepository: *prepared}
		local.work.Scope = f.work.Scope
		local.indexWork.Scope = local.work.Scope
		machinePolicy := policy
		machinePolicy.Work, machinePolicy.HistoryWork = &local.work, &local.indexWork
		return source.AcquireMachine(ctx, &machinePolicy, func(ctx context.Context) error {
			if localCredentialsErr := local.localCredentials(ctx); localCredentialsErr != nil {
				return localCredentialsErr
			}
			credentials := [2]string{local.username, local.password}
			local.material = local.resolved.Identity(credentials)
			return nil
		}, func(ctx context.Context, policyValue *cache.Policy, localArtifact *store.SourceArtifact) (*store.SourceArtifact, error) {
			acquisitionPolicy := *policyValue
			if localArtifact != nil {
				return localArtifact, ctx.Err()
			}
			if connectErr := f.connectHTTP(&local.httpRepository); connectErr != nil {
				return nil, connectErr
			}
			return f.fetchHTTPChart(ctx, &local.httpRepository, hc, &acquisitionPolicy)
		})
	}
}

func (f *operation) fetchOCIChart(
	ctx context.Context,
	hc *manifest.HelmChartSource,
	r *manifest.HelmRepository,
	policyValue *cache.Policy,
) (source.PreparedFetch, error) {
	policy := *policyValue
	prepared, err := f.oci.FetchChart(ctx, hc.OCIRepository(r), f.reads, &policy)
	restamp := func(prepared source.PreparedFetch) source.PreparedFetch {
		fetch := prepared.Fetch
		if fetch != nil {
			prepared.Fetch = func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
				artifact, fetchErr := fetch(ctx, local)
				if artifact != nil {
					artifact.Kind = manifest.KindHelmChart
				}
				return artifact, fetchErr
			}
		}
		return prepared
	}
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryAmbient != nil {
		retry := missing.RetryAmbient
		wrapped := *missing
		wrapped.RetryAmbient = func(ctx context.Context) (source.PreparedFetch, error) {
			result, retryErr := retry(ctx)
			return restamp(result), retryErr
		}
		return source.PreparedFetch{}, &wrapped
	}
	return restamp(prepared), err
}

// FetchDependency MUST require admission of the selected repository before credential access.
// A probe MUST capture identities without preparing credentials or transports.
//
//nolint:revive // The acquisition boundary MUST retain discovery, captured reads, cache policy, and probe mode without another public options type.
func (f *Fetcher) FetchDependency(
	ctx context.Context,
	namespace string,
	dependency *chart.Dependency,
	index *manifest.DiscoveryIndex,
	reads *store.ReadSet,
	policy *cache.Policy,
	probe bool,
) (source.PreparedFetch, error) {
	if err := ctx.Err(); err != nil {
		return source.PreparedFetch{}, fmt.Errorf("prepare chart dependency: %w", err)
	}
	target, err := dependencyURL(dependency.Repository)
	if err != nil {
		return source.PreparedFetch{}, err
	}
	ids := dependencyRepositories(namespace, index, reads)
	var winner *manifest.HelmRepository
	var selected manifest.NamedResource
	var admitted bool
	for _, id := range ids {
		repository, live, lookupErr := dependencyDeclaration(id, index, reads)
		if lookupErr != nil {
			return source.PreparedFetch{
				Inputs: []manifest.DependencyRef{{NamedResource: id, RequiresDeclaration: true}},
			}, lookupErr
		}
		if repository == nil {
			continue
		}
		normalized, normalizeErr := dependencyURL(repository.URL)
		if normalizeErr == nil && normalized == target && winner == nil {
			winner, selected, admitted = repository, id, live
		}
	}
	if winner != nil && !admitted {
		return source.PreparedFetch{
			Inputs: []manifest.DependencyRef{{NamedResource: selected, RequiresDeclaration: true}},
		}, nil
	}
	if winner == nil {
		winner = &manifest.HelmRepository{Namespace: namespace, URL: target}
	}
	if probe {
		for ref := range manifest.References(winner, manifest.ReferenceRelations) {
			if ref.ContentInput() {
				reads.GetDeclaration(ref.NamedResource)
			}
		}
		return source.PreparedFetch{}, nil
	}
	hc := &manifest.HelmChartSource{
		Namespace: namespace,
		Name:      dependency.Name,
		Chart:     dependency.Name,
		Version:   dependency.Version,
	}
	op := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads)}
	return op.fetchRepository(ctx, hc, winner, policy)
}

func dependencyDeclaration(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
	reads *store.ReadSet,
) (*manifest.HelmRepository, bool, error) {
	object := reads.GetDeclaration(id)
	live := object != nil
	if object == nil {
		record, _ := index.Lookup(id)
		object = record.Declaration
	}
	if object == nil {
		return nil, false, manifest.NewError(
			manifest.ErrObjectNotFound,
			nil,
			fmt.Sprintf("dependency repository %s declaration unavailable", id),
		)
	}
	repository, _ := object.(*manifest.HelmRepository)
	return repository, live, nil
}

func dependencyRepositories(
	namespace string,
	index *manifest.DiscoveryIndex,
	reads *store.ReadSet,
) []manifest.NamedResource {
	ids := make([]manifest.NamedResource, 0)
	if index != nil {
		for id := range index.Records {
			if id.Group == manifest.SourceDomain && id.Kind == manifest.KindHelmRepository &&
				id.Namespace == namespace {
				ids = append(ids, id)
			}
		}
	}
	for _, object := range reads.ListObjects(manifest.SourceDomain, manifest.KindHelmRepository) {
		if object.Named().Namespace == namespace {
			ids = append(ids, object.Named())
		}
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	ids = slices.Compact(ids)
	return ids
}

func dependencyURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", manifest.NewError(manifest.ErrInput, err, "")
	}
	if u.Host == "" || u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "oci" {
		return "", manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("invalid dependency repository URL %q", raw))
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = strings.TrimRight(u.RawPath, "/")
	if u.Scheme != "oci" {
		u.Path += "/"
		u.RawPath += "/"
	}
	return u.String(), nil
}
