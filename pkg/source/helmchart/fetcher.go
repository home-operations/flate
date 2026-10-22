// Package helmchart resolves HelmRepository charts through the shared acquisition cache.
package helmchart

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Masterminds/semver/v3"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// ociFetcher is the slice of the OCI source fetcher the OCI branch
// delegates to (source/oci.Fetcher satisfies it). Kept as an interface so
// the branch is unit-testable without standing up a registry, and so this
// package doesn't import pkg/source/oci.
type ociFetcher = source.TypedFetcher[*manifest.OCIRepository]

// Fetcher resolves a HelmChart into an immutable source artifact.
type Fetcher struct {
	oci     ociFetcher   // OCI-backed charts (a synthesized OCIRepository)
	cache   *cache.Cache // content-addressed store for HTTP chart tarballs
	network *source.Network

	indexCache                 sync.Map                // map[string]*repo.IndexFile
	indexLocks                 *keylock.KeyMap[string] // serialize parsed memo publication
	disableChartDigestTracking bool
}

// Config fixes acquisition and chart identity policy before reconciliation.
type Config struct {
	// Cache remains caller-owned and MUST outlive acquired artifacts.
	Cache *cache.Cache
	// Network is the required non-nil invocation owner, borrowed by this fetcher.
	Network *source.Network
	// DisableChartDigestTracking makes exact chart versions immutable cache pins.
	DisableChartDigestTracking bool
}

// New constructs a fetcher borrowing the caller's cache and required non-nil Network.
func New(oci ociFetcher, cfg Config) *Fetcher {
	return &Fetcher{oci: oci, cache: cfg.Cache,
		network: cfg.Network, disableChartDigestTracking: cfg.DisableChartDigestTracking,
		indexLocks: keylock.New[string]()}
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
		// Delegate to the OCI fetcher via a synthesized OCIRepository. The
		// artifact is re-stamped KindHelmChart so the Store entry matches
		// the synthetic HelmChart's identity.
		syn := synthesizeOCIRepository(r, hc)
		syn.Interval = r.Interval
		art, err := f.oci.Fetch(ctx, syn, f.reads, policy)
		if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryWithRegistryConfig != nil {
			retry := missing.RetryWithRegistryConfig
			wrapped := *missing
			wrapped.RetryWithRegistryConfig = func(ctx context.Context, configPath string) (*store.SourceArtifact, error) {
				art, err := retry(ctx, configPath)
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
		return nil, err
	}
	return f.fetchHTTPChart(ctx, prepared, hc, policy)
}

// Synthesize builds an in-memory HelmChart for a single chart served by a
// HelmRepository. The HelmRelease controller registers it so the source
// controller fetches the chart here; the chart name+version live on the
// consuming HelmRelease, not the HelmRepository, so there's no standalone CR.
// The id is syntheticChartName(...): distinct charts/versions from the same
// repo get distinct Store ids.
func Synthesize(r *manifest.HelmRepository, chartName, version string) *manifest.HelmChartSource {
	chartURL := normalizeChartURL(r.URL, chartName)
	return &manifest.HelmChartSource{
		Name:      syntheticChartName(r.Name, chartName, chartURL, version),
		Namespace: r.Namespace,
		Chart:     chartName,
		Version:   version,
		SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: r.Name},
	}
}

// normalizeChartURL joins a HelmRepository base URL and a chart name into the
// chart's URL, tolerating a trailing slash on the base.
func normalizeChartURL(url, chartName string) string {
	return strings.TrimSuffix(url, "/") + "/" + chartName
}

// synthesizeOCIRepository builds an in-memory OCIRepository for a single
// chart served by a type=oci HelmRepository (precondition: r.IsOCI()).
// A HelmRepository(oci) is only a registry base; the chart name and version
// live on the consuming HelmRelease, so there is no standalone OCIRepository
// CR. The OCI branch of Fetch builds one on the fly and hands it to the OCI
// fetcher, which applies spec.verify / cert / insecure / layerSelector
// exactly as for a real OCIRepository.
//
// The HelmRepository's auth / TLS / insecure / provider are lifted (it
// carries no proxySecretRef). The version is classified by versionToOCIRef:
// a ':' is a digest, an exact semver pin or a non-semver literal stays a
// tag, and a semver constraint becomes a SemVer ref so the OCI fetcher
// resolves it to the highest matching published tag — matching the
// OCIRepository path.
func synthesizeOCIRepository(r *manifest.HelmRepository, hc *manifest.HelmChartSource) *manifest.OCIRepository {
	chartURL := normalizeChartURL(r.URL, hc.Chart)
	syn := &manifest.OCIRepository{Namespace: r.Namespace}
	syn.Name = syntheticChartName(r.Name, hc.Chart, chartURL, hc.Version)
	syn.URL = chartURL
	syn.Provider = r.Provider
	if hc.Version != "" {
		syn.Reference = versionToOCIRef(hc)
	}
	syn.SecretRef = r.SecretRef
	syn.CertSecretRef = r.CertSecretRef
	syn.Insecure = r.Insecure
	return syn
}

// versionToOCIRef classifies a HelmRelease chart version into an OCI
// reference. A ':' means a digest. Otherwise a string that parses as a
// concrete semver version is an exact pin kept as a literal tag; a string
// that is not a concrete version but is a valid semver constraint (1.20.x,
// *, ~, ^, ranges) is routed to SemVer so the OCI fetcher's semver resolver
// selects the highest matching published tag; anything else is a non-semver
// literal tag (e.g. "latest").
//
// The exact-version-before-constraint ordering is load-bearing:
// semver.NewConstraint("1.20.0") succeeds as "=1.20.0", so testing the
// constraint first would misroute exact pins and bare partials (1.20, 1) to
// SemVer. Peeling concrete versions off first is what separates a pin from a
// constraint.
func versionToOCIRef(hc *manifest.HelmChartSource) *sourcev1.OCIRepositoryRef {
	version := hc.Version
	ref := &sourcev1.OCIRepositoryRef{}
	switch {
	case strings.Contains(version, ":"):
		ref.Digest = version
	case hc.ExactVersion():
		ref.Tag = version
	case isSemverConstraint(version):
		ref.SemVer = version
	default:
		ref.Tag = version
	}
	return ref
}

// isSemverConstraint reports whether version parses as a semver constraint
// (wildcards, tilde/caret, ranges).
func isSemverConstraint(version string) bool {
	_, err := semver.NewConstraint(version)
	return err == nil
}

const syntheticChartHashHexLength = 7

// syntheticChartName derives the stable <helmrepo>-<chart>-<short hash>
// identity shared by the synthetic HelmChart (Synthesize) and the synthetic
// OCIRepository (synthesizeOCIRepository) for one chart served by a
// HelmRepository. Hashing the normalized chartURL@version keeps distinct
// charts/versions from the same repo on distinct ids and stays name-legal
// when the version is a digest (whose ':' isn't a valid name character). The
// synthetic OCIRepository is never stored (the OCI fetcher caches by URL+ref,
// not name), so for it the name is mainly log readability.
func syntheticChartName(repoName, chartName, chartURL, version string) string {
	return repoName + "-" + chartName + "-" + manifest.SHA256Hex([]byte(chartURL + "@" + version))[:syntheticChartHashHexLength]
}
