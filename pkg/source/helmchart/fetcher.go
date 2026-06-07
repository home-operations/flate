// Package helmchart implements the source.Fetcher for KindHelmChart —
// the single authoritative path that fetches a Helm chart (by name +
// version) from its backing HelmRepository. OCI registries are pulled via
// the OCI fetcher; classic HTTP repositories via helm's getter. The
// HelmRelease controller synthesizes a HelmChart per (chart, version,
// repo) and the source controller fetches it here, so every chart pull
// gains retry, the content-addressed Store, and depwait uniformly with
// every other source kind.
package helmchart

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

// RepoLookup resolves a HelmRepository CR by (namespace, name). The
// orchestrator wires it against the canonical Store.
type RepoLookup func(namespace, name string) *manifest.HelmRepository

// ociFetcher is the slice of the OCI source fetcher the OCI branch
// delegates to (source/oci.Fetcher satisfies it). Kept as an interface so
// the branch is unit-testable without standing up a registry, and so this
// package doesn't import pkg/source/oci.
type ociFetcher = source.TypedFetcher[*manifest.OCIRepository]

// Fetcher resolves a HelmChart into an on-disk chart artifact.
type Fetcher struct {
	secrets source.SecretGetter // HTTP-branch SecretRef/CertSecretRef auth
	repos   RepoLookup          // resolves the chart's backing HelmRepository
	oci     ociFetcher          // OCI-backed charts (a synthesized OCIRepository)
	cache   *source.Cache       // content-addressed store for HTTP chart tarballs

	indexCache    sync.Map                // map[string]*repo.IndexFile (process lifetime)
	indexLocks    *keylock.KeyMap[string] // coalesce one index.yaml fetch per repo
	downloadLocks *keylock.KeyMap[string] // coalesce one download per chart
	tmpDir        string                  // index/TLS temp files
}

// New constructs a HelmChart fetcher. cache is the shared content-addressed
// store HTTP chart tarballs land in (so they dedup with the rest of the cache
// and the GC sweep sees them); layout supplies the helm tmp dir for index/TLS
// temp files.
func New(secrets source.SecretGetter, repos RepoLookup, oci ociFetcher, cache *source.Cache, layout cacheroot.Layout) (*Fetcher, error) {
	tmpDir := layout.HelmTmp()
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return nil, fmt.Errorf("helmchart: tmp dir: %w", err)
	}
	return &Fetcher{
		secrets:       secrets,
		repos:         repos,
		oci:           oci,
		cache:         cache,
		indexLocks:    keylock.New[string](),
		downloadLocks: keylock.New[string](),
		tmpDir:        tmpDir,
	}, nil
}

// Fetch implements source.TypedFetcher[*manifest.HelmChartSource]. It
// resolves the backing HelmRepository, then pulls the chart: OCI repos via
// the OCI fetcher (a synthesized OCIRepository), HTTP repos via the getter.
func (f *Fetcher) Fetch(ctx context.Context, hc *manifest.HelmChartSource) (*store.SourceArtifact, error) {
	// Only HelmRepository-backed charts need a dedicated chart fetch. A
	// HelmChart whose sourceRef is a GitRepository/Bucket is resolved by the
	// consuming HelmRelease directly from that source's on-disk artifact
	// (ResolveChartRef repoints the chart there), so it's existence-only
	// here — mark Ready without an artifact. The synthetic HelmCharts the HR
	// controller emits are always HelmRepository-backed, so they take the
	// real path below.
	if k := hc.SourceRef.Kind; k != "" && k != manifest.KindHelmRepository {
		return nil, nil
	}
	r := f.repos(hc.Namespace, hc.SourceRef.Name)
	if r == nil {
		return nil, fmt.Errorf("%w: HelmRepository %s/%s backing HelmChart %s",
			manifest.ErrObjectNotFound, hc.Namespace, hc.SourceRef.Name, hc.Named().String())
	}
	if isOCIHelmRepo(r) {
		// Delegate to the OCI fetcher via a synthesized OCIRepository. The
		// artifact is re-stamped KindHelmChart so the Store entry matches
		// the synthetic HelmChart's identity. Retry is owned by the
		// source.WithRetry wrapper around THIS fetcher — the inner OCI
		// fetcher is bare (not separately wrapped).
		art, err := f.oci.Fetch(ctx, synthesizeOCIRepository(r, hc.Chart, hc.Version))
		if err != nil {
			return nil, err
		}
		if art != nil {
			art.Kind = manifest.KindHelmChart
		}
		return art, nil
	}
	return f.fetchHTTPChart(ctx, r, hc.Chart, hc.Version)
}

// Synthesize builds an in-memory HelmChart for a single chart served by a
// HelmRepository. The HelmRelease controller registers it so the source
// controller fetches the chart here; the chart name+version live on the
// consuming HelmRelease, not the HelmRepository, so there's no standalone CR.
// The id is syntheticChartName(...): distinct charts/versions from the same
// repo get distinct Store ids.
func Synthesize(r *manifest.HelmRepository, chartName, version string) *manifest.HelmChartSource {
	chartURL := normalizeChartURL(r.URL, chartName)
	hc := &manifest.HelmChartSource{
		Name:      syntheticChartName(r.Name, chartName, chartURL, version),
		Namespace: r.Namespace,
	}
	hc.Chart = chartName
	hc.Version = version
	hc.SourceRef = sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: r.Name}
	return hc
}

// normalizeChartURL joins a HelmRepository base URL and a chart name into the
// chart's URL, tolerating a trailing slash on the base.
func normalizeChartURL(url, chartName string) string {
	return strings.TrimSuffix(url, "/") + "/" + chartName
}

// isOCIHelmRepo reports whether a HelmRepository serves charts from an OCI
// registry (spec.type: oci, or an oci:// URL) rather than a classic HTTP
// index.yaml.
func isOCIHelmRepo(r *manifest.HelmRepository) bool {
	return r.Type == manifest.RepoTypeOCI || strings.HasPrefix(r.URL, "oci://")
}

// synthesizeOCIRepository builds an in-memory OCIRepository for a single
// chart served by a type=oci HelmRepository (precondition: isOCIHelmRepo(r)).
// A HelmRepository(oci) is only a registry base; the chart name and version
// live on the consuming HelmRelease, so there is no standalone OCIRepository
// CR. The OCI branch of Fetch builds one on the fly and hands it to the OCI
// fetcher, which applies spec.verify / cert / insecure / layerSelector
// exactly as for a real OCIRepository.
//
// The HelmRepository's auth / TLS / insecure / provider are lifted (it
// carries no proxySecretRef). The resolved version becomes a digest ref
// when it contains ':' else a tag, matching the OCIRepository path.
func synthesizeOCIRepository(r *manifest.HelmRepository, chartName, version string) *manifest.OCIRepository {
	chartURL := normalizeChartURL(r.URL, chartName)
	syn := &manifest.OCIRepository{Namespace: r.Namespace}
	syn.Name = syntheticChartName(r.Name, chartName, chartURL, version)
	syn.URL = chartURL
	syn.Provider = r.Provider
	if version != "" {
		ref := &manifest.OCIRepositoryRef{}
		if strings.Contains(version, ":") {
			ref.Digest = version
		} else {
			ref.Tag = version
		}
		syn.Reference = ref
	}
	syn.SecretRef = r.SecretRef
	syn.CertSecretRef = r.CertSecretRef
	syn.Insecure = r.Insecure
	return syn
}

// syntheticChartName derives the stable <helmrepo>-<chart>-<short hash>
// identity shared by the synthetic HelmChart (Synthesize) and the synthetic
// OCIRepository (synthesizeOCIRepository) for one chart served by a
// HelmRepository. Hashing the normalized chartURL@version keeps distinct
// charts/versions from the same repo on distinct ids and stays name-legal
// when the version is a digest (whose ':' isn't a valid name character). The
// synthetic OCIRepository is never stored (the OCI fetcher caches by URL+ref,
// not name), so for it the name is mainly log readability.
func syntheticChartName(repoName, chartName, chartURL, version string) string {
	sum := sha256.Sum256([]byte(chartURL + "@" + version))
	return repoName + "-" + chartName + "-" + hex.EncodeToString(sum[:])[:7]
}
