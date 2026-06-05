package helm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/getter"
	repo "helm.sh/helm/v4/pkg/repo/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// ChartLoadResult is the loaded chart plus the on-disk path it came from.
//
// Fingerprint, when non-empty, is a content-addressed sha256 hex of
// the chart's loader.Load inputs (Metadata + Templates + Files +
// Schema + chart defaults + subchart contents). Computed lazily by
// LoadChart so the template-output cache can build a stable key
// without re-walking the chart on every render. Memoized per
// (Client, path) keyed by the same (mtime, size) fingerprint
// chartCacheEntry uses, so a mutable OCI re-push invalidates this
// digest just as it invalidates the cached *chart.Chart pointer.
// Empty when the template cache is disabled.
type ChartLoadResult struct {
	Path        string
	Chart       *chart.Chart
	Fingerprint string
}

// locateLocalChart resolves a chart whose source is a fetched on-disk
// artifact — GitRepository, Bucket, or ExternalArtifact. The chart
// lives at <artifact.LocalPath>/<chart.Name> in every case.
func (c *Client) locateLocalChart(hr *manifest.HelmRelease) (string, error) {
	art := c.resolveLocalSource(hr)
	if art == nil {
		return "", fmt.Errorf("%w: %s %s not available for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoKind, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}
	path := filepath.Join(art.LocalPath, hr.Chart.Name)
	if _, err := os.Stat(filepath.Join(path, "Chart.yaml")); err != nil {
		return "", fmt.Errorf("chart not found at %s: %w", path, err)
	}
	return path, nil
}

// IsOCIHelmRepo reports whether a HelmRepository serves charts from an OCI
// registry (spec.type: oci, or an oci:// URL) rather than a classic HTTP
// index.yaml. Such a repo has no standalone chart source CR — its charts are
// materialized into synthetic OCIRepositories (see SynthesizeOCIRepository).
func IsOCIHelmRepo(r *manifest.HelmRepository) bool {
	return r.Type == manifest.RepoTypeOCI || strings.HasPrefix(r.URL, "oci://")
}

// SynthesizeOCIRepository builds an in-memory OCIRepository for a single
// chart served by a type=oci HelmRepository (precondition: IsOCIHelmRepo(r)).
// A HelmRepository(oci) is only a registry base; the chart name and version
// live on the consuming HelmRelease, so there is no standalone OCIRepository
// CR. The orchestrator's HelmRelease controller registers this synthetic
// object with the source controller so the chart is fetched through the
// single source path (fetch + retry + Store + depwait), exactly like a real
// OCIRepository — rather than an inline lazy pull.
//
// The HelmRepository's auth / TLS / insecure / provider are lifted so the
// OCI fetcher honors them (HelmRepository carries no proxySecretRef). The
// resolved version becomes a digest ref when it contains ':' else a tag,
// matching how the OCIRepository path treats a pinned chart version.
func SynthesizeOCIRepository(r *manifest.HelmRepository, chartName, version string) *manifest.OCIRepository {
	chartURL := strings.TrimSuffix(r.URL, "/") + "/" + chartName
	syn := &manifest.OCIRepository{Namespace: r.Namespace}
	syn.Name = syntheticOCIName(r.Name, chartName, chartURL, version)
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

// syntheticOCIName derives a stable, unique Store identity for a synthetic
// OCIRepository: <helmrepo>-<chart>-<short hash of url@version>. The hash
// disambiguates two HelmReleases pulling different versions of the same
// chart from the same repo (same <helmrepo>-<chart> prefix would otherwise
// collide on one Store id and clobber each other's artifact) and keeps the
// name valid when the version is a digest (whose ':' isn't a legal name
// character).
func syntheticOCIName(repoName, chartName, chartURL, version string) string {
	sum := sha256.Sum256([]byte(chartURL + "@" + version))
	return repoName + "-" + chartName + "-" + hex.EncodeToString(sum[:])[:7]
}

// locateHelmRepoChart resolves a chart from an HTTP HelmRepository: download
// index.yaml via getter, pick the version, fetch the tarball — applying any
// SecretRef credentials.
//
// type=oci HelmRepositories never reach here: the orchestrator's HelmRelease
// controller repoints them to a synthesized OCIRepository before chart
// resolution (see SynthesizeOCIRepository / materializeOCIChartSource), so
// LocateChart dispatches them to locateOCIChart instead. A direct embedder
// that skips that step and hands an oci:// HelmRepository to LocateChart will
// fall through to the index.yaml fetch below and fail there — call
// SynthesizeOCIRepository (or use the orchestrator) for type=oci.
func (c *Client) locateHelmRepoChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	r := c.resolveHelmRepo(hr)
	if r == nil {
		return "", fmt.Errorf("%w: HelmRepository %s not registered for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}

	authOpts, err := c.helmRepoAuthOptions(r)
	if err != nil {
		return "", err
	}
	tlsOpts, cleanup, err := c.helmRepoTLSOptions(r)
	if err != nil {
		return "", err
	}
	defer cleanup()
	allOpts := append(authOpts, tlsOpts...)

	indexURL := strings.TrimSuffix(r.URL, "/") + "/index.yaml"
	idx, err := c.fetchIndex(ctx, r.Namespace+"/"+r.Name+"@"+indexURL, indexURL, allOpts)
	if err != nil {
		return "", err
	}
	cv, err := idx.Get(hr.Chart.Name, hr.Chart.Version)
	if err != nil {
		return "", fmt.Errorf("%w: chart %s@%s not found in %s: %v",
			manifest.ErrObjectNotFound, hr.Chart.Name, hr.Chart.Version, r.URL, err)
	}
	if len(cv.URLs) == 0 {
		return "", fmt.Errorf("%w: chart %s@%s in %s has no URLs",
			manifest.ErrObjectNotFound, hr.Chart.Name, hr.Chart.Version, r.URL)
	}
	chartURL, err := absChartURL(r.URL, cv.URLs[0])
	if err != nil {
		return "", err
	}

	wantDigest := normalizeChartDigest(cv.Digest)
	if path, ok := c.chartTarballByDigest(wantDigest); ok {
		return path, nil
	}

	release, err := c.chartDownloadLocks.Acquire(ctx, chartDownloadKey(r, hr, cv, chartURL, wantDigest))
	if err != nil {
		return "", err
	}
	defer release()

	if path, ok := c.chartTarballByDigest(wantDigest); ok {
		return path, nil
	}
	g, err := getter.NewHTTPGetter()
	if err != nil {
		return "", err
	}
	buf, err := g.Get(chartURL, allOpts...)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", chartURL, err)
	}
	dir, digest, err := c.chartBlobs.PutBytes(ctx, buf.Bytes(), "chart.tgz")
	if err != nil {
		return "", fmt.Errorf("store chart %s: %w", chartURL, err)
	}
	if wantDigest != "" && digest != wantDigest {
		return "", fmt.Errorf("chart %s@%s digest mismatch: index has %s, downloaded %s",
			hr.Chart.Name, cv.Version, wantDigest, digest)
	}
	return filepath.Join(dir, "chart.tgz"), nil
}

func (c *Client) chartTarballByDigest(digest string) (string, bool) {
	if digest == "" || !c.chartBlobs.Exists(digest) {
		return "", false
	}
	return filepath.Join(c.chartBlobs.Path(digest), "chart.tgz"), true
}

func normalizeChartDigest(digest string) string {
	return strings.TrimPrefix(strings.TrimSpace(digest), "sha256:")
}

func chartDownloadKey(r *manifest.HelmRepository, hr *manifest.HelmRelease, cv *repo.ChartVersion, chartURL, digest string) string {
	if digest != "" {
		return "sha256:" + digest
	}
	return safeName(r.Namespace+"-"+r.Name+"-"+hr.Chart.Name) + "-" + cv.Version + "@" + chartURL
}

// helmRepoAuthOptions / helmRepoTLSOptions live in auth.go (paired
// with auth_test.go).

// fetchIndex returns the parsed index.yaml for a HelmRepository. The
// parsed *repo.IndexFile is memoized on Client.indexCache for the
// process lifetime, keyed by `<ns>/<name>@<indexURL>`. N concurrent
// HelmReleases pointing at the same repo coalesce on indexLocks so
// exactly one HTTP fetch runs and the rest hit the populated cache.
//
// cacheKey is derived by the caller (locateHelmRepoChart) so the
// cache distinguishes two HelmRepository CRs that share a URL but
// may carry different auth contexts. The HTTP fetch itself uses opts
// (auth, TLS) the caller resolved against the CR's SecretRef.
func (c *Client) fetchIndex(ctx context.Context, cacheKey, indexURL string, opts []getter.Option) (*repo.IndexFile, error) {
	if v, ok := c.indexCache.Load(cacheKey); ok {
		return v.(*repo.IndexFile), nil
	}
	release, err := c.indexLocks.Acquire(ctx, cacheKey)
	if err != nil {
		return nil, err
	}
	defer release()
	// Re-check after acquiring the lock: a sibling that beat us into
	// the critical section populated the entry while we waited.
	if v, ok := c.indexCache.Load(cacheKey); ok {
		return v.(*repo.IndexFile), nil
	}
	g, err := getter.NewHTTPGetter()
	if err != nil {
		return nil, err
	}
	buf, err := g.Get(indexURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", indexURL, err)
	}
	tmp, err := os.CreateTemp(c.tmpDir, "helm-index-*.yaml")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	idx, err := repo.LoadIndexFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", indexURL, err)
	}
	c.indexCache.Store(cacheKey, idx)
	return idx, nil
}

// locateOCIChart + ociChartPathFromArtifact + findChartSubdir +
// ociPullRef + fetchOCIChart + safeName live in oci_chart.go (paired
// with oci_chart_test.go).

// absChartURL resolves urlStr against base — HelmRepository index
// entries often carry relative URLs which need to be joined against
// the repo's spec.url to produce something fetchable.
func absChartURL(base, urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}
	if u.IsAbs() {
		return urlStr, nil
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	return baseURL.ResolveReference(u).String(), nil
}
