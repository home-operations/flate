package helm

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/getter"
	repo "helm.sh/helm/v4/pkg/repo/v1"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// chartCacheLocks serializes concurrent fetches of the same cached
// chart tarball so two reconcilers don't race on the same file.
var chartCacheLocks = keylock.New[string]()

// writeAtomic writes data to path via a temp file + rename so partial
// writes never appear at the target path to concurrent readers. Both
// the file contents and the directory entry are fsynced so a
// power-loss between rename and post-close can't leave a zero-byte
// chart that the next Stat-OK path serves to loader.Load (which then
// reports a misleading "corrupt chart" instead of just re-pulling).
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op if rename succeeds
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// Fsync the directory so the rename's directory entry survives
	// power loss. Without this, the contents fsync above guarantees
	// file bytes survive but the rename can be lost — leaving the
	// old path or no entry at all. Best-effort: fsync of a directory
	// is not portable to every filesystem, so log+ignore on failure.
	if d, err := os.Open(dir); err == nil { //nolint:gosec // dir = filepath.Dir(path); path is caller-controlled cache file
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// ChartLoadResult is the loaded chart plus the on-disk path it came from.
type ChartLoadResult struct {
	Path  string
	Chart *chart.Chart
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

// pullHelmRepoOCI handles the `HelmRepository.type=oci` branch by
// synthesizing an OCIRepository (carrying the HelmRepository's
// secret/cert/proxy/insecure fields) and routing it through the
// OCIPuller — the same source/oci.Fetcher the OCIRepository path
// uses. This gives HelmRepository(type=oci) parity with
// OCIRepository for spec.verify / cert / auth / proxy / insecure;
// previously those fields were silently ignored and SecretRef
// rejected outright with a "not yet implemented" error.
//
// When no puller is wired (EnableOCI=false runs or embedders
// without an OCI fetcher), fall back to the registry-client pull —
// preserves the legacy anonymous-pull behavior for backward
// compatibility.
func (c *Client) pullHelmRepoOCI(ctx context.Context, r *manifest.HelmRepository, hr *manifest.HelmRelease) (string, error) {
	chartURL := strings.TrimSuffix(r.URL, "/") + "/" + hr.Chart.Name
	if puller := c.ociPullerSnapshot(); puller != nil {
		// Name the synthetic OCIRepository after the source
		// HelmRepository's identity (NOT the chart name): the puller's
		// internal slot/dedup/log key uses (Namespace, Name), and
		// using the chart name would conflate two distinct
		// HelmRepositories that happen to ship a chart with the same
		// name. Disambiguate slot identity further by suffixing the
		// chart name so distinct charts from the same HelmRepository
		// also get distinct slots.
		syn := &manifest.OCIRepository{
			Name:      r.Name + "-" + hr.Chart.Name,
			Namespace: r.Namespace,
		}
		syn.URL = chartURL
		if hr.Chart.Version != "" {
			ref := &manifest.OCIRepositoryRef{}
			if strings.Contains(hr.Chart.Version, ":") {
				ref.Digest = hr.Chart.Version
			} else {
				ref.Tag = hr.Chart.Version
			}
			syn.Reference = ref
		}
		// Lift HelmRepository's auth / TLS / proxy / insecure into
		// the synthetic OCIRepository so the puller honors them.
		syn.SecretRef = r.SecretRef
		syn.CertSecretRef = r.CertSecretRef
		syn.Insecure = r.Insecure
		var (
			art *store.SourceArtifact
			err error
		)
		c.yieldDuring(func() {
			art, err = puller.Fetch(ctx, syn)
		})
		if err != nil {
			return "", err
		}
		if art != nil && art.LocalPath != "" {
			path, err := ociChartPathFromArtifact(art.LocalPath)
			if err != nil {
				return "", fmt.Errorf("HelmRepository %s/%s (oci): %w", r.Namespace, r.Name, err)
			}
			return path, nil
		}
	}
	// Registry-client fallback — anonymous pull, no auth/TLS/verify.
	// Preserve the previous SecretRef rejection so a user with
	// credentials configured against this path gets a clear error
	// rather than a silently-anonymous pull failure.
	if r.SecretRef != nil {
		return "", fmt.Errorf(
			"HelmRepository %s/%s: SecretRef on type=oci requires an OCI puller "+
				"(typically EnableOCI=true); reference the chart via a sibling "+
				"OCIRepository CR or enable OCI",
			r.Namespace, r.Name)
	}
	return c.fetchOCIChart(ctx, chartURL, hr.Chart.Version)
}

// locateHelmRepoChart resolves a chart from a HelmRepository. For OCI
// HelmRepositories the URL is `oci://...` and we delegate to the OCI
// path. Otherwise we download the chart tarball via getter, applying
// any SecretRef credentials.
func (c *Client) locateHelmRepoChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	r := c.resolveHelmRepo(hr)
	if r == nil {
		return "", fmt.Errorf("%w: HelmRepository %s not registered for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}

	if r.Type == manifest.RepoTypeOCI || strings.HasPrefix(r.URL, "oci://") {
		return c.pullHelmRepoOCI(ctx, r, hr)
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

	// Include the HelmRepository's identity in the cache key so two
	// repos that publish the same chart name + version don't
	// overwrite each other's tarball on disk. Without the repo
	// prefix, `nginx:15.0.0` from repo A and a different
	// `nginx:15.0.0` from repo B alternately overwrote each other's
	// bytes; the in-memory chartCache (keyed by path) then served
	// whichever bytes were last written.
	cacheKey := safeName(r.Namespace+"-"+r.Name+"-"+hr.Chart.Name) + "-" + cv.Version + ".tgz"
	target := filepath.Join(c.cacheDir, cacheKey)

	release, err := chartCacheLocks.Acquire(ctx, target)
	if err != nil {
		return "", err
	}
	defer release()

	if _, err := os.Stat(target); err == nil {
		return target, nil
	}
	g, err := getter.NewHTTPGetter()
	if err != nil {
		return "", err
	}
	buf, err := g.Get(chartURL, allOpts...)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", chartURL, err)
	}
	if err := writeAtomic(target, buf.Bytes()); err != nil {
		return "", err
	}
	return target, nil
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
