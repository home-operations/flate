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
)

// chartCacheLocks serializes concurrent fetches of the same cached
// chart tarball so two reconcilers don't race on the same file.
var chartCacheLocks = keylock.New[string]()

// writeAtomic writes data to path via a temp file + rename so partial
// writes never appear at the target path to concurrent readers.
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
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
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
			manifest.ErrObjectNotFound, hr.Chart.RepoKind, hr.Chart.RepoFullName(), hr.NamespacedName())
	}
	path := filepath.Join(art.LocalPath, hr.Chart.Name)
	if _, err := os.Stat(filepath.Join(path, "Chart.yaml")); err != nil {
		return "", fmt.Errorf("chart not found at %s: %w", path, err)
	}
	return path, nil
}

// locateHelmRepoChart resolves a chart from a HelmRepository. For OCI
// HelmRepositories the URL is `oci://...` and we delegate to the OCI
// path. Otherwise we download the chart tarball via getter, applying
// any SecretRef credentials.
func (c *Client) locateHelmRepoChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	r := c.resolveHelmRepo(hr)
	if r == nil {
		return "", fmt.Errorf("%w: HelmRepository %s not registered for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoFullName(), hr.NamespacedName())
	}

	if r.Type == manifest.RepoTypeOCI || strings.HasPrefix(r.URL, "oci://") {
		if r.SecretRef != nil {
			return "", fmt.Errorf(
				"HelmRepository %s/%s: SecretRef on OCI HelmRepositories is not yet implemented; "+
					"reference the chart via a sibling OCIRepository CR instead",
				r.Namespace, r.Name)
		}
		return c.fetchOCIChart(ctx, r.URL+"/"+hr.Chart.Name, hr.Chart.Version)
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
	idx, err := c.fetchIndex(indexURL, allOpts)
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

	cacheKey := safeName(hr.Chart.Name) + "-" + cv.Version + ".tgz"
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

// fetchIndex downloads and parses a HelmRepository index.yaml.
func (c *Client) fetchIndex(indexURL string, opts []getter.Option) (*repo.IndexFile, error) {
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
