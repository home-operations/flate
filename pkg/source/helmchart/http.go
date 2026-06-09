package helmchart

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"helm.sh/helm/v4/pkg/getter"
	repo "helm.sh/helm/v4/pkg/repo/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// fetchHTTPChart pulls a chart from a classic HTTP HelmRepository: resolve the
// chart name+version to a concrete (version, digest, URL), then fetch the .tgz
// via helm's getter, store it content-addressed, and return an artifact whose
// LocalPath is the blob dir containing chart.tgz. A warm run with a fresh
// on-disk resolution and the blob already cached opens zero sockets.
func (f *Fetcher) fetchHTTPChart(ctx context.Context, r *manifest.HelmRepository, chartName, version string) (*store.SourceArtifact, error) {
	authOpts, err := f.helmRepoAuthOptions(r)
	if err != nil {
		return nil, err
	}
	tlsOpts, cleanup, err := f.helmRepoTLSOptions(r)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	allOpts := slices.Concat(authOpts, tlsOpts)

	res, err := f.resolveChart(ctx, r, chartName, version, allOpts)
	if err != nil {
		return nil, err
	}

	if art, ok := f.chartArtifactByDigest(res.ChartURL, res.Version, res.Digest); ok {
		return art, nil
	}
	release, err := f.downloadLocks.Acquire(ctx, chartDownloadKey(r, chartName, res.Version, res.ChartURL, res.Digest))
	if err != nil {
		return nil, err
	}
	defer release()
	if art, ok := f.chartArtifactByDigest(res.ChartURL, res.Version, res.Digest); ok {
		return art, nil
	}

	buf, err := httpGet(res.ChartURL, allOpts)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", res.ChartURL, err)
	}
	dir, digest, err := f.cache.PutBytes(ctx, buf.Bytes(), "chart.tgz")
	if err != nil {
		return nil, fmt.Errorf("store chart %s: %w", res.ChartURL, err)
	}
	if res.Digest != "" && digest != res.Digest {
		return nil, fmt.Errorf("chart %s@%s digest mismatch: index has %s, downloaded %s",
			chartName, res.Version, res.Digest, digest)
	}
	return httpChartArtifact(res.ChartURL, dir, res.Version, digest), nil
}

// resolveChart maps (chartName, requested version) to a concrete
// (version, digest, URL) for an HTTP HelmRepository. Three tiers, cheapest
// first: fetchIndex's in-process index memo (Tier 1), an on-disk resolve slot
// fresh within spec.interval that skips the network entirely (Tier 2), then a
// live index.yaml fetch whose result is persisted back to the slot (Tier 3).
//
// The slot key uses the RAW requested version, so a semver range and a pin
// never share a slot — within one spec.interval window the cache deliberately
// serves the same resolution a live fetch would have at populate time (the OCI
// mutable-ref / resolve-marker contract, now applied to helm ranges). A pinned
// exact version resolves identically regardless, so freshness only matters for
// ranges. The chart .tgz stays content-addressed by digest, so this changes no
// rendered output — only whether the index is fetched.
func (f *Fetcher) resolveChart(ctx context.Context, r *manifest.HelmRepository, chartName, version string, opts []getter.Option) (chartResolution, error) {
	slot, err := f.cache.Slot(ctx, r.URL, "helm-resolve:"+chartName+"@"+version, helmRepoAuthIdentity(r))
	if err != nil {
		return chartResolution{}, fmt.Errorf("helm resolve slot for %s: %w", r.URL, err)
	}
	defer slot.Release()
	if slot.Exists {
		if res, ok := readResolveFresh(slot.Path, r.Interval.Duration); ok {
			return res, nil
		}
	}

	indexURL := strings.TrimSuffix(r.URL, "/") + "/index.yaml"
	idx, err := f.fetchIndex(ctx, r.Namespace+"/"+r.Name+"@"+indexURL, indexURL, opts)
	if err != nil {
		return chartResolution{}, err
	}
	cv, err := idx.Get(chartName, version)
	if err != nil {
		return chartResolution{}, fmt.Errorf("%w: chart %s@%s not found in %s: %v",
			manifest.ErrObjectNotFound, chartName, version, r.URL, err)
	}
	if len(cv.URLs) == 0 {
		return chartResolution{}, fmt.Errorf("%w: chart %s@%s in %s has no URLs",
			manifest.ErrObjectNotFound, chartName, version, r.URL)
	}
	chartURL, err := absChartURL(r.URL, cv.URLs[0])
	if err != nil {
		return chartResolution{}, err
	}
	res := chartResolution{Version: cv.Version, Digest: normalizeChartDigest(cv.Digest), ChartURL: chartURL}
	if err := persistResolve(slot, res); err != nil {
		return chartResolution{}, err
	}
	return res, nil
}

// chartArtifactByDigest returns the cached chart artifact when the index
// supplied a digest and the blob is already present (content-addressed
// dedup across HelmRepositories). A digest-less index is mutable → miss.
func (f *Fetcher) chartArtifactByDigest(chartURL, version, digest string) (*store.SourceArtifact, bool) {
	dir, ok := f.cache.BlobByDigest(digest)
	if !ok {
		return nil, false
	}
	return httpChartArtifact(chartURL, dir, version, digest), true
}

func httpChartArtifact(chartURL, dir, version, digest string) *store.SourceArtifact {
	return &store.SourceArtifact{
		Kind:      manifest.KindHelmChart,
		URL:       chartURL,
		LocalPath: dir, // dir containing chart.tgz
		Revision:  version,
		Digest:    digest,
	}
}

// fetchIndex returns the parsed index.yaml for a HelmRepository, memoized on
// indexCache for the process lifetime and keyed by `<ns>/<name>@<indexURL>`
// (CR identity, so two repos sharing a URL but different auth don't collide).
// N concurrent chart fetches against the same repo coalesce on indexLocks so
// exactly one HTTP fetch runs.
func (f *Fetcher) fetchIndex(ctx context.Context, cacheKey, indexURL string, opts []getter.Option) (*repo.IndexFile, error) {
	if idx, ok := f.cachedIndex(cacheKey); ok {
		return idx, nil
	}
	release, err := f.indexLocks.Acquire(ctx, cacheKey)
	if err != nil {
		return nil, err
	}
	defer release()
	if idx, ok := f.cachedIndex(cacheKey); ok {
		return idx, nil
	}
	buf, err := httpGet(indexURL, opts)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", indexURL, err)
	}
	tmp, err := os.CreateTemp(f.tmpDir, "helm-index-*.yaml")
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
	f.indexCache.Store(cacheKey, idx)
	return idx, nil
}

// cachedIndex returns the memoized index.yaml for cacheKey, if present.
func (f *Fetcher) cachedIndex(cacheKey string) (*repo.IndexFile, bool) {
	v, ok := f.indexCache.Load(cacheKey)
	if !ok {
		return nil, false
	}
	return v.(*repo.IndexFile), true
}

func chartDownloadKey(r *manifest.HelmRepository, chartName, version, chartURL, digest string) string {
	if digest != "" {
		return "sha256:" + digest
	}
	return safeName(r.Namespace+"-"+r.Name+"-"+chartName) + "-" + version + "@" + chartURL
}

func normalizeChartDigest(digest string) string {
	return strings.TrimPrefix(strings.TrimSpace(digest), "sha256:")
}

// absChartURL resolves urlStr against base — HelmRepository index entries
// often carry relative URLs which need joining against the repo's spec.url.
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

// helmHTTPTimeout bounds a single helm HTTP request (an index.yaml or a
// chart .tgz). A liveness backstop, not a determinism knob: the chart-source
// wait is now bound to fetch-task completion rather than a per-dep wall
// clock, and helm's getter builds an http.Client{Timeout: 0} (unbounded)
// that ignores ctx, so a socket that connects but never delivers bytes would
// keep the task pool's active count above zero and wedge the whole run.
// Sized per-request (not per-retry-budget) and large enough that a slow-but-
// live repo still completes — l7mp.io routinely takes tens of seconds, with
// an occasional retried EOF — while a dead socket always terminates. A var so
// tests can shrink it; mutate only before a run starts to stay race-clean.
var helmHTTPTimeout = 120 * time.Second

// httpGet fetches url with helm's HTTP getter and the given options. Callers
// wrap the error with their own context (index fetch vs chart download).
func httpGet(url string, opts []getter.Option) (*bytes.Buffer, error) {
	g, err := getter.NewHTTPGetter()
	if err != nil {
		return nil, err
	}
	// Prepend the liveness timeout so a caller-supplied WithTimeout (none
	// today) still overrides it — getter applies options in order, last
	// write wins. source.WithRetry layers attempts on top.
	opts = append([]getter.Option{getter.WithTimeout(helmHTTPTimeout)}, opts...)
	return g.Get(url, opts...)
}

func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '-'
	}, s)
}
