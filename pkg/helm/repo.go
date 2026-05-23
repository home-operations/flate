package helm

import (
	"context"
	"errors"
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
	"github.com/home-operations/flate/pkg/source"
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
		return "", fmt.Errorf("chart %s@%s not found in %s: %w", hr.Chart.Name, hr.Chart.Version, r.URL, err)
	}
	if len(cv.URLs) == 0 {
		return "", fmt.Errorf("chart %s@%s in %s has no URLs", hr.Chart.Name, hr.Chart.Version, r.URL)
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

// helmRepoAuthOptions resolves SecretRef credentials for a HelmRepository
// into helm getter options. Returns nil options when no SecretRef is
// configured (anonymous). Username/password basic auth + optional
// PassCredentials forwarding. Insecure flag is OCI-only per Flux's
// schema so it is intentionally not surfaced here.
func (c *Client) helmRepoAuthOptions(r *manifest.HelmRepository) ([]getter.Option, error) {
	if r.SecretRef == nil {
		return nil, nil
	}
	c.mu.RLock()
	getSec := c.secrets
	c.mu.RUnlock()
	if getSec == nil {
		return nil, fmt.Errorf("HelmRepository %s/%s references secretRef but no SecretGetter is wired",
			r.Namespace, r.Name)
	}
	sec := getSec(r.Namespace, r.SecretRef.Name)
	if sec == nil {
		return nil, fmt.Errorf("HelmRepository %s/%s: secret %s/%s not found",
			r.Namespace, r.Name, r.Namespace, r.SecretRef.Name)
	}
	username := source.StringFromSecret(sec, "username")
	password := source.StringFromSecret(sec, "password")
	if username == "" || password == "" {
		return nil, fmt.Errorf("HelmRepository %s/%s: secret %s/%s missing username/password",
			r.Namespace, r.Name, r.Namespace, r.SecretRef.Name)
	}
	opts := []getter.Option{getter.WithBasicAuth(username, password)}
	if r.PassCredentials {
		opts = append(opts, getter.WithPassCredentialsAll(true))
	}
	return opts, nil
}

// helmRepoTLSOptions resolves spec.certSecretRef into helm getter
// options. The Secret should carry one or both of (tls.crt, tls.key)
// for client cert auth, plus optional ca.crt for a custom server CA.
// Each present file is materialized to a temp file (helm getter v4's
// WithTLSClientConfig accepts paths, not bytes) and removed by the
// returned cleanup func — always safe to call.
func (c *Client) helmRepoTLSOptions(r *manifest.HelmRepository) ([]getter.Option, func(), error) {
	noCleanup := func() {}
	if r.CertSecretRef == nil {
		return nil, noCleanup, nil
	}
	c.mu.RLock()
	getSec := c.secrets
	c.mu.RUnlock()
	if getSec == nil {
		return nil, noCleanup, fmt.Errorf("HelmRepository %s/%s references certSecretRef but no SecretGetter is wired",
			r.Namespace, r.Name)
	}
	sec := getSec(r.Namespace, r.CertSecretRef.Name)
	if sec == nil {
		return nil, noCleanup, fmt.Errorf("HelmRepository %s/%s: cert secret %s/%s not found",
			r.Namespace, r.Name, r.Namespace, r.CertSecretRef.Name)
	}

	var tmpFiles []string
	writeKey := func(key string) (string, error) {
		v := source.StringFromSecret(sec, key)
		if v == "" {
			return "", nil
		}
		tmp, err := os.CreateTemp(c.tmpDir, "helm-tls-*.pem")
		if err != nil {
			return "", fmt.Errorf("temp %s: %w", key, err)
		}
		if _, err := tmp.WriteString(v); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return "", fmt.Errorf("write %s: %w", key, err)
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmp.Name())
			return "", fmt.Errorf("close %s: %w", key, err)
		}
		tmpFiles = append(tmpFiles, tmp.Name())
		return tmp.Name(), nil
	}
	cleanup := func() {
		for _, p := range tmpFiles {
			_ = os.Remove(p)
		}
	}

	certPath, err := writeKey("tls.crt")
	if err != nil {
		cleanup()
		return nil, noCleanup, err
	}
	keyPath, err := writeKey("tls.key")
	if err != nil {
		cleanup()
		return nil, noCleanup, err
	}
	caPath, err := writeKey("ca.crt")
	if err != nil {
		cleanup()
		return nil, noCleanup, err
	}
	if certPath == "" && keyPath == "" && caPath == "" {
		cleanup()
		return nil, noCleanup, fmt.Errorf("HelmRepository %s/%s: certSecretRef %s/%s contains none of tls.crt / tls.key / ca.crt",
			r.Namespace, r.Name, r.Namespace, r.CertSecretRef.Name)
	}
	return []getter.Option{getter.WithTLSClientConfig(certPath, keyPath, caPath)}, cleanup, nil
}

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

// locateOCIChart resolves a chart whose source is an OCIRepository.
// The OCIRepository.url already points at the chart artifact (Flux's
// "chart-as-OCI-artifact" model) so we use it verbatim — the chart's
// short name from the HelmRelease is metadata, not part of the URL.
func (c *Client) locateOCIChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	r := c.resolveOCIRepo(hr)
	if r == nil {
		return "", fmt.Errorf("%w: OCIRepository %s not registered", manifest.ErrObjectNotFound, hr.Chart.RepoFullName())
	}
	ver, err := r.Version()
	if err != nil {
		return "", err
	}
	return c.fetchOCIChart(ctx, r.URL, ver)
}

// ociPullRef joins an OCI repo URL and an optional ref into the form
// the helm registry client expects. A digest ref (`sha256:<hex>` and
// friends) joins with `@`; a tag joins with `:`. Per OCI tag spec a
// tag can never contain `:`, so its presence in `version` is an
// unambiguous digest signal — without this branch, the helm client
// rejects `repo:sha256:<hex>` as an invalid tag.
func ociPullRef(ref, version string) string {
	if version == "" {
		return ref
	}
	sep := ":"
	if strings.Contains(version, ":") {
		sep = "@"
	}
	return ref + sep + version
}

// fetchOCIChart pulls an OCI chart via the helm registry client.
func (c *Client) fetchOCIChart(ctx context.Context, ref, version string) (string, error) {
	if c.registry == nil {
		return "", errors.New("helm registry client not initialized")
	}
	target := filepath.Join(c.cacheDir, safeName(filepath.Base(ref))+"-"+version+".tgz")

	release, err := chartCacheLocks.Acquire(ctx, target)
	if err != nil {
		return "", err
	}
	defer release()

	if _, err := os.Stat(target); err == nil {
		return target, nil
	}

	pullRef := ociPullRef(ref, version)
	_ = ctx // reserved for future per-pull cancellation when helm supports it
	result, err := c.registry.Pull(pullRef)
	if err != nil {
		return "", fmt.Errorf("oci pull %s: %w", pullRef, err)
	}
	if result == nil || result.Chart == nil {
		return "", fmt.Errorf("oci pull %s: empty result", pullRef)
	}
	if err := writeAtomic(target, result.Chart.Data); err != nil {
		return "", err
	}
	return target, nil
}

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

func safeName(s string) string {
	out := strings.Builder{}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-' || r == '_' || r == '.':
			out.WriteRune(r)
		default:
			out.WriteRune('-')
		}
	}
	return out.String()
}
