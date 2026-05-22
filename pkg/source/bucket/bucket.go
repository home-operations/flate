// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
package bucket

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher pulls a Flux Bucket CR into the on-disk cache. Only the
// "generic" provider (any S3-API-compatible storage) is wired up
// today via minio-go. The aws/gcp/azure providers parse and route
// here but return a clear "not implemented" error rather than silently
// falling back.
type Fetcher struct {
	Cache   *source.Cache
	Secrets source.SecretGetter
}

// Fetch implements source.Fetcher for *manifest.Bucket.
func (f *Fetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	b, ok := obj.(*manifest.Bucket)
	if !ok {
		return nil, fmt.Errorf("%w: Fetcher: unexpected payload %T", manifest.ErrInput, obj)
	}
	if b.Provider != "" && b.Provider != manifest.BucketProviderGeneric {
		return nil, fmt.Errorf(
			"bucket %s/%s provider %q is not implemented; flate currently supports only %q (S3-compatible)",
			b.Namespace, b.Name, b.Provider, manifest.BucketProviderGeneric,
		)
	}

	creds, err := f.resolveCredentials(b)
	if err != nil {
		return nil, err
	}

	endpoint, secure, err := normalizeEndpoint(b.Endpoint, b.Insecure)
	if err != nil {
		return nil, err
	}

	transport, err := f.resolveTransport(b)
	if err != nil {
		return nil, err
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    secure,
		Region:    b.Region,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("bucket %s/%s: minio client: %w", b.Namespace, b.Name, err)
	}

	// Cache key: bucket+prefix; reset on first error so retries start
	// clean. The revision identifier (sha256 over sorted etags) is
	// recomputed after listing.
	slot, _, err := f.Cache.Slot(endpoint+"/"+b.BucketName, b.Prefix)
	if err != nil {
		return nil, fmt.Errorf("bucket %s/%s cache slot: %w", b.Namespace, b.Name, err)
	}

	keys, revHash, err := walkBucket(ctx, client, b.BucketName, b.Prefix, slot)
	if err != nil {
		_ = f.Cache.Reset(slot)
		return nil, fmt.Errorf("bucket %s/%s walk: %w", b.Namespace, b.Name, err)
	}

	return &store.SourceArtifact{
		Kind:      manifest.KindBucket,
		URL:       fmt.Sprintf("%s://%s/%s", schemeFor(secure), endpoint, b.BucketName),
		LocalPath: slot,
		Revision:  revHash,
		Metadata: map[string]string{
			"objectCount": fmt.Sprintf("%d", len(keys)),
		},
	}, nil
}

// resolveTransport builds a custom *http.Transport from
// spec.certSecretRef and/or spec.proxySecretRef. Returns nil to let
// minio-go use its default transport. The Insecure flag is
// intentionally applied at the protocol level (normalizeEndpoint)
// rather than the TLS layer, mirroring Flux's source-controller
// behavior.
func (f *Fetcher) resolveTransport(b *manifest.Bucket) (*http.Transport, error) {
	proxy, err := source.ResolveProxy(f.Secrets, b.Namespace, "Bucket",
		b.Namespace+"/"+b.Name, b.ProxySecretRef)
	if err != nil {
		return nil, err
	}
	if b.CertSecretRef == nil && proxy == nil {
		return nil, nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if b.CertSecretRef != nil {
		cfg, terr := f.resolveTLSConfig(b)
		if terr != nil {
			return nil, terr
		}
		tr.TLSClientConfig = cfg
	}
	if proxy != nil {
		pfn, perr := proxy.HTTPProxyFunc()
		if perr != nil {
			return nil, perr
		}
		tr.Proxy = pfn
	}
	return tr, nil
}

func (f *Fetcher) resolveTLSConfig(b *manifest.Bucket) (*tls.Config, error) {
	if f.Secrets == nil {
		return nil, fmt.Errorf("bucket %s/%s references certSecretRef but no source.SecretGetter is wired",
			b.Namespace, b.Name)
	}
	sec := f.Secrets(b.Namespace, b.CertSecretRef.Name)
	if sec == nil {
		return nil, fmt.Errorf("bucket %s/%s: cert secret %s/%s not found",
			b.Namespace, b.Name, b.Namespace, b.CertSecretRef.Name)
	}
	crt := source.StringFromSecret(sec, "tls.crt")
	key := source.StringFromSecret(sec, "tls.key")
	ca := source.StringFromSecret(sec, "ca.crt")
	if crt == "" && key == "" && ca == "" {
		return nil, fmt.Errorf("bucket %s/%s: certSecretRef %s/%s contains none of tls.crt / tls.key / ca.crt",
			b.Namespace, b.Name, b.Namespace, b.CertSecretRef.Name)
	}
	if (crt == "") != (key == "") {
		return nil, fmt.Errorf("bucket %s/%s: certSecretRef %s/%s must provide both tls.crt and tls.key together",
			b.Namespace, b.Name, b.Namespace, b.CertSecretRef.Name)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if crt != "" {
		cert, err := tls.X509KeyPair([]byte(crt), []byte(key))
		if err != nil {
			return nil, fmt.Errorf("bucket %s/%s: parse tls.crt/tls.key: %w",
				b.Namespace, b.Name, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, fmt.Errorf("bucket %s/%s: ca.crt did not parse as PEM",
				b.Namespace, b.Name)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// resolveCredentials picks up accesskey/secretkey from the SecretRef
// or falls back to anonymous (which is valid for public buckets).
func (f *Fetcher) resolveCredentials(b *manifest.Bucket) (*credentials.Credentials, error) {
	if b.SecretRef == nil {
		return credentials.NewStaticV4("", "", ""), nil
	}
	if f.Secrets == nil {
		return nil, fmt.Errorf("bucket %s/%s references secretRef but no SecretGetter is wired",
			b.Namespace, b.Name)
	}
	sec := f.Secrets(b.Namespace, b.SecretRef.Name)
	if sec == nil {
		return nil, fmt.Errorf("bucket %s/%s: secret %s/%s not found",
			b.Namespace, b.Name, b.Namespace, b.SecretRef.Name)
	}
	access := source.StringFromSecret(sec, "accesskey")
	secret := source.StringFromSecret(sec, "secretkey")
	if access == "" || secret == "" {
		return nil, fmt.Errorf("bucket %s/%s: secret %s/%s missing accesskey/secretkey",
			b.Namespace, b.Name, b.Namespace, b.SecretRef.Name)
	}
	return credentials.NewStaticV4(access, secret, ""), nil
}

// normalizeEndpoint splits a Flux-style endpoint into the
// host[:port] form minio-go expects and a tls flag.
func normalizeEndpoint(endpoint string, insecure bool) (host string, secure bool, err error) {
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		host = strings.TrimPrefix(endpoint, "https://")
		secure = true
	case strings.HasPrefix(endpoint, "http://"):
		host = strings.TrimPrefix(endpoint, "http://")
		secure = false
	default:
		host = endpoint
		secure = !insecure
	}
	host = strings.TrimRight(host, "/")
	if host == "" {
		return "", false, errors.New("bucket endpoint is empty")
	}
	if _, perr := url.Parse(schemeFor(secure) + "://" + host); perr != nil {
		return "", false, fmt.Errorf("parse Bucket endpoint %q: %w", endpoint, perr)
	}
	return host, secure, nil
}

func schemeFor(secure bool) string {
	if secure {
		return "https"
	}
	return "http"
}

// walkBucket lists the bucket under prefix, downloads each object
// into slot preserving the prefix-relative path, and returns the
// sorted key list + a content-addressed revision (sha256 of
// "<key>\t<etag>\n" pairs, sorted).
func walkBucket(ctx context.Context, client *minio.Client, bucket, prefix, slot string) ([]string, string, error) {
	type entry struct{ key, etag string }
	var entries []entry

	for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix: prefix, Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, "", obj.Err
		}
		if strings.HasSuffix(obj.Key, "/") {
			// "Directory" placeholder — skip.
			continue
		}
		entries = append(entries, entry{key: obj.Key, etag: obj.ETag})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	for _, e := range entries {
		rel := strings.TrimPrefix(strings.TrimPrefix(e.key, prefix), "/")
		if rel == "" {
			rel = filepath.Base(e.key)
		}
		dst := filepath.Join(slot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return nil, "", err
		}
		if err := downloadObject(ctx, client, bucket, e.key, dst); err != nil {
			return nil, "", err
		}
	}

	h := sha256.New()
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		_, _ = fmt.Fprintf(h, "%s\t%s\n", e.key, e.etag)
		keys = append(keys, e.key)
	}
	return keys, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func downloadObject(ctx context.Context, client *minio.Client, bucket, key, dst string) error {
	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = obj.Close() }()
	f, err := os.Create(dst) //nolint:gosec // dst is composed from cache slot + bucket key
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, obj); err != nil {
		return fmt.Errorf("copy %s: %w", key, err)
	}
	return nil
}
