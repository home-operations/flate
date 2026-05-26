package bucket

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// resolveTransport builds the http.Transport minio-go should use when
// the Bucket carries a CertSecretRef (custom CA / client cert / TLS
// disabled) and/or a ProxySecretRef. Returns nil when both are absent
// so the minio-go default transport is used.
//
// CertSecretRef key conventions (matching Flux):
//   - ca.crt    — PEM-encoded CA bundle, root-trust the server cert
//   - tls.crt + tls.key — client cert for mTLS
//
// spec.insecure (HTTP-only endpoint) is NOT a TLS-level toggle but
// intentionally applied at the protocol level (normalizeEndpoint)
// rather than the TLS layer, mirroring Flux's source-controller
// behavior.
func (f *Fetcher) resolveTransport(b *manifest.Bucket) (*http.Transport, error) {
	proxy, err := source.ResolveProxy(f.Secrets, b.Namespace, "Bucket",
		b.Namespace+"/"+b.Name, b.ProxySecretRef)
	if err != nil {
		return nil, err
	}
	var tlsCfg *tls.Config
	if b.CertSecretRef != nil {
		tlsCfg, err = source.ResolveCertSecret(f.Secrets, b.Namespace, "Bucket",
			b.Namespace+"/"+b.Name, b.CertSecretRef)
		if err != nil {
			return nil, err
		}
	}
	return source.NewHTTPTransport(tlsCfg, proxy)
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
		return nil, source.MissingSecretErr("bucket", b.Namespace, b.Name, b.SecretRef.Name, "not found")
	}
	access := source.StringFromSecret(sec, "accesskey")
	secret := source.StringFromSecret(sec, "secretkey")
	if access == "" || secret == "" {
		// Empty covers both missing-key and PLACEHOLDER-wiped values
		// (the ExternalSecret case). Same sentinel so
		// --allow-missing-secrets covers both shapes.
		return nil, source.MissingSecretErr("bucket", b.Namespace, b.Name, b.SecretRef.Name, "missing accesskey/secretkey")
	}
	return credentials.NewStaticV4(access, secret, ""), nil
}
