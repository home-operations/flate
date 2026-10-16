package bucket

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// resolveTransport preserves the bounded, guarded kernel for every Bucket,
// including anonymous requests without TLS or proxy customization.
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
	owner := b.Named()
	owner.Name = b.Namespace + "/" + b.Name
	proxy, err := source.ResolveProxy(f.Secrets, owner, b.ProxySecretRef)
	if err != nil {
		return nil, err
	}
	var tlsCfg *tls.Config
	if b.CertSecretRef != nil {
		tlsCfg, err = source.ResolveCertSecret(f.Secrets, owner, b.CertSecretRef)
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
		return nil, fmt.Errorf("%s references secretRef but no SecretGetter is wired", bucketID(b))
	}
	sec := f.Secrets(b.Namespace, b.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr(b.Named(), b.SecretRef.Name, "not found")
	}
	access, _, _ := sec.Value("accesskey")
	secret, _, _ := sec.Value("secretkey")
	if access == "" || secret == "" || manifest.HasValuePlaceholder(access) || manifest.HasValuePlaceholder(secret) {
		// Empty covers both missing-key and PLACEHOLDER-wiped values
		// (the ExternalSecret case). Same sentinel so
		// --allow-missing-secrets covers both shapes.
		return nil, source.MissingSecretErr(b.Named(), b.SecretRef.Name, "missing accesskey/secretkey")
	}
	return credentials.NewStaticV4(access, secret, ""), nil
}
