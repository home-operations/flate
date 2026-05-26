package bucket

import (
	"crypto/tls"
	"net/http"

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
