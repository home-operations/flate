package helmchart

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// helmRepoAuthIdentity is the cache auth tag for a HelmRepository's
// acquired chart and index keys: it folds the SecretRef (basic auth) and CertSecretRef
// (TLS material) so a public-index resolution can't leak across a different-
// auth repo that shares the URL. Mirrors oci.authIdentity.
func helmRepoAuthIdentity(r *manifest.HelmRepository) string {
	return source.AuthIdentityFromRefs(r.Namespace, r.SecretRef, r.CertSecretRef)
}

func (f *Fetcher) helmRepoCredentials(r *manifest.HelmRepository) (string, string, error) {
	if r.SecretRef == nil {
		return "", "", nil
	}
	if f.secrets == nil {
		return "", "", fmt.Errorf("%w: %s references secretRef but no SecretGetter is wired", manifest.ErrMissingSecret, helmID(r))
	}
	sec := f.secrets(r.Namespace, r.SecretRef.Name)
	if sec == nil {
		return "", "", source.MissingSecretErr("HelmRepository", r.Namespace, r.Name, r.SecretRef.Name, "not found")
	}
	return source.BasicAuthFromSecret(sec, "HelmRepository", r.Namespace, r.Name, r.SecretRef.Name)
}

// helmRepoTransport resolves custom TLS before the private transport is wrapped.
// Without certSecretRef, httpGet reuses this fetcher's anonymous transport.
func (f *Fetcher) helmRepoTransport(r *manifest.HelmRepository) (*http.Transport, error) {
	if r.CertSecretRef == nil {
		return nil, nil
	}
	tlsCfg, err := source.ResolveCertSecret(f.secrets, r.Namespace, "HelmRepository", helmID(r), r.CertSecretRef)
	if err != nil {
		return nil, err
	}
	return helmTransport(tlsCfg)
}

// helmTransport builds the helm-specific HTTP transport kernel: the SSRF
// egress guard (source.NewHTTPTransport) carrying tlsCfg, with compression
// disabled to match helm's own getter transport (chart tarballs are already
// compressed — the only source kind that disables it).
func helmTransport(tlsCfg *tls.Config) (*http.Transport, error) {
	tr, err := source.NewHTTPTransport(tlsCfg, nil)
	if err != nil {
		return nil, err
	}
	tr.DisableCompression = true
	return tr, nil
}
