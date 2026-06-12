package helmchart

import (
	"fmt"
	"net/http"

	"helm.sh/helm/v4/pkg/getter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// helmRepoAuthIdentity is the cache auth tag for a HelmRepository's
// resolve/blob slots: it folds the SecretRef (basic auth) and CertSecretRef
// (TLS material) so a public-index resolution can't leak across a different-
// auth repo that shares the URL. Mirrors oci.authIdentity.
func helmRepoAuthIdentity(r *manifest.HelmRepository) string {
	return source.AuthIdentityFromRefs(r.Namespace, r.SecretRef, r.CertSecretRef)
}

// helmRepoAuthOptions resolves SecretRef credentials for a HelmRepository
// into helm getter options. Returns nil options when no SecretRef is set
// (anonymous). Username/password basic auth + optional PassCredentials.
func (f *Fetcher) helmRepoAuthOptions(r *manifest.HelmRepository) ([]getter.Option, error) {
	if r.SecretRef == nil {
		return nil, nil
	}
	if f.secrets == nil {
		// Same sentinel as "secret not found" so --allow-missing-secrets
		// covers both shapes — the dependency is equally unresolved.
		return nil, fmt.Errorf("%w: %s references secretRef but no SecretGetter is wired",
			manifest.ErrMissingSecret, helmID(r))
	}
	sec := f.secrets(r.Namespace, r.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr("HelmRepository", r.Namespace, r.Name, r.SecretRef.Name, "not found")
	}
	username, password, err := source.BasicAuthFromSecret(sec, "HelmRepository", r.Namespace, r.Name, r.SecretRef.Name)
	if err != nil {
		return nil, err
	}
	opts := []getter.Option{getter.WithBasicAuth(username, password)}
	if r.PassCredentials {
		opts = append(opts, getter.WithPassCredentialsAll(true))
	}
	return opts, nil
}

// helmRepoTransport builds a per-repo guarded HTTP transport when a
// HelmRepository sets spec.certSecretRef, returning nil for the common
// no-custom-TLS case (httpGet's package-default guarded transport then applies).
// The transport carries client-cert / CA TLS resolved via source.ResolveCertSecret
// (the canonical cross-kind cert helper — same error sentinels as git/OCI/bucket,
// incl. the --allow-missing-secrets case) AND the SSRF egress guard
// (source.NewHTTPTransport wraps it). It is passed to helm's getter via
// getter.WithTransport, the only way to apply BOTH flate's dial guard and the
// repo's TLS — helm's getter treats WithTransport and WithTLSClientConfig as
// mutually exclusive. DisableCompression mirrors helm's own getter transport
// (chart tarballs are already compressed).
func (f *Fetcher) helmRepoTransport(r *manifest.HelmRepository) (*http.Transport, error) {
	if r.CertSecretRef == nil {
		return nil, nil
	}
	tlsCfg, err := source.ResolveCertSecret(f.secrets, r.Namespace, "HelmRepository", helmID(r), r.CertSecretRef)
	if err != nil {
		return nil, err
	}
	tr, err := source.NewHTTPTransport(tlsCfg, nil)
	if err != nil {
		return nil, err
	}
	tr.DisableCompression = true
	return tr, nil
}
