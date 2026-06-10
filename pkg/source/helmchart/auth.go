package helmchart

import (
	"fmt"

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

// helmRepoTLSOptions resolves spec.certSecretRef into helm getter options.
// The Secret carries one or both of (tls.crt, tls.key) for client-cert auth
// plus optional ca.crt. Each present file is materialized to a temp file
// (helm getter v4's WithTLSClientConfig takes paths) removed by cleanup.
func (f *Fetcher) helmRepoTLSOptions(r *manifest.HelmRepository) ([]getter.Option, func(), error) {
	noCleanup := func() {}
	if r.CertSecretRef == nil {
		return nil, noCleanup, nil
	}
	if f.secrets == nil {
		// certSecretRef carries TLS trust material — an unwired SecretGetter
		// is a wiring bug, not a missing-in-cluster secret. Fail loud (no
		// ErrMissingSecret wrap, which --allow-missing-secrets would soft-skip):
		// silently dropping TLS material is a security downgrade. Mirrors
		// source.ResolveCertSecret, the canonical cross-kind cert helper.
		return nil, noCleanup, fmt.Errorf("%s references certSecretRef but no SecretGetter is wired", helmID(r))
	}
	sec := f.secrets(r.Namespace, r.CertSecretRef.Name)
	if sec == nil {
		// A genuinely-absent secret IS the --allow-missing-secrets case
		// (cert materialized live, not in git) — same sentinel git/oci/bucket
		// use via source.ResolveCertSecret.
		return nil, noCleanup, source.MissingSecretErr("HelmRepository", r.Namespace, r.Name, r.CertSecretRef.Name, "not found")
	}

	// Scope the materialized PEM files under the fetcher's per-fetch
	// tmpDir; helm's getter only reads them. cleanup removes every file
	// a successful write registered (see source.TempFiles).
	tf := source.NewTempFiles(f.tmpDir)
	cleanup := tf.Cleanup

	// Materialize tls.crt / tls.key / ca.crt in the order WithTLSClientConfig
	// takes them; a missing/empty key yields no file (path stays "").
	var paths [3]string
	for i, key := range [3]string{"tls.crt", "tls.key", "ca.crt"} {
		p, err := tf.Write("helm-tls-*.pem", source.StringFromSecret(sec, key))
		if err != nil {
			cleanup()
			return nil, noCleanup, err
		}
		paths[i] = p
	}
	if paths == [3]string{} {
		cleanup()
		// A secret present but carrying none of the TLS keys is malformed
		// config — fail loud (no ErrMissingSecret wrap), like BuildTLSConfig.
		return nil, noCleanup, fmt.Errorf("%s: certSecretRef %s/%s contains none of tls.crt / tls.key / ca.crt",
			helmID(r), r.Namespace, r.CertSecretRef.Name)
	}
	// A client cert needs both halves; reject a half-pair rather than feed
	// helm a cert without its key (or vice versa). Matches source.BuildTLSConfig,
	// which the OCI/Bucket paths enforce via ResolveCertSecret.
	if (paths[0] == "") != (paths[1] == "") {
		cleanup()
		return nil, noCleanup, fmt.Errorf("%s: certSecretRef %s/%s must provide both tls.crt and tls.key (or neither)",
			helmID(r), r.Namespace, r.CertSecretRef.Name)
	}
	return []getter.Option{getter.WithTLSClientConfig(paths[0], paths[1], paths[2])}, cleanup, nil
}
