package helmchart

import (
	"fmt"

	"helm.sh/helm/v4/pkg/getter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

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
		return nil, fmt.Errorf("%w: HelmRepository %s/%s references secretRef but no SecretGetter is wired",
			manifest.ErrMissingSecret, r.Namespace, r.Name)
	}
	sec := f.secrets(r.Namespace, r.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr("HelmRepository", r.Namespace, r.Name, r.SecretRef.Name, "not found")
	}
	username := source.StringFromSecret(sec, "username")
	password := source.StringFromSecret(sec, "password")
	if username == "" || password == "" {
		// Empty covers missing-key and PLACEHOLDER-wiped values (the
		// ExternalSecret case). Same sentinel.
		return nil, source.MissingSecretErr("HelmRepository", r.Namespace, r.Name, r.SecretRef.Name, "missing username/password")
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
		return nil, noCleanup, fmt.Errorf("HelmRepository %s/%s references certSecretRef but no SecretGetter is wired",
			r.Namespace, r.Name)
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
	// a successful writeKey registered (see source.TempFiles).
	tf := source.NewTempFiles(f.tmpDir)
	cleanup := tf.Cleanup
	writeKey := func(key string) (string, error) {
		return tf.Write("helm-tls-*.pem", source.StringFromSecret(sec, key))
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
		// A secret present but carrying none of the TLS keys is malformed
		// config — fail loud (no ErrMissingSecret wrap), like BuildTLSConfig.
		return nil, noCleanup, fmt.Errorf("HelmRepository %s/%s: certSecretRef %s/%s contains none of tls.crt / tls.key / ca.crt",
			r.Namespace, r.Name, r.Namespace, r.CertSecretRef.Name)
	}
	return []getter.Option{getter.WithTLSClientConfig(certPath, keyPath, caPath)}, cleanup, nil
}
