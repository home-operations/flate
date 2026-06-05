package helmchart

import (
	"fmt"
	"os"

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
	if f.Secrets == nil {
		// Same sentinel as "secret not found" so --allow-missing-secrets
		// covers both shapes — the dependency is equally unresolved.
		return nil, fmt.Errorf("%w: HelmRepository %s/%s references secretRef but no SecretGetter is wired",
			manifest.ErrMissingSecret, r.Namespace, r.Name)
	}
	sec := f.Secrets(r.Namespace, r.SecretRef.Name)
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
	if f.Secrets == nil {
		return nil, noCleanup, fmt.Errorf("%w: HelmRepository %s/%s references certSecretRef but no SecretGetter is wired",
			manifest.ErrMissingSecret, r.Namespace, r.Name)
	}
	sec := f.Secrets(r.Namespace, r.CertSecretRef.Name)
	if sec == nil {
		return nil, noCleanup, fmt.Errorf("%w: HelmRepository %s/%s: cert secret %s/%s not found",
			manifest.ErrMissingSecret, r.Namespace, r.Name, r.Namespace, r.CertSecretRef.Name)
	}

	var tmpFiles []string
	cleanup := func() {
		for _, p := range tmpFiles {
			_ = os.Remove(p)
		}
	}
	writeKey := func(key string) (string, error) {
		v := source.StringFromSecret(sec, key)
		if v == "" {
			return "", nil
		}
		tmp, err := os.CreateTemp(f.tmpDir, "helm-tls-*.pem")
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
		return nil, noCleanup, fmt.Errorf("%w: HelmRepository %s/%s: certSecretRef %s/%s contains none of tls.crt / tls.key / ca.crt",
			manifest.ErrMissingSecret, r.Namespace, r.Name, r.Namespace, r.CertSecretRef.Name)
	}
	return []getter.Option{getter.WithTLSClientConfig(certPath, keyPath, caPath)}, cleanup, nil
}
