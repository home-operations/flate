package helm

import (
	"fmt"
	"os"

	"helm.sh/helm/v4/pkg/getter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// helmRepoAuthOptions resolves SecretRef credentials for a HelmRepository
// into helm getter options. Returns nil options when no SecretRef is
// configured (anonymous). Username/password basic auth + optional
// PassCredentials forwarding. Insecure flag is OCI-only per Flux's
// schema so it is intentionally not surfaced here.
func (c *Client) helmRepoAuthOptions(r *manifest.HelmRepository) ([]getter.Option, error) {
	if r.SecretRef == nil {
		return nil, nil
	}
	getSec := c.secretGetter()
	if getSec == nil {
		// Use the same sentinel as the "secret not found" path so
		// --allow-missing-secrets covers both shapes — from the
		// caller's perspective the dependency is equally unresolved.
		return nil, fmt.Errorf("%w: HelmRepository %s/%s references secretRef but no SecretGetter is wired",
			manifest.ErrMissingSecret, r.Namespace, r.Name)
	}
	sec := getSec(r.Namespace, r.SecretRef.Name)
	if sec == nil {
		return nil, fmt.Errorf("%w: HelmRepository %s/%s: secret %s/%s not found",
			manifest.ErrMissingSecret, r.Namespace, r.Name, r.Namespace, r.SecretRef.Name)
	}
	username := source.StringFromSecret(sec, "username")
	password := source.StringFromSecret(sec, "password")
	if username == "" || password == "" {
		// Empty covers both missing-key and PLACEHOLDER-wiped values
		// (the ExternalSecret case). Same sentinel so
		// --allow-missing-secrets covers both shapes.
		return nil, fmt.Errorf("%w: HelmRepository %s/%s: secret %s/%s missing username/password",
			manifest.ErrMissingSecret, r.Namespace, r.Name, r.Namespace, r.SecretRef.Name)
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
	getSec := c.secretGetter()
	if getSec == nil {
		return nil, noCleanup, fmt.Errorf("%w: HelmRepository %s/%s references certSecretRef but no SecretGetter is wired",
			manifest.ErrMissingSecret, r.Namespace, r.Name)
	}
	sec := getSec(r.Namespace, r.CertSecretRef.Name)
	if sec == nil {
		return nil, noCleanup, fmt.Errorf("%w: HelmRepository %s/%s: cert secret %s/%s not found",
			manifest.ErrMissingSecret, r.Namespace, r.Name, r.Namespace, r.CertSecretRef.Name)
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
		return nil, noCleanup, fmt.Errorf("%w: HelmRepository %s/%s: certSecretRef %s/%s contains none of tls.crt / tls.key / ca.crt",
			manifest.ErrMissingSecret, r.Namespace, r.Name, r.Namespace, r.CertSecretRef.Name)
	}
	return []getter.Option{getter.WithTLSClientConfig(certPath, keyPath, caPath)}, cleanup, nil
}
