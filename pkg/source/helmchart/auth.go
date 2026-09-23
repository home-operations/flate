package helmchart

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

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
//
// When the secretRef can't resolve offline — the Secret is missing, or its
// username/password are absent or PLACEHOLDER-wiped — the global docker-style
// credential stores are probed for the repo's host before giving up (#999):
// --registry-config first, then docker's default lookup. Each probe must
// find an actual credential for the host, so a config covering other
// registries (the norm on CI runners) can't replace the ErrMissingSecret
// sentinel — --allow-missing-secrets and producer-backed skips keep working.
func (f *Fetcher) helmRepoAuthOptions(ctx context.Context, r *manifest.HelmRepository) ([]getter.Option, error) {
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
	var username, password string
	var err error
	if sec == nil {
		err = source.MissingSecretErr("HelmRepository", r.Namespace, r.Name, r.SecretRef.Name, "not found")
	} else {
		username, password, err = source.BasicAuthFromSecret(sec, "HelmRepository", r.Namespace, r.Name, r.SecretRef.Name)
	}
	if err == nil {
		return basicAuthOptions(r, username, password), nil
	}
	u, p, ferr := f.fallbackBasicAuth(ctx, r)
	switch {
	case ferr != nil:
		return nil, ferr
	case u != "":
		return basicAuthOptions(r, u, p), nil
	}
	return nil, err
}

// basicAuthOptions builds the getter options for username/password auth,
// honoring spec.passCredentials on redirects. WithURL is load-bearing:
// helm's getter only attaches basic auth when the option URL's scheme+host
// match the fetched URL, so without it the credentials are silently
// dropped (helm pairs WithURL with auth the same way in its own repo
// code). Cross-host chart URLs then need spec.passCredentials.
func basicAuthOptions(r *manifest.HelmRepository, username, password string) []getter.Option {
	opts := []getter.Option{getter.WithURL(r.URL), getter.WithBasicAuth(username, password)}
	if r.PassCredentials {
		opts = append(opts, getter.WithPassCredentialsAll(true))
	}
	return opts
}

// fallbackBasicAuth probes the global docker-style credential stores for a
// basic-auth credential for r's host (the docker config's auths keys are
// bare host[:port]): --registry-config first, then docker's default lookup
// (#999). Returns a non-empty username when a credential was found; a
// non-nil error is a hard failure (a corrupt explicit --registry-config)
// that must not degrade to the missing-secret sentinel.
func (f *Fetcher) fallbackBasicAuth(ctx context.Context, r *manifest.HelmRepository) (string, string, error) {
	host, ok := repoHost(r.URL)
	if !ok {
		return "", "", nil
	}
	if f.registryConfig != "" {
		credStore, err := source.RegistryCredentialStore(f.registryConfig)
		if err != nil {
			return "", "", err
		}
		if cred, found := source.CredentialForHost(ctx, credStore, host); found {
			slog.Warn("helmchart: secretRef unresolvable; authenticating via --registry-config",
				"id", helmID(r), "secret", r.Namespace+"/"+r.SecretRef.Name, "registry", host)
			return cred.Username, cred.Password, nil
		}
	}
	// Empty configPath: RegistryCredentialStore falls back to docker's
	// default lookup (~/.docker/config.json).
	if credStore, err := source.RegistryCredentialStore(""); err == nil && credStore != nil {
		if cred, found := source.CredentialForHost(ctx, credStore, host); found {
			slog.Warn("helmchart: secretRef unresolvable; authenticating via docker config",
				"id", helmID(r), "secret", r.Namespace+"/"+r.SecretRef.Name, "registry", host)
			return cred.Username, cred.Password, nil
		}
	}
	return "", "", nil
}

// repoHost extracts host[:port] from a HelmRepository URL for credential
// probing. Reports false when the URL carries no host — the fetch fails on
// its own later, and the fallback stays silent rather than turning a URL
// problem into an auth error.
func repoHost(repoURL string) (string, bool) {
	u, err := url.Parse(repoURL)
	if err != nil || u.Host == "" {
		return "", false
	}
	return u.Host, true
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
