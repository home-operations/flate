package oci

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"

	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// dockerConfigJSONKey is the Secret data key a kubernetes.io/dockerconfigjson
// Secret stores its credentials under.
const dockerConfigJSONKey = ".dockerconfigjson"

// newRepoClient builds the oras registry client for repo: it parses the ref,
// loads credentials from registryConfig, and wires a bounded HTTP transport
// (TLS + proxy) plus docker-style auth.
//
// Retries are owned by the Fetch-level retry decorator (source.WithRetry) so
// they happen once, uniformly, across every source kind — this client uses a
// plain, NON-retrying transport. source.NewHTTPTransport always returns a
// bounded transport (a http.DefaultTransport clone carrying
// ResponseHeaderTimeout as a liveness backstop, plus any TLS/proxy), so oras
// never falls back to its retry-enabled auth.DefaultClient and a black-holed
// registry can't hang the fetch waiting on response headers.
func newRepoClient(repo *manifest.OCIRepository, registryConfig string, tlsCfg *tls.Config, proxy *source.ProxyConfig) (*remote.Repository, error) {
	// parseOCIRef strips any oci:// prefix itself, so pass repo.URL as-is.
	reference, err := parseOCIRef(repo.URL)
	if err != nil {
		return nil, err
	}
	repoClient, err := remote.NewRepository(reference)
	if err != nil {
		return nil, fmt.Errorf("oras: %w", err)
	}
	credStore, err := source.RegistryCredentialStore(registryConfig)
	if err != nil {
		return nil, err
	}
	transport, err := source.NewHTTPTransport(tlsCfg, proxy)
	if err != nil {
		return nil, err
	}
	authClient := &auth.Client{Client: &http.Client{Transport: transport}}
	if credStore != nil {
		authClient.Credential = credentials.Credential(credStore)
	}
	repoClient.Client = authClient
	return repoClient, nil
}

// resolveTLS builds a *tls.Config from spec.certSecretRef (PEM-encoded
// tls.crt + tls.key + ca.crt — any subset acceptable) and/or spec.insecure.
// Returns nil when no TLS customization is needed.
func (f *Fetcher) resolveTLS(repo *manifest.OCIRepository) (*tls.Config, error) {
	if repo.CertSecretRef == nil && !repo.Insecure {
		return nil, nil
	}
	cfg, err := source.ResolveCertSecret(f.Secrets, repo.Namespace, "OCIRepository",
		repo.Namespace+"/"+repo.Name, repo.CertSecretRef)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if repo.Insecure {
		cfg.InsecureSkipVerify = true //nolint:gosec // honoring user-declared spec.insecure
	}
	return cfg, nil
}

// resolveRegistryConfig picks the credential source for a fetch.
// Precedence:
//  1. per-OCIRepository spec.secretRef (a kubernetes.io/dockerconfigjson
//     Secret materialized to a temp file).
//  2. global --registry-config path (f.RegistryConfig).
//  3. docker's default lookup (~/.docker/config.json), handled inside
//     RegistryCredentialStore when configPath is empty.
//
// When the secretRef can't resolve offline — the Secret is missing, or its
// .dockerconfigjson is absent or PLACEHOLDER-wiped — steps 2 and 3 are tried
// before giving up (#999). Each candidate store is probed for an actual
// credential for the repo's registry, and the first store holding one wins.
// The probe is what keeps ErrMissingSecret the genuine last resort: falling
// through blindly would let a config file that exists but doesn't cover the
// registry (the norm on CI runners) replace the sentinel — and with it
// --allow-missing-secrets and producer-backed skips — with an
// anonymous-pull 401.
//
// The cleanup func removes any temp file the SecretRef path created;
// safe to call when no temp file was made (no-op).
func (f *Fetcher) resolveRegistryConfig(ctx context.Context, repo *manifest.OCIRepository) (string, func(), error) {
	noCleanup := func() {}
	if repo.SecretRef == nil {
		return f.RegistryConfig, noCleanup, nil
	}
	if f.Secrets == nil {
		return "", noCleanup, fmt.Errorf("%s references secretRef but no source.SecretGetter is wired", ociID(repo))
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec != nil {
		if configJSON := source.StringFromSecret(sec, dockerConfigJSONKey); configJSON != "" {
			// System temp (dir ""): the docker credential store only needs the
			// file to exist for the duration of the pull.
			tf := source.NewTempFiles("")
			path, err := tf.Write("flate-oci-creds-*.json", configJSON)
			if err != nil {
				return "", noCleanup, err
			}
			return path, tf.Cleanup, nil
		}
	}
	// The secretRef is unresolvable offline. This reason wording is what
	// surfaces verbatim in skip messages when no fallback credential
	// exists, so don't rephrase it lightly.
	reason := "not found"
	if sec != nil {
		// Empty .dockerconfigjson covers both (a) the Secret has no
		// .dockerconfigjson key at all and (b) the key exists but
		// `--wipe-secrets` (always on) replaced its value with
		// PLACEHOLDER, which StringFromSecret returns as "". The
		// ExternalSecret case in #190 hits (b): the Secret manifest is
		// in-tree but its data is materialized live. Matching only the
		// literal "secret not found" path would leave the actual
		// reporter's case still failing.
		reason = "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)"
	}
	host, err := registryHost(repo.URL)
	if err != nil {
		return "", noCleanup, err
	}
	if f.RegistryConfig != "" {
		credStore, err := source.RegistryCredentialStore(f.RegistryConfig)
		if err != nil {
			return "", noCleanup, err
		}
		if _, found := source.CredentialForHost(ctx, credStore, host); found {
			slog.Warn("oci: secretRef unresolvable; authenticating via --registry-config",
				"id", ociID(repo), "secret", repo.Namespace+"/"+repo.SecretRef.Name, "registry", host)
			return f.RegistryConfig, noCleanup, nil
		}
	}
	// Empty configPath: RegistryCredentialStore falls back to docker's
	// default lookup, and so does newRepoClient when the returned path is "".
	if credStore, err := source.RegistryCredentialStore(""); err == nil && credStore != nil {
		if _, found := source.CredentialForHost(ctx, credStore, host); found {
			slog.Warn("oci: secretRef unresolvable; authenticating via docker config",
				"id", ociID(repo), "secret", repo.Namespace+"/"+repo.SecretRef.Name, "registry", host)
			return "", noCleanup, nil
		}
	}
	return "", noCleanup, source.MissingSecretErr("OCIRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, reason)
}
