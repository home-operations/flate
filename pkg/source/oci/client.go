package oci

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

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
	credStore, err := loadCredentials(registryConfig)
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
//     loadCredentials when configPath is empty.
//
// The cleanup func removes any temp file the SecretRef path created;
// safe to call when no temp file was made (no-op).
func (f *Fetcher) resolveRegistryConfig(repo *manifest.OCIRepository) (string, func(), error) {
	noCleanup := func() {}
	if repo.SecretRef == nil {
		return f.RegistryConfig, noCleanup, nil
	}
	if f.Secrets == nil {
		return "", noCleanup, fmt.Errorf("%s references secretRef but no source.SecretGetter is wired", ociID(repo))
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return "", noCleanup, source.MissingSecretErr("OCIRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "not found")
	}
	configJSON := source.StringFromSecret(sec, dockerConfigJSONKey)
	if configJSON == "" {
		// Empty here covers both (a) the Secret has no .dockerconfigjson
		// key at all and (b) the key exists but `--wipe-secrets` (always
		// on) replaced its value with PLACEHOLDER, which StringFromSecret
		// returns as "". The ExternalSecret case in #190 hits (b): the
		// Secret manifest is in-tree but its data is materialized live.
		// Same ErrMissingSecret sentinel so --allow-missing-secrets
		// covers both — matching only the literal "secret not found"
		// path would leave the actual reporter's case still failing.
		return "", noCleanup, source.MissingSecretErr("OCIRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)")
	}
	// System temp (dir ""): the docker credential store only needs the
	// file to exist for the duration of the pull.
	tf := source.NewTempFiles("")
	path, err := tf.Write("flate-oci-creds-*.json", configJSON)
	if err != nil {
		return "", noCleanup, err
	}
	return path, tf.Cleanup, nil
}

// loadCredentials returns a credentials.Store backed by the given config
// path. An empty configPath uses the docker default lookup.
func loadCredentials(configPath string) (credentials.Store, error) {
	if configPath != "" {
		s, err := credentials.NewFileStore(configPath)
		if err != nil {
			return nil, fmt.Errorf("load credentials %s: %w", configPath, err)
		}
		return s, nil
	}
	s, err := credentials.NewStoreFromDocker(credentials.StoreOptions{AllowPlaintextPut: false})
	if err != nil {
		// Missing docker config is not fatal — anonymous pulls work.
		// Distinguish os.ErrNotExist (the common case: no docker login
		// on this machine) from permission / corrupt-JSON errors so an
		// operator running flate with a broken ~/.docker/config.json
		// gets a breadcrumb instead of a silent "401 unauthorized"
		// from the registry.
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		slog.Debug("oci: docker credentials load failed; falling back to anonymous pulls",
			"err", err)
		return nil, nil
	}
	return s, nil
}
