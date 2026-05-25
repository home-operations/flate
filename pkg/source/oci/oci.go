// Package oci implements the source.Fetcher for KindOCIRepository
// via oras-go. Generic provider only — IRSA / Workload Identity is
// out of scope for offline flate.
package oci

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Masterminds/semver/v3"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2"
	orasoci "oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/retry"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the Fetcher implementation for KindOCIRepository.
// RegistryConfig is the global --registry-config docker-style
// config.json path used when no per-repo SecretRef is set. Secrets is
// the per-repo source.SecretGetter (typically the orchestrator-provided
// Store.GetByName), required when any OCIRepository has spec.secretRef
// pointing at a kubernetes.io/dockerconfigjson Secret.
type Fetcher struct {
	Cache          *source.Cache
	RegistryConfig string
	Secrets        source.SecretGetter
}

// Fetch implements source.Fetcher for *manifest.OCIRepository.
func (f *Fetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	repo, ok := obj.(*manifest.OCIRepository)
	if !ok {
		return nil, fmt.Errorf("%w: Fetcher: unexpected payload %T", manifest.ErrInput, obj)
	}
	if repo.Provider != "" && repo.Provider != sourcev1.GenericOCIProvider {
		return nil, fmt.Errorf(
			"OCIRepository %s/%s provider %q is not implemented; flate currently supports only %q (SecretRef or --registry-config credentials)",
			repo.Namespace, repo.Name, repo.Provider, sourcev1.GenericOCIProvider,
		)
	}
	configPath, cleanup, err := f.resolveRegistryConfig(repo)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	tlsCfg, err := f.resolveTLS(repo)
	if err != nil {
		return nil, err
	}
	proxy, err := source.ResolveProxy(f.Secrets, repo.Namespace, "OCIRepository",
		repo.Namespace+"/"+repo.Name, repo.ProxySecretRef)
	if err != nil {
		return nil, err
	}
	return fetch(ctx, f, repo, configPath, tlsCfg, proxy)
}

// resolveTLS builds a *tls.Config from spec.certSecretRef (PEM-encoded
// tls.crt + tls.key + ca.crt — any subset acceptable) and/or
// spec.insecure. Returns nil when no TLS customization is needed.
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
		return "", noCleanup, fmt.Errorf("OCIRepository %s/%s references secretRef but no source.SecretGetter is wired",
			repo.Namespace, repo.Name)
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return "", noCleanup, fmt.Errorf("%w: OCIRepository %s/%s: secret %s/%s not found",
			manifest.ErrMissingSecret, repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name)
	}
	configJSON := source.StringFromSecret(sec, ".dockerconfigjson")
	if configJSON == "" {
		// Empty here covers both (a) the Secret has no .dockerconfigjson
		// key at all and (b) the key exists but `--wipe-secrets` (always
		// on) replaced its value with PLACEHOLDER, which StringFromSecret
		// returns as "". The ExternalSecret case in #190 hits (b): the
		// Secret manifest is in-tree but its data is materialized live.
		// Same ErrMissingSecret sentinel so --allow-missing-secrets
		// covers both — matching only the literal "secret not found"
		// path would leave the actual reporter's case still failing.
		return "", noCleanup, fmt.Errorf(
			"%w: OCIRepository %s/%s: secret %s/%s missing .dockerconfigjson "+
				"(must be type kubernetes.io/dockerconfigjson)",
			manifest.ErrMissingSecret, repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name)
	}
	tmp, err := os.CreateTemp("", "flate-oci-creds-*.json")
	if err != nil {
		return "", noCleanup, fmt.Errorf("temp docker config: %w", err)
	}
	if _, err := tmp.WriteString(configJSON); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", noCleanup, fmt.Errorf("write temp docker config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return "", noCleanup, fmt.Errorf("close temp docker config: %w", err)
	}
	cleanup := func() { _ = os.Remove(tmp.Name()) }
	return tmp.Name(), cleanup, nil
}

// fetch pulls the OCIRepository artifact into cache. Credentials are
// read from a docker-style config.json honored by oras-go's
// credentials.NewFileStore. When spec.ref.semver is set, the registry
// is listed and the highest matching tag (filtered by semverFilter, if
// any) is resolved before pulling. When spec.verify is set, the pulled
// digest is verified against the trusted public keys before returning.
func fetch(ctx context.Context, f *Fetcher, repo *manifest.OCIRepository, registryConfig string, tlsCfg *tls.Config, proxy *source.ProxyConfig) (*store.SourceArtifact, error) {
	cache := f.Cache
	// Note: Fetch already type-asserts repo non-nil before calling
	// fetch(), so no nil check needed here.
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: OCIRepository %s missing url", manifest.ErrInput, repo.RepoName())
	}

	reference, err := parseOCIRef("oci://" + strings.TrimPrefix(repo.URL, "oci://"))
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
	// Compose the http.Client transport: oras's retry transport over a
	// customized http.Transport when TLS or proxy is configured. Without
	// either, oras's default is used.
	var httpClient *http.Client
	if tlsCfg != nil || proxy != nil {
		baseTransport := http.DefaultTransport.(*http.Transport).Clone()
		if tlsCfg != nil {
			baseTransport.TLSClientConfig = tlsCfg
		}
		if proxy != nil {
			pfn, perr := proxy.HTTPProxyFunc()
			if perr != nil {
				return nil, perr
			}
			baseTransport.Proxy = pfn
		}
		httpClient = &http.Client{Transport: retry.NewTransport(baseTransport)}
	}
	if credStore != nil {
		ac := &auth.Client{Credential: credentials.Credential(credStore)}
		if httpClient != nil {
			ac.Client = httpClient
		}
		repoClient.Client = ac
	} else if httpClient != nil {
		// No auth needed but TLS still has to be configured.
		repoClient.Client = &auth.Client{Client: httpClient}
	}

	// Resolve spec.ref into a concrete (tag-or-digest) BEFORE choosing
	// the cache slot, so different semver matches don't share a slot.
	var ref manifest.OCIRepositoryRef
	if repo.Reference != nil {
		ref = *repo.Reference
	}
	if ref.SemVer != "" {
		resolved, err := resolveOCISemver(ctx, repoClient, ref.SemVer, ref.SemverFilter)
		if err != nil {
			return nil, fmt.Errorf("OCIRepository %s semver: %w", repo.RepoName(), err)
		}
		ref = manifest.OCIRepositoryRef{Tag: resolved}
	}

	versioned := versionedURL(repo.URL, ref)
	slot, exists, release, err := cache.Slot(versioned, "")
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", versioned, err)
	}
	defer release()
	if exists {
		cachedDigest := readCachedDigest(slot)
		// `.flate-digest` is written as the FINAL step of a successful
		// fetch, so its absence on a non-empty slot signals a crashed
		// or aborted prior run that left partial blobs/ layout behind.
		// (Slot.exists returns true on ANY non-empty dir.) Without
		// this guard, the next fetch silently serves the corrupt
		// slot as a cache hit. Fall through to a fresh pull, and only
		// honor ref.Digest as the cachedDigest when one is explicitly
		// pinned in spec.ref.digest — the orig spec-pin path that
		// never wrote `.flate-digest` in the first place.
		if cachedDigest == "" && ref.Digest == "" {
			_ = cache.Reset(slot)
			exists = false
		} else if cachedDigest == "" {
			cachedDigest = ref.Digest
		}
		// When verification is configured, re-verify the cached digest
		// against the registry. Cheap (one metadata fetch) and closes the
		// gap where a slot was populated under a prior policy.
		if exists && repo.Verify != nil {
			if err := f.verifyCosignSignature(ctx, repoClient, repo, cachedDigest); err != nil {
				// Cosign rejected the cached bytes. Without resetting, the
				// next reconcile re-hits the same poisoned slot and fails
				// verify identically — a hard-to-debug repeated failure.
				// Reset so a fresh pull is attempted.
				_ = cache.Reset(slot)
				return nil, err
			}
		}
		if exists {
			return &store.SourceArtifact{
				Kind: manifest.KindOCIRepository,
				URL:  repo.URL, LocalPath: slot,
				Revision: ociRevision(ref, cachedDigest),
				Digest:   cachedDigest,
			}, nil
		}
	}

	tag := versionTag(ref)
	if tag == "" {
		tag = "latest"
	}

	// OCI Image Layout content store: blobs land at
	// `slot/blobs/<algo>/<hex>` regardless of title annotations, so we
	// no longer need a custom fallback to keep unnamed blobs on disk.
	// applyLayerSelector reads from the same standard layout and wipes
	// it after extracting the selected layer.
	dest, err := orasoci.New(slot)
	if err != nil {
		return nil, fmt.Errorf("oras oci store: %w", err)
	}
	// content/oci.Store has no Close() method — all writes flush
	// synchronously per blob via os.Rename — so reset-on-error is the
	// only cleanup we need.
	resetOnErr := func() { _ = cache.Reset(slot) }

	desc, err := oras.Copy(ctx, repoClient, tag, dest, tag, oras.DefaultCopyOptions)
	if err != nil {
		resetOnErr()
		return nil, fmt.Errorf("oras copy %s: %w", versioned, err)
	}

	digest := desc.Digest.String()
	if repo.Verify != nil {
		if err := f.verifyCosignSignature(ctx, repoClient, repo, digest); err != nil {
			resetOnErr()
			return nil, err
		}
	}
	if err := applyLayerSelector(ctx, slot, desc.Digest.String(), repo.LayerSelector); err != nil {
		resetOnErr()
		return nil, fmt.Errorf("OCIRepository %s/%s: layer select: %w", repo.Namespace, repo.Name, err)
	}
	// Source-controller's default ignore set includes `*.tar.gz`. For
	// operation=copy the artifact IS the .tar.gz file we just produced
	// at slot/<copiedLayerFilename>, so running ApplyIgnore would
	// delete it. Skip the ignore pass in that case; for operation=
	// extract the slot holds the extracted directory tree and the
	// ignore semantics apply as Flux ships them.
	if effectiveLayerOperation(repo.LayerSelector) == manifest.OCILayerOperationExtract {
		if err := source.ApplyIgnore(slot, repo.Ignore); err != nil {
			resetOnErr()
			return nil, fmt.Errorf("OCIRepository %s/%s: %w", repo.Namespace, repo.Name, err)
		}
	}
	// Persist the resolved digest so a subsequent cache hit can
	// re-verify against the exact bytes we wrote, even when the spec
	// pinned only a tag. A write failure isn't fatal — the next fetch
	// just falls through to a fresh pull — but it does silently weaken
	// spec.verify on cache hits, so log it.
	if err := writeCachedDigest(slot, digest); err != nil {
		slog.Warn("oci: failed to persist cached digest",
			"ociRepository", repo.Namespace+"/"+repo.Name,
			"err", err)
	}
	return &store.SourceArtifact{
		Kind: manifest.KindOCIRepository,
		URL:  repo.URL, LocalPath: slot,
		Revision: ociRevision(ref, digest),
		Digest:   digest,
		Size:     desc.Size,
	}, nil
}

// cachedDigestFile is the slot-relative path where flate records the
// resolved digest of an OCIRepository pull. Used to re-verify on cache
// hit when spec.verify is configured.
const cachedDigestFile = ".flate-digest"

func writeCachedDigest(slot, digest string) error {
	return os.WriteFile(filepath.Join(slot, cachedDigestFile), []byte(digest), 0o600)
}

func readCachedDigest(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, cachedDigestFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// ociRevision composes a Flux-style "<tag>@<digest>" revision string.
// When tag is empty, falls back to bare digest; when digest is empty,
// returns just the tag. Matches source-controller's ocirepository
// revision conventions.
func ociRevision(ref manifest.OCIRepositoryRef, digest string) string {
	tag := ref.Tag
	if tag == "" && ref.Digest == "" {
		tag = "latest"
	}
	switch {
	case tag != "" && digest != "":
		return tag + "@" + digest
	case digest != "":
		return digest
	}
	return tag
}

// versionedURL composes a Flux-style versioned URL from a base + ref.
// Used here for cache-slot keying after semver resolution.
func versionedURL(base string, ref manifest.OCIRepositoryRef) string {
	switch {
	case ref.Digest != "":
		return base + "@" + ref.Digest
	case ref.Tag != "":
		return base + ":" + ref.Tag
	}
	return base
}

// resolveOCISemver lists the remote tags, applies an optional regex
// filter, then returns the highest tag matching the semver constraint.
// Mirrors source-controller's `getTagBySemver` (ocirepository_controller.go).
func resolveOCISemver(ctx context.Context, repoClient *remote.Repository, expr, filterPattern string) (string, error) {
	var collected []string
	if err := repoClient.Tags(ctx, "", func(tags []string) error {
		collected = append(collected, tags...)
		return nil
	}); err != nil {
		return "", fmt.Errorf("list tags: %w", err)
	}
	return pickSemverTag(collected, expr, filterPattern)
}

// pickSemverTag picks the highest semver-matching tag from a list,
// applying an optional regex filter. Pure function so it's testable
// without a real registry.
func pickSemverTag(tags []string, expr, filterPattern string) (string, error) {
	constraint, err := semver.NewConstraint(expr)
	if err != nil {
		return "", fmt.Errorf("semver %q: %w", expr, err)
	}
	var pattern *regexp.Regexp
	if filterPattern != "" {
		pattern, err = regexp.Compile(filterPattern)
		if err != nil {
			return "", fmt.Errorf("semverFilter %q: %w", filterPattern, err)
		}
	}

	var matching semver.Collection
	var matchingTags []string
	for _, tag := range tags {
		if pattern != nil && !pattern.MatchString(tag) {
			continue
		}
		v, perr := semver.NewVersion(tag)
		if perr != nil {
			continue
		}
		if constraint.Check(v) {
			matching = append(matching, v)
			matchingTags = append(matchingTags, tag)
		}
	}
	if len(matching) == 0 {
		return "", fmt.Errorf("no tag matched semver %q (filter %q)", expr, filterPattern)
	}
	// Highest match wins — find the max-index by walking once so the
	// parallel matching[]/matchingTags[] stay aligned.
	hi := 0
	for i := 1; i < len(matching); i++ {
		if matching[hi].LessThan(matching[i]) {
			hi = i
		}
	}
	return matchingTags[hi], nil
}

// loadCredentials returns a credentials.Store backed by the given config
// path. An empty configPath uses the docker default lookup.
func loadCredentials(configPath string) (credentials.Store, error) {
	opts := credentials.StoreOptions{AllowPlaintextPut: false}
	if configPath != "" {
		s, err := credentials.NewFileStore(configPath)
		if err != nil {
			return nil, fmt.Errorf("load credentials %s: %w", configPath, err)
		}
		return s, nil
	}
	s, err := credentials.NewStoreFromDocker(opts)
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

// parseOCIRef converts a Flux versioned URL into the form oras-go expects:
//
//	oci://ghcr.io/owner/chart:tag  → ghcr.io/owner/chart
//	oci://ghcr.io/owner/chart@sha  → ghcr.io/owner/chart
//
// The tag/digest is dropped here and re-supplied to oras.Copy below.
func parseOCIRef(versioned string) (string, error) {
	versioned = strings.TrimPrefix(versioned, "oci://")
	// Strip ":<tag>" or "@<digest>" portion for the reference; oras
	// takes them separately.
	if i := strings.LastIndex(versioned, "@"); i > 0 {
		versioned = versioned[:i]
	}
	if i := strings.LastIndex(versioned, ":"); i > 0 {
		// Don't confuse port numbers with tags ("registry:5000/x").
		if !strings.Contains(versioned[i+1:], "/") {
			versioned = versioned[:i]
		}
	}
	if _, err := url.Parse("oci://" + versioned); err != nil {
		return "", fmt.Errorf("parse OCI ref %q: %w", versioned, err)
	}
	return versioned, nil
}

func versionTag(ref manifest.OCIRepositoryRef) string {
	switch {
	case ref.Digest != "":
		return ref.Digest
	case ref.Tag != "":
		return ref.Tag
	case ref.SemVer != "":
		return ref.SemVer
	}
	return ""
}
