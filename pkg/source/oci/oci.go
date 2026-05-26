// Package oci implements the source.Fetcher for KindOCIRepository
// via oras-go. Generic provider only — IRSA / Workload Identity is
// out of scope for offline flate.
package oci

import (
	"cmp"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
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
	"github.com/home-operations/flate/pkg/source/atomic"
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

// Fetch implements source.TypedFetcher[*manifest.OCIRepository].
// The typed signature is wrapped via source.Wrap at orchestrator
// registration — a payload mismatch returns ErrInput once at the
// adapter site rather than panicking here.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.OCIRepository) (*store.SourceArtifact, error) {
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
		return "", noCleanup, source.MissingSecretErr("OCIRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "not found")
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
		return "", noCleanup, source.MissingSecretErr("OCIRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)")
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
	baseTransport, err := source.NewHTTPTransport(tlsCfg, proxy)
	if err != nil {
		return nil, err
	}
	var httpClient *http.Client
	if baseTransport != nil {
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
	slot, err := cache.Slot(ctx, versioned, "", authIdentity(repo))
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", versioned, err)
	}
	defer slot.Release()
	if slot.Exists {
		artifact, hit, hitErr := f.checkCacheHit(ctx, repoClient, repo, slot.Path, ref, versioned)
		if hitErr != nil {
			_ = slot.Reset()
			return nil, hitErr
		}
		if hit {
			return artifact, nil
		}
		// Reset + restage for a fresh pull.
		if err := slot.Reset(); err != nil {
			return nil, fmt.Errorf("cache reset for %s: %w", versioned, err)
		}
		if err := slot.Stage(); err != nil {
			return nil, fmt.Errorf("cache stage for %s: %w", versioned, err)
		}
	}

	tag := cmp.Or(versionTag(ref), "latest")

	// OCI Image Layout content store: blobs land at
	// `slot/blobs/<algo>/<hex>` regardless of title annotations, so we
	// no longer need a custom fallback to keep unnamed blobs on disk.
	// applyLayerSelector reads from the same standard layout and wipes
	// it after extracting the selected layer.
	//
	// Writes go to slot.Path which is the staging dir at this point;
	// on success slot.Commit() atomic-renames it over the final slot.
	// Any error path returns without committing, and Release wipes
	// the staging dir — the final slot stays absent / unchanged,
	// never torn.
	dest, err := orasoci.New(slot.Path)
	if err != nil {
		return nil, fmt.Errorf("oras oci store: %w", err)
	}

	desc, err := oras.Copy(ctx, repoClient, tag, dest, tag, oras.DefaultCopyOptions)
	if err != nil {
		return nil, fmt.Errorf("oras copy %s: %w", versioned, err)
	}

	digest := desc.Digest.String()
	if repo.Verify != nil {
		if err := f.verifyCosignSignature(ctx, repoClient, repo, digest); err != nil {
			return nil, err
		}
	}
	if err := applyLayerSelector(ctx, slot.Path, desc.Digest.String(), repo.LayerSelector); err != nil {
		return nil, fmt.Errorf("OCIRepository %s/%s: layer select: %w", repo.Namespace, repo.Name, err)
	}
	// Source-controller's default ignore set includes `*.tar.gz`. For
	// operation=copy the artifact IS the .tar.gz file we just produced
	// at slot/<copiedLayerFilename>, so running ApplyIgnore would
	// delete it. Skip the ignore pass in that case; for operation=
	// extract the slot holds the extracted directory tree and the
	// ignore semantics apply as Flux ships them.
	if effectiveLayerOperation(repo.LayerSelector) == manifest.OCILayerOperationExtract {
		if err := source.ApplyIgnore(slot.Path, repo.Ignore); err != nil {
			return nil, fmt.Errorf("OCIRepository %s/%s: %w", repo.Namespace, repo.Name, err)
		}
	}
	if err := writeCachedDigest(slot.Path, digest); err != nil {
		// A write failure here is fatal — without it the next fetch
		// would treat the committed slot as having "no marker" and
		// reset+re-pull on every reconcile. Returning the error
		// (and skipping Commit) means the staging dir is wiped by
		// Release and the next run starts clean.
		return nil, fmt.Errorf("OCIRepository %s/%s: persist cached digest: %w", repo.Namespace, repo.Name, err)
	}
	// Record the verify-policy fingerprint so subsequent cache hits
	// can skip the cosign re-fetch when spec.verify is unchanged.
	// Best-effort: a write failure costs us the next cache hit's
	// offline win (re-verify falls back to the network) but the slot
	// remains correct and the next pull will retry.
	// Persist the marker ONLY for actual verification (SecretRef set).
	// The keyless path returns success from verifyCosignSignature with
	// just a WARN — writing the marker there would silence the WARN on
	// every subsequent cache hit, because checkCacheHit sees the marker
	// match and skips re-verifyCosignSignature entirely. Leave the
	// marker absent for keyless so the WARN re-fires every reconcile.
	if repo.Verify != nil && repo.Verify.SecretRef != nil {
		if err := writeVerifyMarker(slot.Path, verifyFingerprint(repo.Verify)); err != nil {
			slog.Warn("oci: failed to persist verify marker; cache hits will re-verify online",
				"ociRepository", repo.Namespace+"/"+repo.Name, "err", err)
		}
	}
	if err := slot.Commit(); err != nil {
		return nil, fmt.Errorf("OCIRepository %s/%s: commit slot: %w", repo.Namespace, repo.Name, err)
	}
	return ociArtifact(repo, slot.Path, ref, digest, desc.Size), nil
}

// ociArtifact is the single SourceArtifact-construction helper used
// by both the cache-hit and successful-pull paths. Lifting the
// literal out keeps the two paths from drifting (the pre-helper code
// dropped Size on the cache-hit path), and a future field addition
// (e.g. spec.verify provenance metadata) only touches one site.
func ociArtifact(repo *manifest.OCIRepository, localPath string, ref manifest.OCIRepositoryRef, digest string, size int64) *store.SourceArtifact {
	return &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		URL:       repo.URL,
		LocalPath: localPath,
		Revision:  ociRevision(ref, digest),
		Digest:    digest,
		Size:      size,
	}
}

// checkCacheHit applies the cache-hit gauntlet to a populated slot:
// (1) require a well-formed cached digest, (2) reject leftover OCI
// Image Layout artifacts, (3) re-verify cosign when configured (but
// skip the re-verify when the persisted verify marker proves the
// cached digest was checked under the same spec.verify policy —
// closes the "offline tool that requires online" gap on flate's hot
// path).
//
// Returns (artifact, true, nil) on a confirmed hit; (nil, false, nil)
// when the slot should be reset and re-pulled; (nil, false, err) on
// a fatal failure (e.g. cosign rejected the cached bytes).
func (f *Fetcher) checkCacheHit(ctx context.Context, repoClient *remote.Repository, repo *manifest.OCIRepository, slotPath string, ref manifest.OCIRepositoryRef, versioned string) (*store.SourceArtifact, bool, error) {
	cachedDigest := readCachedDigest(slotPath)
	if cachedDigest == "" {
		// `.flate-digest` is written as the FINAL step of a successful
		// fetch (and the slot is committed via atomic rename only after
		// that write), so its absence on a final slot means the slot
		// was committed from a pre-marker flate version or someone
		// hand-modified the cache.
		return nil, false, nil
	}
	if hasUnfinishedOCILayout(slotPath) {
		// Defensive: a valid `.flate-digest` should imply
		// applyLayerSelector ran to completion and wiped the OCI
		// Image Layout artifacts. Atomic-rename makes this much less
		// likely (a crashed run never publishes a final slot), but
		// legacy slots from older flate versions or hand-modifications
		// can still trip this. Reset so the next pull rebuilds the
		// slot cleanly.
		slog.Warn("oci: cache slot has leftover OCI Image Layout artifacts; resetting and re-fetching",
			"slot", slotPath, "url", versioned)
		return nil, false, nil
	}
	if repo.Verify != nil {
		want := verifyFingerprint(repo.Verify)
		if want != readVerifyMarker(slotPath) {
			// Verify policy changed since the slot was populated (or
			// the marker is missing) — re-fetch the signature
			// material and validate. This is the only path that
			// hits the registry on a cache hit; with a stable verify
			// policy and intact marker the cache hit is fully offline.
			//
			// Keyless verify (SecretRef==nil) intentionally leaves the
			// marker absent (see the post-pull write site), so cache
			// hits ALWAYS land here and verifyCosignSignature re-emits
			// its WARN — surface the unverified-render status on every
			// reconcile rather than once-per-process.
			if err := f.verifyCosignSignature(ctx, repoClient, repo, cachedDigest); err != nil {
				return nil, false, err
			}
			// Persist the new fingerprint so subsequent hits skip the
			// network — but only for keyed verification. Keyless skips
			// the marker for the WARN-re-fire reason above.
			if repo.Verify.SecretRef != nil {
				if err := writeVerifyMarker(slotPath, want); err != nil {
					slog.Warn("oci: failed to persist verify marker after re-verify; future hits will re-verify online",
						"slot", slotPath, "err", err)
				}
			}
		}
	}
	return ociArtifact(repo, slotPath, ref, cachedDigest, 0), true, nil
}

// cachedDigestFile is the slot-relative path where flate records the
// resolved digest of an OCIRepository pull. Used to re-verify on cache
// hit when spec.verify is configured.
const cachedDigestFile = ".flate-digest"

// verifyMarkerFile records the cosign verify-policy fingerprint that
// the slot's cached digest was last validated against. When the
// current spec.verify hashes to the same value, the cache-hit path
// can skip the re-verify roundtrip — restoring flate's offline
// promise for sources with verify configured. A missing or
// mismatched marker forces re-verify (covering policy changes,
// pre-marker slots, and tampered caches).
const verifyMarkerFile = ".flate-verified"

// verifyFingerprint hashes the verify spec into a short identifier
// stored next to the cached digest. Any meaningful change to the
// verify policy (provider, MatchOIDCIdentity, SecretRef) produces a
// different fingerprint and forces re-verify. JSON-marshal the spec
// for a deterministic representation — the upstream Verify struct
// has stable JSON tags.
func verifyFingerprint(v *manifest.OCIRepositoryVerify) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		// Marshal-of-typed-struct shouldn't fail; if it does we
		// fingerprint as empty which forces re-verify (safe default).
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

// writeVerifyMarker persists the verify-policy fingerprint to the
// slot atomically. atomic.WriteFile handles the temp-file + fsync +
// rename dance so a crash mid-write can't leave a partial fingerprint
// that would falsely match.
func writeVerifyMarker(slot, fingerprint string) error {
	return atomic.WriteFile(filepath.Join(slot, verifyMarkerFile), []byte(fingerprint), 0o600, true)
}

// readVerifyMarker returns the cached fingerprint or "" when the
// marker is missing / unreadable. Empty matches the fingerprint of a
// nil Verify; a different non-empty value forces re-verify.
func readVerifyMarker(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, verifyMarkerFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// digestRE matches a well-formed OCI content digest:
// "<algorithm>:<hex>" where the hex side is at least 32 chars (sha256
// truncated, the OCI spec minimum). Catches torn writes where the
// previous run died mid-WriteFile and left a partial digest string —
// rather than passing the partial to cosign and getting a misleading
// "signature not found" error, treat it as a missing marker.
var digestRE = regexp.MustCompile(`^[a-z0-9]+:[a-fA-F0-9]{32,}$`)

// writeCachedDigest persists digest atomically via atomic.WriteFile.
// Without atomicity, a crash mid-write could leave a partial digest
// string that would later mis-read on cache hit and trigger a
// misleading cosign failure on the next reconcile.
func writeCachedDigest(slot, digest string) error {
	return atomic.WriteFile(filepath.Join(slot, cachedDigestFile), []byte(digest), 0o600, true)
}

// readCachedDigest returns the cached digest only when it parses as a
// well-formed OCI content digest. Empty + malformed both return "" so
// the caller's "no marker" branch handles the recovery uniformly.
func readCachedDigest(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, cachedDigestFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	s := strings.TrimSpace(string(b))
	if !digestRE.MatchString(s) {
		return ""
	}
	return s
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

// authIdentity returns the cache-key auth tag for an OCIRepository.
// Combines SecretRef (registry creds), CertSecretRef (TLS material),
// ProxySecretRef, and verify.SecretRef (cosign keys) since each can
// change which artifact bytes a reconcile resolves to.
func authIdentity(repo *manifest.OCIRepository) string {
	var secret, cert, proxy, verify string
	if repo.SecretRef != nil {
		secret = source.SecretRefID(repo.Namespace, repo.SecretRef.Name)
	}
	if repo.CertSecretRef != nil {
		cert = source.SecretRefID(repo.Namespace, repo.CertSecretRef.Name)
	}
	if repo.ProxySecretRef != nil {
		proxy = source.SecretRefID(repo.Namespace, repo.ProxySecretRef.Name)
	}
	if repo.Verify != nil && repo.Verify.SecretRef != nil {
		verify = source.SecretRefID(repo.Namespace, repo.Verify.SecretRef.Name)
	}
	return source.AuthIdentity(secret, cert, proxy, verify)
}
