package oci

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

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
