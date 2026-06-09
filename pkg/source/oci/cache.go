package oci

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"

	"oras.land/oras-go/v2/registry/remote"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// resolveCachePrefix namespaces the tag→digest resolve-cache slot so it never
// collides with the artifact slot for the same key.
const resolveCachePrefix = "resolve:"

// cacheOptsSep joins the human-readable ref with the hashed render options in a
// slot key (ref#opts:<hash>), so a layerSelector/ignore change picks a fresh
// slot without losing the at-a-glance ref.
const cacheOptsSep = "#opts:"

// ociArtifact is the single SourceArtifact-construction helper used by both the
// cache-hit and successful-pull paths. Lifting the literal out keeps the two
// paths from drifting (the pre-helper code dropped Size on the cache-hit path),
// and a future field addition only touches one site.
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

// ociResolveCacheKey keys the tag→digest resolve cache (a tiny slot holding
// just the resolved digest in its meta sidecar, no artifact).
func ociResolveCacheKey(repo *manifest.OCIRepository, ref manifest.OCIRepositoryRef) string {
	return resolveCachePrefix + ociCacheKey(repo, ref, "")
}

// ociCacheKey is the artifact slot key: the concrete ref (resolved digest, or
// tag, or "latest") plus a short hash of every input that changes the produced
// bytes — layer media type, layer operation, and the ignore pattern.
func ociCacheKey(repo *manifest.OCIRepository, ref manifest.OCIRepositoryRef, resolvedDigest string) string {
	var ignore string
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	payload := struct {
		Ref            string `json:"ref"`
		LayerMediaType string `json:"layerMediaType,omitempty"`
		LayerOperation string `json:"layerOperation,omitempty"`
		Ignore         string `json:"ignore,omitempty"`
	}{
		Ref:            cmp.Or(resolvedDigest, versionTag(ref), latestTag),
		LayerMediaType: layerMediaType(repo.LayerSelector),
		LayerOperation: effectiveLayerOperation(repo.LayerSelector),
		Ignore:         ignore,
	}
	h, _ := source.CacheKeyHash(payload, 8)
	return payload.Ref + cacheOptsSep + h
}

// lookupResolveCache returns the resolve-cache slot and the digest it holds (if
// fresh) for a mutable tag-only ref. Digest/semver refs don't use the resolve
// cache (digest is already concrete; semver must re-list to honor moving
// constraints) and return (nil, "").
func lookupResolveCache(ctx context.Context, cache *source.Cache, repo *manifest.OCIRepository, ref manifest.OCIRepositoryRef, authID string) (*source.Slot, string, error) {
	if ref.Digest != "" || ref.SemVer != "" {
		return nil, "", nil
	}
	slot, err := cache.Slot(ctx, repo.URL, ociResolveCacheKey(repo, ref), authID)
	if err != nil {
		return nil, "", fmt.Errorf("cache resolve slot for %s: %w", repo.URL, err)
	}
	if slot.Exists {
		if digest, ok := cachedDigestFresh(slot.Path, repo.Interval.Duration); ok {
			return slot, digest, nil
		}
	}
	return slot, "", nil
}

// writeResolveCache records a freshly resolved digest in the resolve-cache
// slot. No-op when there's no slot (digest/semver ref) or nothing to record.
func writeResolveCache(slot *source.Slot, digest string) error {
	if slot == nil || digest == "" {
		return nil
	}
	return slot.PersistMeta(func(m *source.SlotMeta) { m.Digest = digest })
}

// checkCacheHit applies the cache-hit gauntlet to a populated slot:
// (1) require a well-formed cached digest, (2) reject leftover OCI Image Layout
// artifacts, (3) re-verify cosign when configured (but skip the re-verify when
// the persisted verify marker proves the cached digest was checked under the
// same spec.verify policy — closes the "offline tool that requires online" gap
// on flate's hot path).
//
// Returns (artifact, true, nil) on a confirmed hit; (nil, false, nil) when the
// slot should be reset and re-pulled; (nil, false, err) on a fatal failure
// (e.g. cosign rejected the cached bytes).
func (f *Fetcher) checkCacheHit(ctx context.Context, repoClient *remote.Repository, repo *manifest.OCIRepository, slotPath string, ref manifest.OCIRepositoryRef, versioned, expectedDigest string) (*store.SourceArtifact, bool, error) {
	cachedDigest := readCachedDigest(slotPath)
	if cachedDigest == "" {
		// The cached digest is recorded in the meta sidecar as the FINAL
		// step of a successful fetch (and the slot is committed via atomic
		// rename only after that write), so its absence on a final slot
		// means the slot was committed from a pre-marker flate version or
		// someone hand-modified the cache.
		return nil, false, nil
	}
	if hasUnfinishedOCILayout(slotPath) {
		// Defensive: a valid cached digest should imply applyLayerSelector
		// ran to completion and wiped the OCI Image Layout artifacts.
		// Atomic-rename makes this much less likely (a crashed run never
		// publishes a final slot), but legacy slots from older flate versions
		// or hand-modifications can still trip this. Reset so the next pull
		// rebuilds the slot cleanly.
		slog.Warn("oci: cache slot has leftover OCI Image Layout artifacts; resetting and re-fetching",
			"slot", slotPath, "url", versioned)
		return nil, false, nil
	}
	if expectedDigest != "" && cachedDigest != expectedDigest {
		return nil, false, nil
	}
	if repo.Verify != nil {
		want := verifyFingerprint(repo.Verify)
		if want != readVerifyMarker(slotPath) {
			// Verify policy changed since the slot was populated (or the
			// marker is missing) — re-fetch the signature material and
			// validate. This is the only path that hits the registry on a
			// cache hit; with a stable verify policy and intact marker the
			// cache hit is fully offline.
			//
			// A skipped verify (keyless, wiped/absent key, or unreachable
			// signature) leaves the marker absent (see the post-pull write
			// site), so cache hits ALWAYS land here and verifyCosignSignature
			// re-emits its WARN — surfacing the unverified-render status on
			// every reconcile rather than once-per-process.
			verified, err := f.verifyCosignSignature(ctx, repoClient, repo, cachedDigest)
			if err != nil {
				return nil, false, err
			}
			// Persist the new fingerprint so subsequent hits skip the network
			// — but only when verification actually succeeded. A skip leaves
			// the marker absent for the WARN-re-fire reason above.
			if verified {
				if err := writeVerifyMarker(slotPath, want); err != nil {
					slog.Warn("oci: failed to persist verify marker after re-verify; future hits will re-verify online",
						"slot", slotPath, "err", err)
				}
			}
		}
	}
	return ociArtifact(repo, slotPath, ref, cachedDigest, 0), true, nil
}
