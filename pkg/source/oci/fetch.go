package oci

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"

	"oras.land/oras-go/v2"
	orasoci "oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// fetch pulls the OCIRepository artifact into cache. It reads as a pipeline:
// build the registry client, resolve spec.ref to a concrete digest (honoring
// the tag→digest resolve cache), acquire the artifact slot and try a cache
// hit, otherwise oras.Copy the artifact and publish it (layer-select, ignore,
// marker, commit). Credentials come from a docker-style config.json; when
// spec.ref.semver is set the registry is listed and the highest matching tag
// resolved before pulling. flate does not verify signatures, so spec.verify is
// ignored and the artifact is pulled unconditionally.
func fetch(ctx context.Context, f *Fetcher, repo *manifest.OCIRepository, registryConfig string, tlsCfg *tls.Config, proxy *source.ProxyConfig) (*store.SourceArtifact, error) {
	cache := f.Cache
	// Fetch already type-asserts repo non-nil before calling fetch().
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: %s missing url", manifest.ErrInput, ociID(repo))
	}
	repoClient, err := newRepoClient(repo, registryConfig, tlsCfg, proxy)
	if err != nil {
		return nil, err
	}

	// Resolve spec.ref to a concrete (tag-or-digest) BEFORE choosing the cache
	// slot, so different semver matches never share a slot. Flux precedence is
	// digest > semver > tag.
	var ref manifest.OCIRepositoryRef
	if repo.Reference != nil {
		ref = *repo.Reference
	}
	authID := authIdentity(repo)

	resolveSlot, resolvedDigest, err := lookupResolveCache(ctx, cache, repo, ref, authID)
	if err != nil {
		return nil, err
	}
	if resolveSlot != nil {
		defer resolveSlot.Release()
	}
	if ref, err = resolveRef(ctx, repoClient, repo, ref); err != nil {
		return nil, err
	}
	tag := cmp.Or(versionTag(ref), latestTag)
	if resolvedDigest == "" {
		if resolvedDigest, err = resolveOCIRefDigest(ctx, repoClient, ref, tag); err != nil {
			return nil, fmt.Errorf("%s resolve %s: %w", ociID(repo), tag, err)
		}
		if err := writeResolveCache(resolveSlot, resolvedDigest); err != nil {
			return nil, err
		}
	}
	if resolveSlot != nil {
		resolveSlot.Release()
	}

	versioned := versionedURL(repo.URL, ref)
	slotRef := ociCacheKey(repo, ref, resolvedDigest)
	if resolvedDigest == "" {
		slotRef = source.MutableCacheKey(slotRef)
	}
	slot, err := cache.Slot(ctx, repo.URL, slotRef, authID)
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", versioned, err)
	}
	defer slot.Release()
	if slot.Exists {
		if resolvedDigest != "" {
			if artifact, hit := checkCacheHit(repo, slot.Path, ref, versioned, resolvedDigest); hit {
				return artifact, nil
			}
		}
		// Stale or unresolved slot — wipe and stage a fresh pull target.
		if err := slot.Refresh(); err != nil {
			return nil, fmt.Errorf("cache refresh for %s: %w", versioned, err)
		}
	}

	digest, size, err := copyArtifact(ctx, repoClient, slot.Path, tag, resolvedDigest, versioned)
	if err != nil {
		return nil, err
	}
	if resolvedDigest != "" && digest != resolvedDigest {
		return nil, fmt.Errorf("%s: resolved digest %s but copied %s", ociID(repo), resolvedDigest, digest)
	}
	return publishArtifact(repo, slot, ref, digest, size)
}

// resolveRef resolves a semver ref to a concrete tag by listing the registry;
// digest and plain-tag refs pass through unchanged.
func resolveRef(ctx context.Context, repoClient *remote.Repository, repo *manifest.OCIRepository, ref manifest.OCIRepositoryRef) (manifest.OCIRepositoryRef, error) {
	if !shouldResolveOCISemver(ref) {
		return ref, nil
	}
	resolved, err := resolveOCISemver(ctx, repoClient, ref.SemVer, ref.SemverFilter, layerMediaType(repo.LayerSelector))
	if err != nil {
		return ref, fmt.Errorf("%s semver: %w", ociID(repo), err)
	}
	return manifest.OCIRepositoryRef{Tag: resolved}, nil
}

// copyArtifact runs oras.Copy of the (tag or resolved-digest) reference into an
// OCI Image Layout content store rooted at slotPath, returning the pulled
// digest and size.
//
// slotPath is the staging dir at this point; on success the caller's
// slot.Commit() atomic-renames it over the final slot. Any error path returns
// without committing and Release wipes the staging dir — the final slot stays
// absent / unchanged, never torn. Blobs land at slot/blobs/<algo>/<hex> per the
// OCI Image Layout spec; publishArtifact's applyLayerSelector reads from there
// and wipes the layout after extracting the selected layer.
func copyArtifact(ctx context.Context, repoClient *remote.Repository, slotPath, tag, resolvedDigest, versioned string) (digest string, size int64, err error) {
	dest, err := orasoci.New(slotPath)
	if err != nil {
		return "", 0, fmt.Errorf("oras oci store: %w", err)
	}
	copyRef := cmp.Or(resolvedDigest, tag)
	desc, err := oras.Copy(ctx, repoClient, copyRef, dest, tag, oras.DefaultCopyOptions)
	if err != nil {
		return "", 0, fmt.Errorf("oras copy %s: %w", versioned, err)
	}
	return desc.Digest.String(), desc.Size, nil
}

// publishArtifact finalizes a freshly pulled slot: select/extract the layer,
// apply source-ignore, persist the digest marker, and commit. It is the last
// leg of the pull and owns slot.Commit(). flate does not verify signatures, so
// spec.verify is ignored.
func publishArtifact(repo *manifest.OCIRepository, slot *source.Slot, ref manifest.OCIRepositoryRef, digest string, size int64) (*store.SourceArtifact, error) {
	if err := applyLayerSelector(slot.Path, digest, repo.LayerSelector); err != nil {
		return nil, fmt.Errorf("%s: layer select: %w", ociID(repo), err)
	}
	// Source-controller's default ignore set includes `*.tar.gz`. For
	// operation=copy the artifact IS the .tar.gz we just produced at
	// slot/<copiedLayerFilename>, so ApplyIgnore would delete it — skip it.
	// For operation=extract the slot holds the extracted tree and the ignore
	// semantics apply as Flux ships them.
	if effectiveLayerOperation(repo.LayerSelector) == manifest.OCILayerOperationExtract {
		if err := source.ApplyIgnore(slot.Path, repo.Ignore); err != nil {
			return nil, fmt.Errorf("%s: %w", ociID(repo), err)
		}
	}
	// Write the digest marker before Commit. A failure here is fatal: without
	// it the next fetch sees a committed slot with "no marker" and
	// resets+re-pulls every reconcile. Returning (skipping Commit) wipes the
	// staging dir via Release so the next run starts clean.
	if err := writeCachedDigest(slot.Path, digest); err != nil {
		return nil, fmt.Errorf("%s: persist cached digest: %w", ociID(repo), err)
	}
	if err := slot.Commit(); err != nil {
		return nil, fmt.Errorf("%s: commit slot: %w", ociID(repo), err)
	}
	return ociArtifact(repo, slot.Path, ref, digest, size), nil
}
