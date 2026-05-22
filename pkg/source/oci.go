package source

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"
	"oras.land/oras-go/v2"
	orasfile "oras.land/oras-go/v2/content/file"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// FetchOCI pulls the OCIRepository artifact into cache. Credentials are
// read from a docker-style config.json honored by oras-go's
// credentials.NewFileStore. When spec.ref.semver is set, the registry
// is listed and the highest matching tag (filtered by semverFilter, if
// any) is resolved before pulling.
func FetchOCI(ctx context.Context, cache *Cache, repo *manifest.OCIRepository, registryConfig string) (*store.OCIArtifact, error) {
	if repo == nil {
		return nil, errors.New("oci repository is nil")
	}
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
	if credStore != nil {
		repoClient.Client = &auth.Client{Credential: credentials.Credential(credStore)}
	}

	// Resolve spec.ref into a concrete (tag-or-digest) BEFORE choosing
	// the cache slot, so different semver matches don't share a slot.
	ref := repo.Ref
	if ref.Semver != "" {
		resolved, err := resolveOCISemver(ctx, repoClient, ref.Semver, ref.SemverFilter)
		if err != nil {
			return nil, fmt.Errorf("OCIRepository %s semver: %w", repo.RepoName(), err)
		}
		ref = manifest.OCIRepositoryRef{Tag: resolved}
	}

	versioned := versionedURL(repo.URL, ref)
	slot, exists, err := cache.Slot(versioned, "")
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", versioned, err)
	}
	if exists {
		return &store.OCIArtifact{URL: repo.URL, LocalPath: slot, Ref: ref, Digest: ref.Digest}, nil
	}

	tag := versionTag(ref)
	if tag == "" {
		tag = "latest"
	}

	dest, err := orasfile.New(slot)
	if err != nil {
		return nil, fmt.Errorf("oras file store: %w", err)
	}
	defer func() { _ = dest.Close() }()

	desc, err := oras.Copy(ctx, repoClient, tag, dest, tag, oras.DefaultCopyOptions)
	if err != nil {
		_ = os.RemoveAll(slot)
		return nil, fmt.Errorf("oras copy %s: %w", versioned, err)
	}

	return &store.OCIArtifact{URL: repo.URL, LocalPath: slot, Ref: ref, Digest: desc.Digest.String()}, nil
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
	err = repoClient.Tags(ctx, "", func(tags []string) error {
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
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("list tags: %w", err)
	}
	if len(matching) == 0 {
		return "", fmt.Errorf("no tag matched semver %q (filter %q)", expr, filterPattern)
	}
	// Highest match wins.
	idx := make([]int, len(matching))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return matching[idx[a]].LessThan(matching[idx[b]]) })
	return matchingTags[idx[len(idx)-1]], nil
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
	case ref.Semver != "":
		return ref.Semver
	}
	return ""
}
