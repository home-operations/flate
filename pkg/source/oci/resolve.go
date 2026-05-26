package oci

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/Masterminds/semver/v3"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/home-operations/flate/pkg/manifest"
)

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

// versionTag returns the per-reference tag oras.Copy should target.
// Digest wins over Tag wins over SemVer (after resolution); empty when
// the caller wants the registry's default ("latest" downstream).
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
