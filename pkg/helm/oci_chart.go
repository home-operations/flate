package helm

import (
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/home-operations/flate/pkg/manifest"
)

const chartDigestPrefixLength = 12

// ociChartVersion matches Flux's source revision semantics, replacing build
// metadata while preserving the chart's prerelease. OCI tags encode '+' as '_'.
func ociChartVersion(version, revision string) (string, error) {
	ver, err := semver.NewVersion(version)
	if err != nil {
		return "", fmt.Errorf("%w: invalid chart version %q: %w", manifest.ErrInput, version, err)
	}
	tag, suffix, err := splitOCIRevision(revision)
	if err != nil {
		return "", err
	}
	if tag != nil && !tag.Equal(ver) {
		return "", fmt.Errorf("%w: artifact revision %q does not match chart version %q", manifest.ErrInput, revision, version)
	}
	tracked, err := ver.SetMetadata(suffix)
	if err != nil {
		return "", fmt.Errorf("%w: invalid artifact revision %q: %w", manifest.ErrInput, revision, err)
	}
	return tracked.String(), nil
}

func splitOCIRevision(revision string) (*semver.Version, string, error) {
	tag, digest, tagged := strings.Cut(revision, "@")
	candidate := revision
	if tagged {
		candidate = strings.ReplaceAll(tag, "_", "+")
	}
	tagVer, err := semver.NewVersion(candidate)
	if err != nil {
		// Flux extracts from the whole revision for digest pins and non-semver tags.
		tagVer = nil
		digest = revision
	} else if !tagged || strings.Contains(digest, "@") {
		return nil, "", fmt.Errorf("%w: invalid artifact revision %q", manifest.ErrInput, revision)
	}
	_, suffix, ok := strings.Cut(digest, ":")
	if !ok || strings.Contains(suffix, ":") || len(suffix) < chartDigestPrefixLength {
		return nil, "", fmt.Errorf("%w: invalid artifact revision %q", manifest.ErrInput, revision)
	}
	return tagVer, suffix[:chartDigestPrefixLength], nil
}
