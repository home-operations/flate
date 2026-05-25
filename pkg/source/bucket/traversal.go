package bucket

import (
	"fmt"
	"path/filepath"
	"strings"
)

// safeJoinUnderSlot validates and joins a bucket-relative key into the
// cache slot. A malicious bucket — either deliberately or via stale
// curation — can produce a key whose filepath.FromSlash form contains
// `..` enough times to climb past slot; filepath.Join happily cleans
// the climb-out without complaint. Without this guard such a key would
// write arbitrary files on the host. Mirrors the safeJoinTarPath
// protection in pkg/source/oci/layer.go (modulo the absolute-path
// rejection — bucket keys aren't tar headers, so an absolute-looking
// key stays under slot by virtue of filepath.Join's component-boundary
// handling, which the traversal_test asserts).
func safeJoinUnderSlot(slot, rel string) (string, error) {
	joined := filepath.Join(slot, filepath.FromSlash(rel))
	cleanSlot := filepath.Clean(slot)
	relInside, err := filepath.Rel(cleanSlot, joined)
	if err != nil {
		return "", fmt.Errorf("path resolution: %w", err)
	}
	if relInside == ".." || strings.HasPrefix(relInside, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal: %q escapes cache slot", rel)
	}
	return joined, nil
}
