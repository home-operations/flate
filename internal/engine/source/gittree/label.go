package gittree

import (
	"path/filepath"

	"github.com/home-operations/flate/pkg/manifest"
)

// LabelOptions supplies ownership coordinates independently of acquisition identity.
type LabelOptions struct {
	Root, Key string
	Mode      ViewMode
	Source    manifest.NamedResource
	Content   any
}

// Label returns an ownership coordinate and its key without accessing the filesystem.
//
//nolint:gocritic // The SDK accepts a value snapshot; a pointer parameter would change its call and aliasing contracts.
func Label(opts LabelOptions) (string, string) {
	key := opts.Key
	if opts.Content != nil {
		key = manifest.Fingerprint(opts.Content)
	}
	if opts.Mode == Direct {
		return filepath.Join(opts.Root, ".flate-base", key), key
	}
	const sourceDir = ".flate-sources"
	parts := [...]string{filepath.Dir(opts.Root), sourceDir, key, opts.Source.Namespace, opts.Source.Name}
	if parts[0] == string(filepath.Separator) {
		// Avoid allocating a cleanup buffer for the doubled separator after the root.
		parts[0], parts[1] = "", string(filepath.Separator)+sourceDir
	}
	return filepath.Join(parts[:]...), key
}
