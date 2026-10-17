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
func Label(opts LabelOptions) (string, string) {
	key := opts.Key
	if opts.Content != nil {
		key = manifest.Fingerprint(opts.Content)
	}
	if opts.Mode == Direct {
		return filepath.Join(opts.Root, ".flate-base", key), key
	}
	return filepath.Join(filepath.Dir(opts.Root), ".flate-sources", key, opts.Source.Namespace, opts.Source.Name), key
}
