package loader

import (
	"context"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ResolveRoot confines traversal to a valid captured source artifact.
//
//nolint:gocritic // The result mirrors physical root acquisition: coordinate, availability and I/O failure.
func (l *Loader) ResolveRoot(
	ctx context.Context,
	reads *store.ReadSet,
	source manifest.NamedResource,
	root string,
) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err //nolint:wrapcheck // Preserve the caller cancellation identity.
	}
	if !reads.Valid() {
		return "", false, nil
	}
	resolved, valid := resolveRoot(reads, source, root)
	if err := ctx.Err(); err != nil {
		return "", false, err //nolint:wrapcheck // Preserve the caller cancellation identity.
	}
	if !reads.Valid() {
		return "", false, nil
	}
	return resolved, valid, nil
}

type sourcePathView struct {
	fs.FS

	valid, missing bool
}

func (v *sourcePathView) Lstat(name string) (fs.FileInfo, error) {
	info, err := fs.Lstat(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			v.missing = true
		} else {
			v.valid = false
		}
	}
	//nolint:wrapcheck // SecureJoinVFS requires the original filesystem error and path contract.
	return info, err
}

func (v *sourcePathView) Readlink(name string) (string, error) {
	target, err := fs.ReadLink(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	resolved := path.Join(path.Dir(strings.TrimLeft(name, "/")), target)
	if err != nil || path.IsAbs(target) || resolved == ".." || strings.HasPrefix(resolved, "../") {
		v.valid = false
	}
	//nolint:wrapcheck // SecureJoinVFS requires the original filesystem error and path contract.
	return target, err
}

func resolveRoot(objects sourceStore, source manifest.NamedResource, root string) (string, bool) {
	art, _ := objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if art == nil || art.FS == nil {
		return "", false
	}
	view := sourcePathView{FS: art.FS}
	resolved, valid := view.resolve(root)
	if !valid || view.missing {
		return "", false
	}
	info, err := fs.Stat(art.FS, resolved)
	return resolved, err == nil && info.IsDir()
}

func (v *sourcePathView) resolve(name string) (string, bool) {
	v.valid, v.missing = true, false
	if name == "" || path.IsAbs(name) || strings.Contains(name, "://") {
		return "", false
	}
	clean := path.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", false
	}
	resolved, err := securejoin.SecureJoinVFS("/", name, v)
	if err != nil || !v.valid {
		return "", false
	}
	resolved = strings.TrimPrefix(filepath.ToSlash(resolved), "/")
	if resolved == "" {
		resolved = "."
	}
	return resolved, true
}
