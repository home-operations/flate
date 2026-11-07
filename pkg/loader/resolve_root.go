package loader

import (
	"context"
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

type admissionRootView struct {
	fs.FS

	valid bool
}

func (v *admissionRootView) Lstat(name string) (fs.FileInfo, error) {
	info, err := fs.Lstat(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	if err != nil {
		v.valid = false
	}
	//nolint:wrapcheck // SecureJoinVFS requires the original filesystem error and path contract.
	return info, err
}

func (v *admissionRootView) Readlink(name string) (string, error) {
	target, err := fs.ReadLink(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	resolved := path.Join(path.Dir(strings.TrimLeft(name, "/")), target)
	if err != nil || path.IsAbs(target) || resolved == ".." || strings.HasPrefix(resolved, "../") {
		v.valid = false
	}
	//nolint:wrapcheck // SecureJoinVFS requires the original filesystem error and path contract.
	return target, err
}

func resolveRoot(objects sourceStore, source manifest.NamedResource, root string) (string, bool) {
	if root == "" || path.IsAbs(root) || strings.Contains(root, "://") || strings.HasPrefix(path.Clean(root), "../") ||
		path.Clean(root) == ".." {
		return "", false
	}
	art, _ := objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if art == nil || art.FS == nil {
		return "", false
	}
	view := &admissionRootView{FS: art.FS, valid: true}
	resolved, err := securejoin.SecureJoinVFS("/", root, view)
	if err != nil || !view.valid {
		return "", false
	}
	resolved = strings.TrimPrefix(filepath.ToSlash(resolved), "/")
	if resolved == "" {
		resolved = "."
	}
	info, err := fs.Stat(art.FS, resolved)
	return resolved, err == nil && info.IsDir()
}
