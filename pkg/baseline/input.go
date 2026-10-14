package baseline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// AdmitDirectory resolves disk coordinates before constructing a direct reader.
// Synthetic labels belong to already admitted inputs and MUST bypass this path.
func AdmitDirectory(ctx context.Context, path, root string) (*store.SourceArtifact, string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, "", fmt.Errorf("resolve input %q: %w", path, err)
	}
	switch root {
	case "":
		root, _, _ = gitrepo.Locate(abs)
		root = cmp.Or(root, abs)
	case path:
		root = abs
	default:
		root, err = filepath.Abs(root)
		if err != nil {
			return nil, "", err
		}
		if resolved, err := filepath.EvalSymlinks(root); err == nil {
			root = resolved
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, "", fmt.Errorf("resolve input root %q: %w", root, err)
		}
	}
	input, err := gittree.AdmitDirectory(ctx, root)
	if err != nil {
		return nil, "", fmt.Errorf("--path %q: %w", path, err)
	}
	if _, err := os.Stat(abs); err != nil {
		return nil, "", fmt.Errorf("--path %q: %w", path, err)
	}
	return input, abs, nil
}
