package baseline

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// AdmitDirectory resolves disk coordinates before constructing a direct reader.
// Synthetic labels belong to already admitted inputs and MUST bypass this path.
func AdmitDirectory(ctx context.Context, path, root string) (*store.SourceArtifact, string, error) {
	location, err := gitrepo.Locate(ctx, path, root)
	if err != nil {
		return nil, "", err
	}
	input, err := admitLocation(ctx, location)
	if err != nil {
		return nil, "", err
	}
	return input, location.ScanPath, nil
}

func admitLocation(ctx context.Context, location *gitrepo.Location) (*store.SourceArtifact, error) {
	if !safepath.Contains(location.ContentRoot, location.ScanPath) {
		return nil, fmt.Errorf("%w: --path %q is outside the source root at %q", manifest.ErrInput, location.ScanPath, location.ContentRoot)
	}
	input, err := gittree.AdmitDirectory(ctx, location)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: --path %q: %w", manifest.ErrInput, location.ScanPath, err)
	}
	if _, err := os.Stat(location.ScanPath); err != nil {
		return nil, fmt.Errorf("%w: --path %q: %w", manifest.ErrInput, location.ScanPath, err)
	}
	return input, nil
}
