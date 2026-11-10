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
func AdmitDirectory(
	ctx context.Context,
	path, root string,
	scope manifest.WorkScope,
) (
	_ *store.SourceArtifact,
	_ string,
	resultErr error,
) {
	work := scope.Record(manifest.WorkPreparation, path)
	work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	defer func() { work.Finish(resultErr) }()
	location, err := gitrepo.Locate(ctx, path, root)
	if err != nil {
		return nil, "", err
	}
	directory, err := admitLocation(ctx, location, scope)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = directory.Close() }()
	return directory.Artifact(), location.ScanPath, nil
}

func admitLocation(
	ctx context.Context,
	location *gitrepo.Location,
	scope manifest.WorkScope,
) (*gittree.Directory, error) {
	if !safepath.Contains(location.ContentRoot, location.ScanPath) {
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("--path %q is outside the source root at %q", location.ScanPath, location.ContentRoot),
		)
	}
	work := scope.Record(manifest.WorkAcquisition, location.ContentRoot)
	work.Add(&manifest.WorkCounts{Attempts: 1, LocalAcquisitions: 1})
	directory, err := gittree.OpenDirectory(ctx, location)
	work.Finish(err)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("--path %q: %w", location.ScanPath, err), "")
	}
	if _, err := os.Stat(location.ScanPath); err != nil {
		_ = directory.Close()
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("--path %q: %w", location.ScanPath, err), "")
	}
	return directory, nil
}
