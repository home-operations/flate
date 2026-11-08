package baseline

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/home-operations/flate/internal/source/gitrepo"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/source/safepath"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
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
	input, err := admitLocation(ctx, location, scope)
	if err != nil {
		return nil, "", err
	}
	return input, location.ScanPath, nil
}

func admitLocation(
	ctx context.Context,
	location *gitrepo.Location,
	scope manifest.WorkScope,
) (*store.SourceArtifact, error) {
	if !safepath.Contains(location.ContentRoot, location.ScanPath) {
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("--path %q is outside the source root at %q", location.ScanPath, location.ContentRoot),
		)
	}
	work := scope.Record(manifest.WorkAcquisition, location.ContentRoot)
	work.Add(&manifest.WorkCounts{Attempts: 1, LocalAcquisitions: 1})
	input, err := gittree.AdmitDirectory(ctx, location)
	work.Finish(err)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("--path %q: %w", location.ScanPath, err), "")
	}
	if _, err := os.Stat(location.ScanPath); err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("--path %q: %w", location.ScanPath, err), "")
	}
	return input, nil
}
