package cli

import (
	"context"
	"testing"

	"github.com/home-operations/flate/pkg/orchestrator"
)

//nolint:gocritic // The fixture MUST match the SDK configuration value boundary.
func renderTreesForTest(
	ctx context.Context,
	tb testing.TB,
	trees []orchestrator.Tree,
	cfg orchestrator.Config,
) ([]orchestrator.Rendered, error) {
	tb.Helper()
	runs, err := orchestrator.RenderTrees(ctx, trees, cfg)
	for _, run := range runs {
		tb.Cleanup(run.Result.Close)
	}
	return runs, err
}
