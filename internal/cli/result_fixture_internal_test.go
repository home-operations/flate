package cli

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/engine/orchestrator"
	"github.com/home-operations/flate/internal/testutil/assert"
)

//nolint:gocritic // The fixture MUST match the SDK configuration value boundary.
func renderTreesForTest(
	ctx context.Context,
	tb testing.TB,
	trees []orchestrator.Tree,
	cfg orchestrator.Config,
) ([]orchestrator.Rendered, error) {
	tb.Helper()
	invocation, err := orchestrator.RenderTrees(ctx, trees, cfg)
	if invocation == nil {
		return nil, err
	}
	tb.Cleanup(func() { assert.NoError(tb, invocation.Close()) })
	return invocation.Results, err
}
