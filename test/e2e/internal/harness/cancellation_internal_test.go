package harness

import (
	"context"
	"log"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/cli"
	"github.com/home-operations/flate/internal/testutil"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
)

// cli.Run and SIGTERM MUST remain serial because they change process state.
func TestRunCLI_CancellationJoins(t *testing.T) {
	t.Setenv("FLATE_CACHE_DIR", t.TempDir())
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [source.yaml, app.yaml]\n")
	testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: remote, namespace: apps}
spec: {url: git://cancellation.invalid/repo.git, ref: {branch: main}}
`)
	testutil.WriteFile(t, root, "flux/app.yaml", testutil.KustomizationYAML("remote", "apps",
		"  path: ./work\n  sourceRef: {kind: GitRepository, name: remote}\n"))
	entered := make(chan context.Context, 1)
	routes := sourcegit.Transports{
		"git": func(ctx context.Context, _ sourcegit.TransportRequest) (sourcegit.Connection, error) {
			entered <- ctx
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	previous, writer := slog.Default(), log.Writer()
	defer func() { slog.SetDefault(previous); log.SetOutput(writer) }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-entered:
			cancel()
		case <-ctx.Done():
		}
	}()
	result, canceled := runCLI(ctx, t, []string{
		"build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--no-progress",
	}, cli.RunConfig{GitTransports: routes})
	cancel()
	<-joined
	assert.Equal(t, canceled, true)
	assert.Equal(t, result.Code, 1)
	assert.Equal(t, strings.Contains(result.Stderr, "context canceled"), true)
}

//nolint:paralleltest // CLI signals and logging change process state.
func TestRunCLI_ExpiredGuardRejectsCompletion(t *testing.T) {
	previous, writer := slog.Default(), log.Writer()
	defer func() { slog.SetDefault(previous); log.SetOutput(writer) }()
	ctx, cancel := context.WithDeadline(t.Context(), time.Time{})
	defer cancel()
	assert.Equal(t, ctx.Err(), context.DeadlineExceeded)
	result, canceled := runCLI(ctx, t, []string{"--help"}, cli.RunConfig{})
	assert.Equal(t, result.Code, 0)
	assert.Equal(t, canceled, true)
}
