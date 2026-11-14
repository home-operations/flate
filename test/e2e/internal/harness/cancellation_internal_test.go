package harness

import (
	"bytes"
	"context"
	"log" //nolint:depguard // Restoring slog.SetDefault also requires restoring the standard logger writer.
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/cli"
	"github.com/home-operations/flate/internal/testutil"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
)

// cli.Run and SIGTERM MUST remain serial because they change process state.
func TestE2E_CancellationSignal(t *testing.T) {
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
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- cli.Run([]string{
			"build", "all", "--path", filepath.Join(root, "flux"),
			"--concurrency", "2", "--no-progress",
		}, &stdout, &stderr, cli.RunConfig{GitTransports: routes})
	}()
	select {
	case <-entered:
		process, err := os.FindProcess(os.Getpid())
		assert.NoError(t, err)
		assert.NoError(t, process.Signal(syscall.SIGTERM))
	case code := <-done:
		t.Fatalf("CLI exited before transport entry: code=%d stderr=%s", code, stderr.String())
	}
	assert.Equal(t, <-done, 1)
	assert.Equal(t, strings.Contains(stderr.String(), "context canceled"), true)
}
