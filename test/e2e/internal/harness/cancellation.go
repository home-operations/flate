package harness

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/cli"
)

func runCLI(ctx context.Context, t *testing.T, args []string, cfg cli.RunConfig) (Result, bool) {
	t.Helper()
	deadline, ok := t.Deadline()
	if !ok {
		t.Fatal("CLI fixture requires a test deadline")
	}
	const cleanupFraction = 2
	ctx, cancel := context.WithTimeout(ctx, time.Until(deadline)/cleanupFraction)
	defer cancel()
	// Keep SIGTERM handled if completion races with the guard.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	defer signal.Stop(signals)
	previous := slog.Default()
	done := make(chan Result, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		code := cli.Run(args, &stdout, &stderr, cfg)
		done <- Result{Stdout: stdout.String(), Stderr: stderr.String(), Code: code}
	}()
	// CLI logging initialization follows signal.NotifyContext. Never signal before that point.
	for slog.Default() == previous {
		select {
		case result := <-done:
			return result, ctx.Err() != nil
		default:
			runtime.Gosched()
		}
	}
	select {
	case result := <-done:
		return result, ctx.Err() != nil
	case <-ctx.Done():
		process, err := os.FindProcess(os.Getpid())
		if err != nil {
			panic(err)
		}
		if err := process.Signal(syscall.SIGTERM); err != nil {
			panic(err)
		}
		return <-done, true
	}
}
