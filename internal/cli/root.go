// Package cli wires flate's command-line interface using cobra.
//
// Every multi-kind command takes the same ks/hr/all positional layout:
//
//   - get   ks|hr|all — list Flux objects or summarize the cluster.
//   - build ks|hr|all — render Flux objects to YAML.
//   - diff  ks|hr|images — compare current vs. another path.
//   - test  ks|hr|all — report reconcile status.
//   - diag             — sanity-check local manifests.
//
// Use New() to obtain a cobra.Command for embedding flate in a parent
// CLI; Execute() and Run() are the entry points used by cmd/flate and
// by in-process E2E tests respectively.
package cli

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// New constructs the root command and wires every subcommand into it.
// Callers that want full control over I/O streams should use Run.
func New() *cobra.Command {
	root := &cobra.Command{
		Use:           "flate",
		Short:         "Validate a local Flux GitOps repo without a live cluster.",
		Long:          "flate renders and diffs Flux manifests using the upstream helm, kustomize, and source SDKs — no `helm`, `kustomize`, or `flux` binaries needed.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error)")
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		lvl, _ := cmd.Flags().GetString("log-level")
		setLogLevel(lvl, cmd.ErrOrStderr())
		return nil
	}

	root.AddCommand(
		newGetCmd(),
		newBuildCmd(),
		newDiffCmd(),
		newTestCmd(),
		newDiagCmd(),
	)
	return root
}

// Execute runs the root command against os.Args and returns the
// suggested process exit code.
func Execute() int {
	return Run(os.Args[1:], os.Stdout, os.Stderr)
}

// Run executes flate with the supplied argv and I/O streams, returning
// the exit code. Used by cmd/flate's main and by in-process tests.
//
// A context that listens for SIGINT / SIGTERM is propagated to commands
// via cobra.Command.Context, so Ctrl-C cleanly cancels in-flight work.
func Run(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := New()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		_, _ = io.WriteString(stderr, "flate error: "+err.Error()+"\n")
		return 1
	}
	return 0
}

func setLogLevel(lvl string, w io.Writer) {
	var l slog.Level
	switch lvl {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: l})))
}
