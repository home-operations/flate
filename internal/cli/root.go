// Package cli wires flate's command-line interface using cobra.
//
// Every multi-kind command takes the same ks/hr/all positional layout:
//
//   - get   ks|hr|images|all — list Flux objects, images, or cluster summary.
//   - build ks|hr|all        — render Flux objects to YAML.
//   - diff  ks|hr|images|all — compare current vs. another path.
//   - test  ks|hr|all        — report reconcile status.
//
// Use New() to obtain a cobra.Command for embedding flate in a parent
// CLI; Execute() and Run() are the entry points used by cmd/flate and
// by in-process E2E tests respectively.
package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// New constructs the root command and wires every subcommand into it.
// Callers that want full control over I/O streams should use Run.
// version is exposed via --version and the standard cobra template.
func New(version string) *cobra.Command {
	root := &cobra.Command{
		Use:           "flate",
		Short:         "Validate a local Flux GitOps repo without a live cluster.",
		Long:          "flate renders and diffs Flux manifests using the upstream helm, kustomize, and source SDKs — no `helm`, `kustomize`, or `flux` binaries needed.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error)")
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		// Env binding runs first so FLATE_LOG_LEVEL is honored by the
		// validation below, and downstream RunE bodies see flags that
		// reflect the merged (CLI > env > default) view.
		if err := applyEnvVars(cmd); err != nil {
			return err
		}
		lvl, _ := cmd.Flags().GetString("log-level")
		return setLogLevel(lvl, cmd.ErrOrStderr())
	}

	root.AddCommand(
		newGetCmd(),
		newBuildCmd(),
		newDiffCmd(),
		newTestCmd(),
		newCacheCmd(),
	)
	annotateEnvUsage(root)
	return root
}

// Execute runs the root command against os.Args and returns the
// suggested process exit code. version is the build identifier
// surfaced via --version.
func Execute(version string) int {
	return run(version, os.Args[1:], os.Stdout, os.Stderr)
}

// Run executes flate with the supplied argv and I/O streams, returning
// the exit code. Test-only — production callers go through Execute.
func Run(args []string, stdout, stderr io.Writer) int {
	return run("dev", args, stdout, stderr)
}

// run is the shared body of Execute / Run.
//
// A context that listens for SIGINT / SIGTERM is propagated to commands
// via cobra.Command.Context, so Ctrl-C cleanly cancels in-flight work.
func run(version string, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := New(version)
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		_, _ = io.WriteString(stderr, "flate error: "+err.Error()+"\n")
		return 1
	}
	return 0
}

// setLogLevel validates lvl against the published enum and installs
// the slog default. Unknown values are rejected up-front so
// `--log-level bogus` fails clearly instead of silently degrading to
// info — the previous behavior misled users who typo'd `--log-level
// dbug` and assumed Debug output was simply quiet.
func setLogLevel(lvl string, w io.Writer) error {
	var l slog.Level
	switch lvl {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return fmt.Errorf("invalid --log-level %q: must be one of debug, info, warn, error", lvl)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: l})))
	return nil
}
