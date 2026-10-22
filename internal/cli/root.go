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
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/spf13/cobra"
)

var renderPersistenceDisabledReason = sourcecache.RenderPersistenceDisabledReason()

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
	root.PersistentFlags().String("log-level", "error", "log level (debug, info, warn, error)")
	root.PersistentFlags().Bool("no-progress", false, "disable the live per-resource progress lines on stderr")
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		if err := applyEnvVars(cmd); err != nil {
			return err
		}
		// The live status bar paints to stderr only when it is an
		// interactive terminal (pipes/CI/e2e buffers stay clean), --no-progress
		// is unset. Logs share the progress renderer when it is active.
		barSink = nil
		logSink := cmd.ErrOrStderr()
		noProgress, _ := cmd.Flags().GetBool("no-progress")
		if progressBarEnabled(noProgress, writerIsTTY(cmd.ErrOrStderr())) {
			barSink = newBarWriter(cmd.ErrOrStderr())
			logSink = barSink
		}
		lvl, _ := cmd.Flags().GetString("log-level")
		if err := setLogLevel(lvl, logSink); err != nil {
			return err
		}
		if cmd.Flags().Lookup("path") != nil && renderPersistenceDisabledReason != "" {
			dir, _ := cmd.Flags().GetString("cache-dir")
			if dir != "" {
				return (output.Encoder{Writer: logSink, Color: output.ColorEnabled(logSink)}).Report(output.Model{Warnings: []manifest.Warning{{
					Category: manifest.WarnRenderPersistenceDisabled,
					Message:  "render persistence is disabled: " + renderPersistenceDisabledReason + "; source caching remains available",
				}}})
			}
		}
		return nil
	}

	root.AddCommand(
		newGetCmd(),
		newBuildCmd(),
		newDiffCmd(),
		newTestCmd(),
		newCacheCmd(),
	)
	var wrapProfiles func(*cobra.Command)
	wrapProfiles = func(cmd *cobra.Command) {
		if cmd.RunE != nil && cmd.Flags().Lookup("path") != nil {
			run := cmd.RunE
			cmd.RunE = func(cmd *cobra.Command, args []string) error {
				stopProfile, err := startProfile()
				if err != nil {
					return err
				}
				defer stopProfile()
				return run(cmd, args)
			}
		}
		for _, child := range cmd.Commands() {
			wrapProfiles(child)
		}
	}
	wrapProfiles(root)
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
	err := root.ExecuteContext(ctx)
	if err != nil {
		if _, ok := errors.AsType[reportedError](err); !ok {
			_, _ = io.WriteString(stderr, "flate error: "+err.Error()+"\n")
		}
		return 1
	}
	return 0
}

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
	// Dependency log.Printf records vary with render-cache hits.
	if l <= slog.LevelDebug {
		log.SetOutput(w)
	} else {
		log.SetOutput(io.Discard)
	}
	return nil
}
