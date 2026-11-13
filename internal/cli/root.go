// Package cli wires flate's command-line interface using cobra.
//
// Every multi-kind command takes the same ks/hr/all positional layout:
//
//   - get   ks|hr|images|all — list Flux objects or images.
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
	"log" //nolint:depguard // Standard-log routing must capture or suppress dependency log output.
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
)

type renderNoticeKey struct{}

//nolint:gochecknoglobals // The startup reason is fixed once and replaced by serial diagnostic tests.
var renderPersistenceDisabledReason = sourcecache.RenderPersistenceDisabledReason()

// RunConfig supplies invocation transports without exposing command-line flags.
type RunConfig struct {
	GitTransports sourcegit.Transports
}

// New snapshots invocation transports before command execution.
func New(version string, cfg RunConfig) *cobra.Command {
	cfg.GitTransports = maps.Clone(cfg.GitTransports)
	network := new(source.Network)
	_, transportErr := sourcegit.NewClient(sourcegit.ClientConfig{Transports: cfg.GitTransports, Network: network})
	network.Close()
	root := &cobra.Command{
		Use:           "flate",
		Short:         "Render and compare Flux manifests offline",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().String("log-level", "error", "Set log level: debug,info,warn,error")
	root.PersistentFlags().Bool("no-progress", false, "Hide live progress on stderr")
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if transportErr != nil {
			return transportErr
		}
		return prepareCommand(cmd, args)
	}

	root.AddCommand(
		newGetCmd(cfg),
		newBuildCmd(cfg),
		newDiffCmd(cfg),
		newTestCmd(cfg),
		newCacheCmd(),
	)

	wrapProfiles(root)
	annotateEnvUsage(root)
	return root
}

// Execute runs the root command against [os.Args] and returns the
// suggested process exit code. version is the build identifier
// surfaced via --version.
func Execute(version string) int {
	return run(New(version, RunConfig{}), os.Args[1:], os.Stdout, os.Stderr)
}

// Run routes dependency logs through stderr and changes process logging state.
func Run(args []string, stdout, stderr io.Writer, cfg RunConfig) int {
	return run(New("dev", cfg), args, stdout, stderr)
}

// A context that listens for SIGINT / SIGTERM is propagated to commands
// via cobra.Command.Context, so Ctrl-C cleanly cancels in-flight work.
func run(root *cobra.Command, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	err := root.ExecuteContext(ctx)
	if err != nil {
		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.DebugContext(ctx, "cli: invocation failed", "err", manifest.ErrorDetails(err))
		}
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

func prepareCommand(cmd *cobra.Command, _ []string) error {
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
	var notice manifest.Warning
	if cmd.Flags().Lookup(pathKey) != nil && renderPersistenceDisabledReason != "" {
		dir, _ := cmd.Flags().GetString("cache-dir")
		if dir != "" {
			notice = manifest.Warning{
				Category: manifest.WarnRenderPersistenceDisabled,
				Message:  "render persistence is disabled: " + renderPersistenceDisabledReason + "; source caching remains available",
			}
		}
	}
	cmd.SetContext(context.WithValue(cmd.Context(), renderNoticeKey{}, notice))
	return nil
}

func wrapProfiles(cmd *cobra.Command) {
	if cmd.RunE != nil && cmd.Flags().Lookup(pathKey) != nil {
		run := cmd.RunE
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			stopProfile, err := startProfile()
			if err != nil {
				notice, _ := cmd.Context().Value(renderNoticeKey{}).(manifest.Warning)
				return finishSelected(&selectedRequest{
					notice: notice,
					stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(),
				},
					&selectedInvocation{err: err, remainder: err}, &selectedPayload{})
			}
			defer stopProfile()
			return run(cmd, args)
		}
	}
	for _, child := range cmd.Commands() {
		wrapProfiles(child)
	}
}
