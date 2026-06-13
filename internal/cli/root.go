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
	root.PersistentFlags().Bool("no-progress", false, "disable the live per-resource progress lines on stderr")
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		// Env binding runs first so FLATE_LOG_LEVEL is honored by the
		// validation below, and downstream RunE bodies see flags that
		// reflect the merged (CLI > env > default) view.
		if err := applyEnvVars(cmd); err != nil {
			return err
		}
		// The live status bar paints to stderr only when it is an
		// interactive terminal (pipes/CI/e2e buffers stay clean), --no-progress
		// is unset, and a --stream build isn't sharing that terminal with its
		// stdout output (the sticky bar and raw streamed YAML would interleave —
		// see progressBarEnabled). When active, slog is routed through the same
		// stderrRouter so log records interleave above the bar instead of
		// corrupting it; otherwise slog writes straight to stderr.
		//
		// `stream` is a build-subcommand flag, absent on other commands —
		// GetBool then returns (false, err), which is the correct "no stream".
		noProgress, _ := cmd.Flags().GetBool("no-progress")
		stream, _ := cmd.Flags().GetBool("stream")
		barSink = nil
		logSink := cmd.ErrOrStderr()
		if progressBarEnabled(noProgress, stream, writerIsTTY(cmd.OutOrStdout()), writerIsTTY(cmd.ErrOrStderr())) {
			barSink = newBarWriter(cmd.ErrOrStderr())
			logSink = barSink
		}
		lvl, _ := cmd.Flags().GetString("log-level")
		return setLogLevel(lvl, logSink)
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
	err := root.ExecuteContext(ctx)
	// Flush any deferred log notes a command didn't already render (diff/get/
	// cache, or a clean run) so nothing buffered is lost.
	if logBuffer != nil {
		for _, n := range logBuffer.drain() {
			line := n.Text
			if n.Count > 1 {
				line = fmt.Sprintf("%s (×%d)", line, n.Count)
			}
			_, _ = io.WriteString(stderr, line+"\n")
		}
	}
	if err != nil {
		// reportFailures already rendered a styled report for this error; don't
		// reprint it as a flat "flate error: …" line.
		var reported reportedError
		if !errors.As(err, &reported) {
			_, _ = io.WriteString(stderr, "flate error: "+err.Error()+"\n")
		}
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
	// Wrap the text handler in a deferring sink so the Warn/Info chatter a
	// failing run emits (chiefly "resource orphaned") is held back and rendered
	// in the report footer instead of interleaving with output; Error records
	// still pass straight through. Commands drain it via reportFailures; run
	// flushes anything left so no note is lost.
	logBuffer = newDeferSink(slog.NewTextHandler(w, &slog.HandlerOptions{Level: l}))
	slog.SetDefault(slog.New(logBuffer))
	// slog.SetDefault also reroutes the standard library `log` package through
	// this handler. A dependency's log.Printf — chiefly Helm's values-coalesce
	// "destination … is a table" warnings (chart/common/util/coalesce.go uses
	// log.Printf, not slog) — would then land in the notes footer, but ONLY on a
	// render-cache miss: a cache hit never re-runs the code that logs it, so the
	// footer differs between otherwise-identical runs depending on cache state
	// (looks like a race). flate's own diagnostics all use slog, never the
	// stdlib logger, so detach `log` from the footer: discard it by default
	// (notes stay deterministic and free of dependency render-noise), or pass it
	// straight to the sink under --log-level debug, where determinism isn't
	// promised and the dependency chatter aids troubleshooting.
	if l <= slog.LevelDebug {
		log.SetOutput(w)
	} else {
		log.SetOutput(io.Discard)
	}
	return nil
}
