package cli

import (
	"bytes"
	"io"
	"log" //nolint:depguard // Standard-log routing must capture or suppress dependency log output.
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

type countedDetailError struct{ calls int }

func (e *countedDetailError) Error() string {
	e.calls++
	return "upstream diagnostic"
}

//nolint:paralleltest // test changes process-wide logging.
func TestRun_FailureDetailsEvaluation(t *testing.T) {
	path := writeFixture(t)
	for _, level := range []string{"error", "debug"} {
		t.Run(level, func(t *testing.T) {
			prior, writer, flags := slog.Default(), log.Writer(), log.Flags()
			t.Cleanup(func() {
				slog.SetDefault(prior)
				log.SetOutput(writer)
				log.SetFlags(flags)
			})
			detail := new(countedDetailError)
			failure := manifest.NewError(manifest.ErrInput, detail, "output failed")
			var stderr bytes.Buffer
			code := Run(
				[]string{
					"build",
					"all",
					"--path",
					path,
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
					"--log-level",
					level,
				},
				failingWriter{err: failure},
				&stderr,
			)
			assert.Equal(t, code, 1)
			assert.Equal(t, detail.calls > 0, level == "debug")
			assert.Equal(t, strings.Contains(stderr.String(), "upstream diagnostic"), level == "debug")
			assert.Equal(t, strings.Contains(stderr.String(), "output failed"), true)
		})
	}
}

const (
	persistenceReason  = "the binary was built from a dirty checkout"
	persistenceMessage = "[RenderPersistenceDisabled] render persistence is disabled: " + persistenceReason + "; source caching remains available"
	persistenceNotice  = "\n  ⚠ warnings (1)\n    " + persistenceMessage + "\n"
)

func preservePersistenceNoticeState(t *testing.T) {
	t.Helper()
	savedReason := renderPersistenceDisabledReason
	savedBar := barSink
	savedLogger, savedLogWriter := slog.Default(), log.Writer()
	t.Cleanup(func() {
		renderPersistenceDisabledReason = savedReason
		barSink = savedBar
		slog.SetDefault(savedLogger)
		log.SetOutput(savedLogWriter)
	})
	t.Setenv("FLATE_CACHE_DIR", "")
	t.Setenv("FLATE_LOG_LEVEL", "info")
	assert.NoError(t, os.Unsetenv("FLATE_LOG_LEVEL"))
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_RenderPersistenceNotice(t *testing.T) {
	preservePersistenceNoticeState(t)
	path := writeMultiNamespaceFixture(t)
	args := []string{"build", "all", "--path", path, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	renderPersistenceDisabledReason = ""
	cli := runCLI(t, args...)
	want, wantErr, wantCode := cli.stdout, cli.stderr, cli.code
	assert.Equal(t, wantCode, 0)
	assert.FatalIf(
		t,
		!strings.Contains(want, "apps-a") || !strings.Contains(want, "apps-b"),
		"fixture did not render both objects: %s",
		want,
	)
	for _, tc := range []struct {
		name   string
		reason string
		args   []string
		notice string
	}{
		{name: "disabled with directory", reason: persistenceReason, args: args, notice: persistenceNotice},
		{name: "eligible with directory", args: args},
		{name: "disabled without directory", reason: persistenceReason, args: append(append([]string{}, args...), "--cache-dir=")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			renderPersistenceDisabledReason = tc.reason
			cli2 := runCLI(t, tc.args...)
			stdout, stderr, code := cli2.stdout, cli2.stderr, cli2.code
			assert.Equal(t, code, wantCode)
			assert.Equal(t, stdout, want)
			assert.Equal(t, stderr, wantErr+tc.notice)
		})
	}
}

func TestRun_RenderPersistenceNoticeEnvAndLevel(t *testing.T) {
	preservePersistenceNoticeState(t)
	path := writeFixture(t)
	args := []string{"build", "all", "--path", path, "--concurrency", "2"}
	renderPersistenceDisabledReason = ""
	cli := runCLI(t, args...)
	want, wantCode := cli.stdout, cli.code
	assert.Equal(t, wantCode, 0)
	renderPersistenceDisabledReason = persistenceReason
	t.Setenv("FLATE_CACHE_DIR", t.TempDir())
	for _, tc := range []struct {
		name     string
		args     []string
		envLevel string
		count    int
	}{
		{name: "environment directory", count: 1},
		{name: "empty flag overrides directory", args: []string{"--cache-dir="}},
		{name: "explicit error threshold", args: []string{"--log-level=error"}, count: 1},
		{name: "environment error threshold", envLevel: "error", count: 1},
		{name: "explicit warn threshold", args: []string{"--log-level=warn"}, count: 1},
		{name: "explicit info threshold", args: []string{"--log-level=info"}, count: 1},
		{name: "explicit debug threshold", args: []string{"--log-level=debug"}, count: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_LOG_LEVEL", tc.envLevel)
			if tc.envLevel == "" {
				assert.NoError(t, os.Unsetenv("FLATE_LOG_LEVEL"))
			}
			cli2 := runCLI(t, append(append([]string{}, args...), tc.args...)...)
			stdout, stderr, code := cli2.stdout, cli2.stderr, cli2.code
			assert.Equal(t, code, wantCode)
			assert.Equal(t, stdout, want)
			assert.Equal(t, strings.Count(stderr, "render persistence is disabled"), tc.count)
			assert.Equal(t, strings.Count(stderr, persistenceNotice), tc.count)
		})
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_RenderPersistenceNoticeCommands(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	t.Setenv("FLATE_CACHE_DIR", t.TempDir())
	path := writeFixture(t)
	orig := t.TempDir()
	testutil.CopyTree(t, path, orig)
	for _, tc := range []struct {
		name   string
		args   []string
		render bool
	}{
		{name: "build", args: []string{"build", "all"}, render: true},
		{name: "get", args: []string{"get", "ks"}, render: true},
		{name: "test", args: []string{"test", "all"}, render: true},
		{name: "diff", args: []string{"diff", "ks", "--path-orig", orig}, render: true},
		{name: "cache gc", args: []string{"cache", "gc", "--dry-run"}},
		{name: "help", args: []string{"build", "all", "--help"}},
		{name: "version", args: []string{"--version"}},
		{name: "completion", args: []string{"completion", "fish"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{}, tc.args...)
			count := 0
			if tc.render {
				args = append(args, "--path", path, "--concurrency", "2")
				count = 1
			}
			cli := runCLI(t, args...)
			stdout, stderr, code := cli.stdout, cli.stderr, cli.code
			assert.Equal(t, code, 0)
			assert.Equal(t, strings.Contains(stdout, "render persistence is disabled"), false)
			assert.Equal(t, strings.Count(stderr, "render persistence is disabled"), count)
			assert.Equal(t, strings.Count(stderr, persistenceMessage), count)
		})
	}
}

func TestRun_RenderPersistenceNoticeErrors(t *testing.T) {
	preservePersistenceNoticeState(t)
	path := writeFixture(t)
	args := []string{"build", "all", "--path", path, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	for _, tc := range []struct {
		name    string
		args    []string
		env     string
		wantErr string
		notice  string
		profile string
	}{
		{name: "invalid level", args: []string{"--log-level=invalid"}, wantErr: "invalid --log-level"},
		{name: "invalid arguments", args: []string{"unexpected"}, wantErr: "unknown command"},
		{name: "invalid environment", env: "invalid", wantErr: "invalid FLATE_CONCURRENCY"},
		{name: "cache initialization", args: []string{"--cache-dir", filepath.Join(path, "flux", "cluster.yaml")}, wantErr: "cache", notice: persistenceNotice},
		{name: "baseline setup", args: []string{"--path-orig", path, "--base", "HEAD"}, wantErr: "mutually exclusive", notice: persistenceNotice},
		{name: "invalid kind", args: []string{"--allow-missing-kinds=invalid"}, wantErr: "--allow-missing-kinds", notice: persistenceNotice},
		{name: "profile startup", profile: "invalid", wantErr: "invalid FLATE_PROFILE", notice: persistenceNotice},
		{name: "missing path", args: []string{"--path", filepath.Join(t.TempDir(), "missing")}, wantErr: "missing", notice: persistenceNotice},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invocation := append(append([]string{}, args...), tc.args...)
			if tc.profile != "" {
				t.Setenv("FLATE_PROFILE", tc.profile)
				t.Setenv("FLATE_PROFILE_OUT", t.TempDir())
			}
			if tc.env != "" {
				t.Setenv("FLATE_CONCURRENCY", tc.env)
				invocation = []string{"build", "all", "--path", path, "--cache-dir", t.TempDir()}
			}
			renderPersistenceDisabledReason = ""
			cli := runCLI(t, invocation...)
			want, wantErr, wantCode := cli.stdout, cli.stderr, cli.code
			assert.Equal(t, wantCode, 1)
			assert.FatalIf(t, !strings.Contains(wantErr, tc.wantErr), "expected %q in %q", tc.wantErr, wantErr)
			renderPersistenceDisabledReason = persistenceReason
			cli2 := runCLI(t, invocation...)
			stdout, stderr, code := cli2.stdout, cli2.stderr, cli2.code
			assert.Equal(t, code, wantCode)
			assert.Equal(t, stdout, want)
			assert.Equal(t, stderr, wantErr+tc.notice)
		})
	}
	t.Run("advisory write failure", func(t *testing.T) {
		renderPersistenceDisabledReason = ""
		cli3 := runCLI(t, args...)
		want, wantCode := cli3.stdout, cli3.code
		assert.Equal(t, wantCode, 0)
		renderPersistenceDisabledReason = persistenceReason
		var stdout bytes.Buffer
		var stderr persistenceNoticeFailWriter
		assert.Equal(t, Run(args, &stdout, &stderr), 1)
		assert.Equal(t, stdout.String(), want)
		assert.FatalIf(
			t,
			!strings.Contains(stderr.String(), io.ErrClosedPipe.Error()),
			"missing diagnostic writer error: %s",
			stderr.String(),
		)
		assert.Equal(t, stderr.rejected, 1)
	})
}

type persistenceNoticeFailWriter struct {
	bytes.Buffer

	rejected int
}

func (w *persistenceNoticeFailWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

func (w *persistenceNoticeFailWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("[RenderPersistenceDisabled]")) {
		w.rejected++
		return failingWriter{err: io.ErrClosedPipe}.Write(p)
	}
	return w.Buffer.Write(p)
}

//nolint:paralleltest // test changes process-wide logging.
func TestRun_ProgressSinkResetsPerCommand(t *testing.T) {
	savedBar := barSink
	savedLogger, savedLogWriter := slog.Default(), log.Writer()
	t.Cleanup(func() {
		barSink = savedBar
		slog.SetDefault(savedLogger)
		log.SetOutput(savedLogWriter)
	})

	// The TTY detector accepts character devices; cache gc never starts a renderer.
	terminal, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	assert.NoError(t, err)
	defer terminal.Close()

	for _, tc := range []struct {
		name   string
		args   []string
		stderr io.Writer
	}{
		{name: "no-progress", args: []string{"--no-progress"}, stderr: terminal},
		{name: "redirected stderr", stderr: &bytes.Buffer{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"cache", "gc", "--dry-run", "--cache-dir", t.TempDir()}
			{
				code := Run(append(args, "--no-progress=false"), io.Discard, terminal)
				assert.FatalIf(t, code != 0, "first command exited %d", code)
			}
			assert.FatalIf(t, barSink == nil, "%v", "first command did not enable progress")
			{
				code := Run(append(args, tc.args...), io.Discard, tc.stderr)
				assert.FatalIf(t, code != 0, "second command exited %d", code)
			}
			assert.ErrorIf(t, barSink != nil, "%v", "second command retained the first command's progress sink")
		})
	}
}
