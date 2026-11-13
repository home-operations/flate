package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // Profile fixtures change process-wide environment variables.
func TestStartProfile_EnvironmentCleanupOnError(t *testing.T) {
	for _, mode := range []string{"cpu", "mem", "block", "mutex", "trace"} {
		for _, verb := range []string{"build", "test", "diff", "get"} {
			t.Run(mode+"/"+verb, func(t *testing.T) { startProfileEnvironmentCleanupOnErrorCase(t, &mode, &verb) })
		}
	}
	t.Run("embedded get", func(t *testing.T) { startProfileEnvironmentCleanupOnErrorEmbeddedGet(t) })
	t.Run("ineligible handlers", func(t *testing.T) { startProfileEnvironmentCleanupOnErrorIneligibleHandlers(t) })
	t.Run("invalid", func(t *testing.T) { startProfileEnvironmentCleanupOnErrorInvalid(t) })
	t.Run(
		"default output directory",
		func(t *testing.T) { startProfileEnvironmentCleanupOnErrorDefaultOutputDirectory(t) },
	)
	t.Run(
		"output directory diagnostic",
		func(t *testing.T) { startProfileEnvironmentCleanupOnErrorOutputDirectoryDiagnostic(t) },
	)
}

func startProfileEnvironmentCleanupOnErrorCase(t *testing.T, mode, verb *string) {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("FLATE_PROFILE", (*mode))
	t.Setenv("FLATE_PROFILE_OUT", dir)
	wantError := "does not exist"
	if (*verb) == "diff" {
		wantError = "not inside a git working tree"
	}
	for range 2 {
		cli := runCLI(t, (*verb), "all", "--path", filepath.Join(dir, "missing"))
		stderr, code := cli.stderr, cli.code
		assert.FatalIf(
			t,
			code != 1 || !strings.Contains(stderr, wantError),
			"exit=%d stderr=%s",
			code,
			stderr,
		)
	}
	name := (*mode) + ".pprof"
	if (*mode) == "trace" {
		name = "trace.out"
	}
	info, err := os.Stat(filepath.Join(dir, name))
	assert.NoError(t, err)
	assert.FatalIf(t, info.Size() == 0, "%v", "profile was not finalized")
}

func startProfileEnvironmentCleanupOnErrorEmbeddedGet(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("FLATE_PROFILE", "cpu")
	t.Setenv("FLATE_PROFILE_OUT", dir)
	path := writeFixture(t)
	for _, tc := range []struct {
		resource string
		fail     bool
	}{{"images", false}, {"ks", true}, {"ks", false}} {
		input := path
		if tc.fail {
			input = filepath.Join(dir, "missing")
		}
		cmd := New("test", RunConfig{})
		cmd.SetArgs([]string{"get", tc.resource, "--path", input, "--concurrency", "2"})
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		err := cmd.ExecuteContext(t.Context())
		assert.FatalIf(t, (err != nil) != tc.fail, "get %s: %v", tc.resource, err)
		info, statErr := os.Stat(filepath.Join(dir, "cpu.pprof"))
		assert.FatalIf(t, statErr != nil || info.Size() == 0, "profile not finalized: %v", statErr)
	}
}

func startProfileEnvironmentCleanupOnErrorIneligibleHandlers(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("FLATE_PROFILE", "cpu")
	t.Setenv("FLATE_PROFILE_OUT", dir)
	for _, args := range [][]string{{"--help"}, {"get", "--help"}, {"--version"}, {"cache"}, {"completion", "fish"}} {
		cli := runCLI(t, args...)
		stderr, code := cli.stderr, cli.code
		assert.FatalIf(t, code != 0, "%v: %s", args, stderr)
	}
	entries, err := os.ReadDir(dir)
	assert.FatalIf(t, err != nil || len(entries) != 0, "ineligible profile: %v %v", entries, err)
}

func startProfileEnvironmentCleanupOnErrorInvalid(t *testing.T) {
	t.Helper()

	t.Setenv("FLATE_PROFILE", "heap")
	t.Setenv("FLATE_PROFILE_OUT", t.TempDir())
	cli := runCLI(t, "build", "all")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "invalid FLATE_PROFILE"),
		"exit=%d stderr=%s",
		code,
		stderr,
	)
}

func startProfileEnvironmentCleanupOnErrorDefaultOutputDirectory(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("FLATE_PROFILE", "mem")
	t.Setenv("FLATE_PROFILE_OUT", "")
	cli := runCLI(t, "build", "all", "--path", filepath.Join(dir, "missing"))
	stderr, code := cli.stderr, cli.code
	info, err := os.Stat(filepath.Join(dir, "mem.pprof"))
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "does not exist") || err != nil || info.Size() == 0,
		"default profile: exit=%d stderr=%s stat=%v",
		code,
		stderr,
		err,
	)
}

func startProfileEnvironmentCleanupOnErrorOutputDirectoryDiagnostic(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "file", "not a directory")
	t.Setenv("FLATE_PROFILE", "cpu")
	t.Setenv("FLATE_PROFILE_OUT", filepath.Join(dir, "file"))
	cli := runCLI(t, "build", "all")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "FLATE_PROFILE_OUT") || strings.Contains(stderr, "--profile-out"),
		"profile destination: exit=%d stderr=%s",
		code,
		stderr,
	)
}
