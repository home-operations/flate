package cli

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/source/cache"
)

func TestCacheGC_ExplicitRoot(t *testing.T) {
	for _, scenario := range []string{"unset", "empty", "flag", "env", "empty-flag-overrides-env"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("FLATE_CACHE_DIR", "")
			root := filepath.Join(t.TempDir(), "cache")
			args := []string{"cache", "gc", "--dry-run"}
			switch scenario {
			case "unset":
				assert.NoError(t, os.Unsetenv("FLATE_CACHE_DIR"))
			case "flag":
				args = append(args, "--cache-dir", root)
			case "env":
				t.Setenv("FLATE_CACHE_DIR", root)
			case "empty-flag-overrides-env":
				t.Setenv("FLATE_CACHE_DIR", root)
				args = append(args, "--cache-dir=")
			}
			for _, key := range []string{"HOME", "XDG_CACHE_HOME", "TMPDIR"} {
				t.Setenv(key, t.TempDir())
			}
			stdout, stderr, code := runCLI(t, args...)
			wantCode := 1
			if scenario == "flag" || scenario == "env" {
				wantCode = 0
			}
			assert.FatalIf(t, code != wantCode, "exit = %d, want %d: %s", code, wantCode, stderr)
			assert.FatalIf(
				t,
				wantCode == 0 && !strings.Contains(stdout, "blob reclamation is decided by OCI graph GC"),
				"missing blob reclamation report: %s",
				stdout,
			)
			{
				_, err := os.Stat(root)
				assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "dry-run created cache root: %v", err)
			}
			for _, key := range []string{"HOME", "XDG_CACHE_HOME", "TMPDIR"} {
				entries, err := os.ReadDir(os.Getenv(key))
				assert.FatalIf(
					t,
					err != nil || len(entries) != 0,
					"cache maintenance wrote under %s: %v, %v",
					key,
					entries,
					err,
				)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestCache_RemovedSurfaceUnavailable(t *testing.T) {
	for _, args := range [][]string{{"cache", "removed-command"}, {"cache", "gc", "--include-mirrors"}} {
		{
			_, stderr, code := runCLI(t, args...)
			assert.FatalIf(t, code != 1, "removed surface %v exited %d: %s", args, code, stderr)
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestCacheGC_WarningWriteError(t *testing.T) {
	root := t.TempDir()
	owner, err := cache.New(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, owner.Close())
	index := filepath.Join(root, "oci", "index.json")
	assert.NoError(t, os.Chmod(index, 0o400))
	t.Cleanup(func() { _ = os.Chmod(index, 0o600) })
	if writable, err := os.OpenFile(index, os.O_WRONLY, 0); err == nil {
		_ = writable.Close()
		t.Skip("filesystem does not enforce fixture permissions")
	}
	args := []string{"cache", "gc", "--cache-dir", root, "--max-age=0"}
	var wantOut, wantErr string
	for run := range 2 {
		stdout, stderr, code := runCLI(t, args...)
		assert.FatalIf(t, code != 0 || strings.Count(stderr, "[CacheMaintenance]") != 1 ||
			!strings.Contains(stderr, "save swept index") ||
			strings.Contains(stdout, "save swept index") ||
			!strings.Contains(stdout, "0 entries") ||
			!strings.Contains(stdout, "1 errors"), "cache advisory: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		switch {
		case run == 0:

			wantOut, wantErr = stdout, stderr

		case stdout != wantOut || stderr != wantErr:

			t.Fatal("cache warning order or product changed")
		}
	}
	writeErr := errors.New("diagnostic closed")
	cmd := New("test")
	cmd.SetArgs(args)
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(failingWriter{err: writeErr})
	{
		err := cmd.ExecuteContext(t.Context())
		assert.FatalIf(t, !errors.Is(err, writeErr), "warning write error = %v", err)
	}
	assert.FatalIf(t, stdout.String() != wantOut, "product = %q, want %q", stdout.String(), wantOut)
	cmd = New("test")
	cmd.SetArgs(args)
	cmd.SetOut(failingWriter{err: io.ErrClosedPipe})
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	{
		err := cmd.ExecuteContext(t.Context())
		assert.FatalIf(
			t,
			!errors.Is(err, io.ErrClosedPipe) || stderr.String() != wantErr,
			"product error = %v, diagnostics = %q",
			err,
			stderr.String(),
		)
	}
}
