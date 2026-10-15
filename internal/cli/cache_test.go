package cli

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCacheGC_ExplicitRoot(t *testing.T) {
	for _, scenario := range []string{"unset", "empty", "flag", "env", "empty-flag-overrides-env"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("FLATE_CACHE_DIR", "")
			root := filepath.Join(t.TempDir(), "cache")
			args := []string{"cache", "gc", "--dry-run"}
			switch scenario {
			case "unset":
				if err := os.Unsetenv("FLATE_CACHE_DIR"); err != nil {
					t.Fatal(err)
				}
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
			if code != wantCode {
				t.Fatalf("exit = %d, want %d: %s", code, wantCode, stderr)
			}
			if wantCode == 0 && !strings.Contains(stdout, "blob reclamation is decided by OCI graph GC") {
				t.Fatalf("missing blob reclamation report: %s", stdout)
			}
			if _, err := os.Stat(root); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("dry-run created cache root: %v", err)
			}
			for _, key := range []string{"HOME", "XDG_CACHE_HOME", "TMPDIR"} {
				entries, err := os.ReadDir(os.Getenv(key))
				if err != nil || len(entries) != 0 {
					t.Fatalf("cache maintenance wrote under %s: %v, %v", key, entries, err)
				}
			}
		})
	}
}

func TestCache_RemovedSurfaceUnavailable(t *testing.T) {
	for _, args := range [][]string{{"cache", "removed-command"}, {"cache", "gc", "--include-mirrors"}} {
		if _, stderr, code := runCLI(t, args...); code != 1 {
			t.Fatalf("removed surface %v exited %d: %s", args, code, stderr)
		}
	}
}
