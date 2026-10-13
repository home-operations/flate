package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartProfile_EnvironmentCleanupOnError(t *testing.T) {
	for _, mode := range []string{"cpu", "mem", "block", "mutex", "trace"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("FLATE_PROFILE", mode)
			t.Setenv("FLATE_PROFILE_OUT", dir)
			for range 2 {
				_, stderr, code := runCLI(t, "build", "all", "--path", filepath.Join(dir, "missing"))
				if code != 1 || !strings.Contains(stderr, "does not exist") {
					t.Fatalf("exit=%d stderr=%s", code, stderr)
				}
			}
			name := mode + ".pprof"
			if mode == "trace" {
				name = "trace.out"
			}
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() == 0 {
				t.Fatal("profile was not finalized")
			}
		})
	}
	t.Run("invalid", func(t *testing.T) {
		t.Setenv("FLATE_PROFILE", "heap")
		t.Setenv("FLATE_PROFILE_OUT", t.TempDir())
		_, stderr, code := runCLI(t, "build", "all")
		if code != 1 || !strings.Contains(stderr, "invalid FLATE_PROFILE") {
			t.Fatalf("exit=%d stderr=%s", code, stderr)
		}
	})
}
