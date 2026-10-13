package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestStartProfile_EnvironmentCleanupOnError(t *testing.T) {
	for _, mode := range []string{"cpu", "mem", "block", "mutex", "trace"} {
		for _, verb := range []string{"build", "test", "diff"} {
			t.Run(mode+"/"+verb, func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("FLATE_PROFILE", mode)
				t.Setenv("FLATE_PROFILE_OUT", dir)
				wantError := "does not exist"
				if verb == "diff" {
					wantError = "not inside a git working tree"
				}
				for range 2 {
					_, stderr, code := runCLI(t, verb, "all", "--path", filepath.Join(dir, "missing"))
					if code != 1 || !strings.Contains(stderr, wantError) {
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
	}
	t.Run("invalid", func(t *testing.T) {
		t.Setenv("FLATE_PROFILE", "heap")
		t.Setenv("FLATE_PROFILE_OUT", t.TempDir())
		_, stderr, code := runCLI(t, "build", "all")
		if code != 1 || !strings.Contains(stderr, "invalid FLATE_PROFILE") {
			t.Fatalf("exit=%d stderr=%s", code, stderr)
		}
	})
	t.Run("default output directory", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		t.Setenv("FLATE_PROFILE", "mem")
		t.Setenv("FLATE_PROFILE_OUT", "")
		_, stderr, code := runCLI(t, "build", "all", "--path", filepath.Join(dir, "missing"))
		info, err := os.Stat(filepath.Join(dir, "mem.pprof"))
		if code != 1 || !strings.Contains(stderr, "does not exist") || err != nil || info.Size() == 0 {
			t.Fatalf("default profile: exit=%d stderr=%s stat=%v", code, stderr, err)
		}
	})
	t.Run("output directory diagnostic", func(t *testing.T) {
		dir := t.TempDir()
		testutil.WriteFile(t, dir, "file", "not a directory")
		t.Setenv("FLATE_PROFILE", "cpu")
		t.Setenv("FLATE_PROFILE_OUT", filepath.Join(dir, "file"))
		_, stderr, code := runCLI(t, "build", "all")
		if code != 1 || !strings.Contains(stderr, "FLATE_PROFILE_OUT") || strings.Contains(stderr, "--profile-out") {
			t.Fatalf("profile destination: exit=%d stderr=%s", code, stderr)
		}
	})
}
