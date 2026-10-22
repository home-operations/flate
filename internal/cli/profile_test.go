package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestStartProfile_EnvironmentCleanupOnError(t *testing.T) {
	for _, mode := range []string{"cpu", "mem", "block", "mutex", "trace"} {
		for _, verb := range []string{"build", "test", "diff", "get"} {
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
	t.Run("embedded get", func(t *testing.T) {
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
			cmd := New("test")
			cmd.SetArgs([]string{"get", tc.resource, "--path", input, "--concurrency", "2"})
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.ExecuteContext(t.Context())
			if (err != nil) != tc.fail {
				t.Fatalf("get %s: %v", tc.resource, err)
			}
			info, statErr := os.Stat(filepath.Join(dir, "cpu.pprof"))
			if statErr != nil || info.Size() == 0 {
				t.Fatalf("profile not finalized: %v", statErr)
			}
		}
	})
	t.Run("ineligible handlers", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("FLATE_PROFILE", "cpu")
		t.Setenv("FLATE_PROFILE_OUT", dir)
		for _, args := range [][]string{{"--help"}, {"get", "--help"}, {"--version"}, {"cache"}, {"completion", "fish"}} {
			_, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("%v: %s", args, stderr)
			}
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("ineligible profile: %v %v", entries, err)
		}
	})
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
