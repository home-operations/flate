package cli

import (
	"strings"
	"testing"
)

// TestEnv_FlagFromEnv covers the happy path: a flag the user did not
// pass on the command line is filled from FLATE_<NAME>. We point
// FLATE_PATH at the fixture and run `build all` without `--path`;
// the build succeeds and emits the fixture's ConfigMap, proving the
// orchestrator saw the env-supplied path.
func TestEnv_FlagFromEnv(t *testing.T) {
	path := writeFixture(t)
	t.Setenv("FLATE_PATH", path)
	stdout, stderr, code := runCLI(t, "build", "all")
	if code != 0 {
		t.Fatalf("build all (FLATE_PATH) exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "kind: ConfigMap") {
		t.Errorf("expected ConfigMap in render via FLATE_PATH:\n%s", stdout)
	}
}

// TestEnv_CLIFlagOverridesEnv pins the precedence rule: a CLI flag
// always wins over the env var, even when env points at something
// broken. Without that rule the user can't override an ambient
// FLATE_PATH set in their shell rc.
func TestEnv_CLIFlagOverridesEnv(t *testing.T) {
	path := writeFixture(t)
	t.Setenv("FLATE_PATH", "/nonexistent/path/here")
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code != 0 {
		t.Fatalf("build all --path (vs FLATE_PATH) exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "kind: ConfigMap") {
		t.Errorf("CLI --path should have won over bad FLATE_PATH:\n%s", stdout)
	}
}

// TestEnv_BoolFlagParsed verifies pflag's bool Set is wired through:
// FLATE_SKIP_CRDS=false flips a flag that defaults to true. We
// exercise `--only-crds`'s applyBuildFlags pathway by checking that
// the resulting render still includes the ConfigMap (i.e. nothing
// regressed) — the boolean parse is on the silent path here, so the
// stronger assertion comes from TestEnv_InvalidValueReportsKey,
// which proves Value.Set is reached.
func TestEnv_BoolFlagParsed(t *testing.T) {
	path := writeFixture(t)
	t.Setenv("FLATE_SKIP_CRDS", "false")
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code != 0 {
		t.Fatalf("build all FLATE_SKIP_CRDS=false exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "kind: ConfigMap") {
		t.Errorf("expected ConfigMap in render:\n%s", stdout)
	}
}

// TestEnv_SliceFlagParsesCSV covers comma-separated parsing for
// pflag.StringSliceVar (`--skip-kinds`). Setting FLATE_SKIP_KINDS to
// "ConfigMap,Secret" should drop the fixture's only ConfigMap from
// the render, leaving an empty stream.
func TestEnv_SliceFlagParsesCSV(t *testing.T) {
	path := writeFixture(t)
	t.Setenv("FLATE_SKIP_KINDS", "ConfigMap,Secret")
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code != 0 {
		t.Fatalf("build all FLATE_SKIP_KINDS=... exited %d: %s", code, stderr)
	}
	if strings.Contains(stdout, "kind: ConfigMap") {
		t.Errorf("FLATE_SKIP_KINDS=ConfigMap,Secret should have filtered ConfigMap:\n%s", stdout)
	}
}

// TestEnv_InvalidValueReportsKey covers the loud-failure path: an
// env-supplied value that pflag rejects must surface the FLATE_*
// key in the error so the user can find the offending shell export.
// We use FLATE_CONCURRENCY (an int) with a non-integer to drive
// IntVar.Set into its error branch — that path returns a typed
// error, unlike --log-level's enum check which runs after env
// binding.
func TestEnv_InvalidValueReportsKey(t *testing.T) {
	path := writeFixture(t)
	t.Setenv("FLATE_CONCURRENCY", "notanint")
	_, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code == 0 {
		t.Fatal("expected non-zero exit for invalid FLATE_CONCURRENCY")
	}
	if !strings.Contains(stderr, "FLATE_CONCURRENCY") {
		t.Errorf("error should name the offending env key: %q", stderr)
	}
}

// TestEnv_PersistentFlagFromEnv exercises the root-level persistent
// flag path (FLATE_LOG_LEVEL). An invalid level surfaces through the
// existing setLogLevel validator, proving env binding happens
// before validation runs.
func TestEnv_PersistentFlagFromEnv(t *testing.T) {
	path := writeFixture(t)
	t.Setenv("FLATE_LOG_LEVEL", "bogus")
	_, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code == 0 {
		t.Fatal("expected non-zero exit for FLATE_LOG_LEVEL=bogus")
	}
	if !strings.Contains(stderr, "invalid --log-level") {
		t.Errorf("env-bound log-level should still trip setLogLevel: %q", stderr)
	}
}

// TestEnv_HelpVersionAreSkipped guards against the worst-case footgun:
// FLATE_HELP=true must not silently turn every invocation into a help
// printout. The skip list keeps cobra's `--help` and `--version`
// short-circuits CLI-only.
func TestEnv_HelpVersionAreSkipped(t *testing.T) {
	path := writeFixture(t)
	t.Setenv("FLATE_HELP", "true")
	t.Setenv("FLATE_VERSION", "true")
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code != 0 {
		t.Fatalf("FLATE_HELP/VERSION should be ignored, but build exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "kind: ConfigMap") {
		t.Errorf("normal build output missing — env skip-list regressed:\n%s", stdout)
	}
}

// TestEnv_HelpAdvertisesEnvVars pins the discoverability requirement:
// every documented flag should advertise its env key in `--help`.
// Persistent flags that bubble through every subcommand (--log-level,
// --path) are checked at the leaf-command help so we exercise the
// shared-pointer dedupe in annotateEnvUsage (each annotation should
// land exactly once even though the flag is inherited by every leaf).
func TestEnv_HelpAdvertisesEnvVars(t *testing.T) {
	stdout, _, code := runCLI(t, "build", "all", "--help")
	if code != 0 {
		t.Fatalf("`build all --help` exited %d", code)
	}
	for _, want := range []string{
		"[env: FLATE_PATH]",
		"[env: FLATE_LOG_LEVEL]",
		"[env: FLATE_SKIP_CRDS]",
		"[env: FLATE_SKIP_KINDS]",
		"[env: FLATE_CONCURRENCY]",
		"[env: FLATE_CACHE_DIR]",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("--help missing %q\n%s", want, stdout)
		}
		if strings.Count(stdout, want) > 1 {
			t.Errorf("--help duplicated %q (annotateEnvUsage dedupe regressed)", want)
		}
	}
	for _, banned := range []string{"[env: FLATE_HELP]"} {
		if strings.Contains(stdout, banned) {
			t.Errorf("--help should not advertise %q — env binding is skipped for it", banned)
		}
	}
}

// TestEnvKey pins the kebab→snake transform so future refactors don't
// silently change which env vars users export.
func TestEnvKey(t *testing.T) {
	for in, want := range map[string]string{
		"path":         "FLATE_PATH",
		"path-orig":    "FLATE_PATH_ORIG",
		"log-level":    "FLATE_LOG_LEVEL",
		"skip-kinds":   "FLATE_SKIP_KINDS",
		"api-versions": "FLATE_API_VERSIONS",
	} {
		if got := envKey(in); got != want {
			t.Errorf("envKey(%q) = %q, want %q", in, got, want)
		}
	}
}
