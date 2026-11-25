package cli

import (
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/testutil/assert"
)

// TestEnv_SliceFlagParsesCSV covers comma-separated parsing for
// pflag.StringSliceVar (`--skip-kinds`). Setting FLATE_SKIP_KINDS to
// "ConfigMap,Secret" should drop the fixture's only ConfigMap from
// the render, leaving an empty stream.
//

func TestEnv_SliceFlagParsesCSV(t *testing.T) {
	t.Run("render", func(t *testing.T) {
		path := writeFixture(t)
		t.Setenv("FLATE_SKIP_KINDS", "ConfigMap,Secret")
		cli := runCLI(t, "build", "all", "--path", path)
		stdout, stderr, code := cli.stdout, cli.stderr, cli.code
		assert.FatalIf(t, code != 0, "build all FLATE_SKIP_KINDS=... exited %d: %s", code, stderr)
		assert.ErrorIf(
			t,
			strings.Contains(stdout, "kind: ConfigMap"),
			"FLATE_SKIP_KINDS=ConfigMap,Secret should have filtered ConfigMap:\n%s",
			stdout,
		)
	})
	for _, tc := range []struct {
		name, env string
		args      []string
		want      []string
		setEnv    bool
		changed   bool
	}{
		{name: "defaults", want: []string{"Secret", "CustomResourceDefinition"}},
		{name: "replacement flag", args: []string{"--skip-kinds", "Job"}, want: []string{"Job"}, changed: true},
		{name: "empty flag", args: []string{"--skip-kinds", ""}, want: []string{}, changed: true},
		{name: "environment", env: "ConfigMap,Secret", setEnv: true, want: []string{"ConfigMap", "Secret"}, changed: true},
		{name: "empty environment", setEnv: true, want: []string{}, changed: true},
		{name: "flag overrides environment", env: "ConfigMap", setEnv: true, args: []string{"--skip-kinds", "Secret"}, want: []string{"Secret"}, changed: true},
		{name: "empty flag overrides environment", env: "ConfigMap", setEnv: true, args: []string{"--skip-kinds", ""}, want: []string{}, changed: true},
		{name: "flag overrides empty environment", setEnv: true, args: []string{"--skip-kinds", "Job"}, want: []string{"Job"}, changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv {
				t.Setenv("FLATE_SKIP_KINDS", tc.env)
			}
			cmd := &cobra.Command{}
			var flags commonFlags
			bindCommon(cmd.Flags(), &flags)
			assert.NoError(t, cmd.Flags().Parse(tc.args))
			assert.NoError(t, applyEnvVars(cmd))
			assert.Diff(t, flags.skipKinds, tc.want)
			assert.Equal(t, cmd.Flags().Changed("skip-kinds"), tc.changed)
		})
	}
}

// TestEnvKey pins the kebab→snake transform so future refactors don't
// silently change which env vars users export.
func TestEnvKey(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"path":         "FLATE_PATH",
		"path-orig":    "FLATE_PATH_ORIG",
		"log-level":    "FLATE_LOG_LEVEL",
		"skip-kinds":   "FLATE_SKIP_KINDS",
		"api-versions": "FLATE_API_VERSIONS",
	} {
		{
			got := envKey(in)
			assert.ErrorIf(t, got != want, "envKey(%q) = %q, want %q", in, got, want)
		}
	}
}

//nolint:paralleltest // CLI invocations call setLogLevel, which replaces slog.Default and log output.
func TestRun_RemovedFlagsRejected(t *testing.T) {
	for _, verb := range []string{"build", "test", "diff", "get"} {
		flags := []string{
			"substitute",
			"no-hooks",
			"show-only",
			"force-generic-provider",
			"is-upgrade",
			"allow-missing-secrets",
			"allow-missing-crds",
			"stream",
			"skip-crds",
			"skip-secrets",
			"only-crds",
			"krmignore",
			"profile",
			"profile-out",
			"skip-schema-validation",
			"source-retry-attempts",
			"source-retry-min-wait",
			"source-retry-max-wait",
			"source-retry-jitter",
			"helm-template-cache-mb",
			"helm-render-cache-mb",
			"kustomize-render-cache-mb",
		}
		if verb != "build" {
			flags = append(flags, "only-kinds")
		}
		for _, flag := range flags {
			t.Run(verb+"/"+flag, func(t *testing.T) {
				cli := runCLI(t, verb, "all", "--"+flag)
				stderr, code := cli.stderr, cli.code
				assert.FatalIf(
					t,
					code != 1 || !strings.Contains(stderr, "unknown flag: --"+flag),
					"removed flag: exit=%d stderr=%s",
					code,
					stderr,
				)
				cli2 := runCLI(t, verb, "all", "--help")
				help, code := cli2.stdout, cli2.code
				assert.FatalIf(
					t,
					code != 0 || strings.Contains(help, "--"+flag) || strings.Contains(help, envKey(flag)),
					"obsolete option advertised: exit=%d help=%s",
					code,
					help,
				)
			})
		}
	}
}

//nolint:paralleltest // CLI invocations call setLogLevel, which replaces slog.Default and log output.
func TestRun_RemovedShorthandRejected(t *testing.T) {
	cli := runCLI(t, "build", "all", "-s", "templates/main.yaml")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "unknown shorthand flag"),
		"removed shorthand: exit=%d stderr=%s",
		code,
		stderr,
	)
}

//nolint:paralleltest // t.Setenv supplies the removed FLATE_* variables for runCLI.
func TestEnv_RemovedOptionsIgnored(t *testing.T) {
	path := writeFixture(t)
	for _, key := range []string{"SUBSTITUTE", "NO_HOOKS", "SHOW_ONLY", "FORCE_GENERIC_PROVIDER", "IS_UPGRADE", "ALLOW_MISSING_SECRETS", "ALLOW_MISSING_CRDS", "STREAM", "SKIP_CRDS", "SKIP_SECRETS", "ONLY_CRDS", "KRMIGNORE", "SKIP_SCHEMA_VALIDATION", "SOURCE_RETRY_ATTEMPTS", "SOURCE_RETRY_MIN_WAIT", "SOURCE_RETRY_MAX_WAIT", "SOURCE_RETRY_JITTER", "HELM_TEMPLATE_CACHE_MB", "HELM_RENDER_CACHE_MB", "KUSTOMIZE_RENDER_CACHE_MB"} {
		t.Setenv("FLATE_"+key, "invalid-value")
	}
	cli := runCLI(t, "build", "all", "--path", path)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 0 || !strings.Contains(stdout, "kind: ConfigMap"),
		"obsolete environment keys affected render: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}

//nolint:paralleltest // t.Setenv changes the process environment.
func TestRun_InvalidEnvFlag(t *testing.T) {
	for _, tc := range []struct{ route, env, value string }{
		{"build ks", "FLATE_ALLOW_MISSING_KINDS", "\""},
		{"build ks", "FLATE_CONCURRENCY", "bad-value"},
		{"build ks", "FLATE_DISABLE_CHART_DIGEST_TRACKING", "bad-value"},
		{"build ks", "FLATE_ENABLE_DNS", "bad-value"},
		{"build ks", "FLATE_GIT_DEPTH", "bad-value"},
		{"build ks", "FLATE_NO_PROGRESS", "bad-value"},
		{"build ks", "FLATE_ONLY_KINDS", "\""},
		{"build ks", "FLATE_RESTRICT_EGRESS", "bad-value"},
		{"build ks", "FLATE_SKIP_KINDS", "\""},
		{"build ks", "FLATE_STRICT_SUBSTITUTIONS", "bad-value"},
		{"diff images", "FLATE_INCLUDE_REMOVED", "bad-value"},
		{"cache gc", "FLATE_DRY_RUN", "bad-value"},
		{"cache gc", "FLATE_MAX_AGE", "bad-value"},
	} {
		t.Run(tc.env, func(t *testing.T) {
			assertInvalidEnvFlag(t, tc.route, tc.env, tc.value)
		})
	}
}

func assertInvalidEnvFlag(t *testing.T, route, env, value string) {
	t.Helper()
	t.Setenv(env, value)
	result := runCLI(t, strings.Fields(route)...)
	assert.Equal(t, result.code, 1)
	assert.Equal(t, result.stdout, "")
	prefix := "flate error: invalid " + env + " " + strconv.Quote(value) + ": "
	cause, ok := strings.CutPrefix(result.stderr, prefix)
	assert.ErrorIf(
		t,
		!ok || strings.TrimSpace(cause) == "",
		"want %q and a parse error, got %q",
		prefix,
		result.stderr,
	)
}
