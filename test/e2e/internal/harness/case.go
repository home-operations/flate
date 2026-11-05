package harness

import (
	"bytes"
	"log" //nolint:depguard // Restoring slog.SetDefault also requires restoring the standard logger writer.
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/cli"
	"github.com/home-operations/flate/internal/testfixture"
)

// Result retains output and exit status for each invocation independently.
type Result struct {
	Step   string
	Stdout string
	Stderr string
	Code   int
}

// Case shares one materialization across its ordered invocation steps.
type Case struct {
	fixture *testfixture.Fixture
}

// Open isolates process state before any fixture invokes the CLI.
func Open(t *testing.T, scenario, caseName string) *Case {
	t.Helper()
	f := testfixture.Materialize(t, scenario, caseName)
	isolate(t, f)
	serve(t, f)
	return &Case{fixture: f}
}

// Run restores fixture trees and applies the declared cache policy on every replay.
func (c *Case) Run(t *testing.T) []Result {
	t.Helper()
	f := c.fixture
	f.Restore(t)
	results := make([]Result, 0, len(f.Plan.Steps))
	for _, step := range f.Plan.Steps {
		if step.Edit != nil {
			f.Edit(t, step)
			continue
		}
		t.Run(step.ID, func(t *testing.T) {
			inv := step.Invoke
			cache := f.Cache(t, inv)
			if f.Plan.Directory != "" {
				t.Chdir(f.Resolve(t, f.Plan.Directory, cache))
			}
			for _, env := range []map[string]string{f.Plan.Env, inv.Env} {
				for key, value := range env {
					t.Setenv(key, f.Resolve(t, value, cache))
				}
			}
			args := make([]string, len(inv.Argv))
			for i, arg := range inv.Argv {
				args[i] = f.Resolve(t, arg, cache)
			}
			previous, writer := slog.Default(), log.Writer()
			defer func() { slog.SetDefault(previous); log.SetOutput(writer) }()
			var stdout, stderr bytes.Buffer
			code := cli.Run(args, &stdout, &stderr)
			results = append(
				results,
				Result{
					Step:   step.ID,
					Stdout: testDuration(args, stdout.String()),
					Stderr: testDuration(args, stderr.String()),
					Code:   code,
				},
			)
		})
	}
	return results
}

// Want reads expectations without changing materialized state or caches.
func (c *Case) Want(t *testing.T) []Result {
	t.Helper()
	f := c.fixture
	want := make([]Result, 0, len(f.Plan.Steps))
	for _, step := range f.Plan.Steps {
		if step.Invoke == nil {
			continue
		}
		inv := step.Invoke
		// Want must not reset a cache or change the materialized case.
		name := inv.Cache.Name
		if name == "" {
			name = "default"
		}
		cache := f.CachePath(name)
		want = append(
			want,
			Result{
				Step:   step.ID,
				Stdout: f.Read(t, inv.Expect.Stdout, cache),
				Stderr: f.Read(t, inv.Expect.Stderr, cache),
				Code:   *inv.Expect.Code,
			},
		)
	}
	return want
}

func testDuration(args []string, stream string) string {
	if len(args) == 0 || args[0] != "test" {
		return stream
	}
	// Keep this grammar identical to corpus-parity.sh:normalize's durations expression.
	pattern := regexp.MustCompile(
		`^([[:space:]]*(✓|✗) [0-9]+ passed( · [0-9]+ (skipped|failed|blocked))*[[:space:]]+)([0-9]+h)?([0-9]+m)?[0-9]+(\.[0-9]+)?(ns|µs|us|ms|s|m|h)$`,
	)
	lines := strings.Split(stream, "\n")
	for i, line := range lines {
		lines[i] = pattern.ReplaceAllString(line, `${1}<DUR>`)
	}
	return strings.Join(lines, "\n")
}

func isolate(t *testing.T, f *testfixture.Fixture) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if isolatedKey(key) {
			t.Setenv(key, "")
		}
	}
	for _, key := range []string{"HOME", "USERPROFILE", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE", "DOCKER_CONFIG", "HELM_CONFIG_HOME", "HELM_CACHE_HOME", "HELM_DATA_HOME"} {
		t.Setenv(key, f.Home)
	}
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
}

func isolatedKey(key string) bool {
	for _, prefix := range []string{"FLATE_", "AWS_", "AZURE_", "GOOGLE_", "GITHUB_", "GH_", "HELM_", "DOCKER_", "GIT_"} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	switch strings.ToUpper(key) {
	case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "KUBECONFIG", "SSL_CERT_FILE", "SSL_CERT_DIR":
		return true
	default:
		return false
	}
}
