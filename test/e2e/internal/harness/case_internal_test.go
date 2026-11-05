package harness

import (
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testfixture"
)

func TestTestDuration_Grammar(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, args, input, want string }{
		{"passed", "test", "  ✓ 2 passed  1.2ms\n", "  ✓ 2 passed  <DUR>\n"},
		{"failed", "test", "✗ 1 passed · 2 failed 1h2m3.4s", "✗ 1 passed · 2 failed <DUR>"},
		{"skipped blocked", "test", "✓ 0 passed · 3 skipped · 4 blocked  15µs\n", "✓ 0 passed · 3 skipped · 4 blocked  <DUR>\n"},
		{"units", "test", "✓ 3 passed  2h\n✓ 4 passed  3m\n✓ 5 passed  4ns\n✓ 6 passed  5us\n", "✓ 3 passed  <DUR>\n✓ 4 passed  <DUR>\n✓ 5 passed  <DUR>\n✓ 6 passed  <DUR>\n"},
		{"build", "build", "✓ 1 passed 1s\n", "✓ 1 passed 1s\n"},
		{"diff", "diff", "✓ 1 passed 1s\n", "✓ 1 passed 1s\n"},
		{"near matches", "test", "prefix ✓ 1 passed 1s\n✓ 1 passed 1s trailing\n✓ 1 passed\n✓ 1 passed .2s\nvalue: 1s\nretry-after: 2s\n\x1b[31m✓ 1 passed 1s\x1b[0m\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := tc.want
			if tc.name == "near matches" {
				want = tc.input
			}
			if got := testDuration(strings.Fields(tc.args), tc.input); got != want {
				t.Fatalf("want %q, got %q", want, got)
			}
		})
	}
}

// The case changes the process environment, directory, logging and transports.
func TestCase_RestoresProcessState(t *testing.T) {
	t.Setenv("FLATE_PATH", "/inherited")
	t.Setenv("AWS_ACCESS_KEY_ID", "credential-canary")
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	logger, transport := slog.Default(), http.DefaultTransport
	t.Run("case", func(t *testing.T) {
		fixture := Open(t, "entry-root", "literal-app")
		if os.Getenv("FLATE_PATH") != "" || os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			t.Fatal("case inherited flags or credentials")
		}
		fixture.fixture.Plan.Directory = "@@repo@@"
		want := fixture.Want(t)
		for range 2 {
			if got := fixture.Run(t); !slices.Equal(got, want) {
				t.Fatalf("want %#v, got %#v", want, got)
			}
			current, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			if current != directory || slog.Default() != logger {
				t.Fatal("invocation leaked directory or logging state")
			}
		}
	})
	if os.Getenv("FLATE_PATH") != "/inherited" || os.Getenv("AWS_ACCESS_KEY_ID") != "credential-canary" ||
		http.DefaultTransport != transport {
		t.Fatal("case leaked environment or transport state")
	}
}

//nolint:paralleltest // CLI invocations change process logging and environment.
func TestCase_ContinuesAfterFailure(t *testing.T) {
	fixture := Open(t, "entry-root", "literal-app")
	want := fixture.Want(t)
	step := fixture.fixture.Plan.Steps[0]
	failed := *step.Invoke
	failed.Argv = []string{"unknown-fixture-command"}
	first := step
	first.ID, first.Invoke = "failed", &failed
	fixture.fixture.Plan.Steps = append([]testfixture.Step{first}, step)
	got := fixture.Run(t)
	if len(got) != 2 || got[0].Step != "failed" || got[0].Code != 1 || got[1] != want[0] {
		t.Fatalf("failed invocation suppressed or changed later results: %#v", got)
	}
}
