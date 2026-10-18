// Package harness provides shared in-process CLI and fixture support for e2e scenarios.
package harness

import (
	"bytes"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/cli"
)

// RunCLIBuffers runs the CLI in process and returns separate stdout, stderr and exit status.
func RunCLIBuffers(args ...string) (stdout, stderr string, code int) {
	var out, err bytes.Buffer
	code = cli.Run(args, &out, &err)
	return out.String(), err.String(), code
}

// RequireCLIOK requires a successful CLI exit and returns separate stdout and stderr.
func RequireCLIOK(t *testing.T, args ...string) (stdout, stderr string) {
	t.Helper()
	stdout, stderr, code := RunCLIBuffers(args...)
	if code != 0 {
		t.Fatalf("flate %s exited %d\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "), code, stdout, stderr)
	}
	return stdout, stderr
}

// RunCLI requires a successful CLI exit and returns stdout followed by stderr.
func RunCLI(t *testing.T, args ...string) string {
	t.Helper()
	stdout, stderr := RequireCLIOK(t, args...)
	return stdout + stderr
}

// RunCLIOutputOnly returns stdout followed by stderr regardless of exit status.
func RunCLIOutputOnly(t *testing.T, args ...string) string {
	t.Helper()
	stdout, stderr, _ := RunCLIBuffers(args...)
	return stdout + stderr
}

// RunCLIStdout requires a successful CLI exit and returns only stdout.
func RunCLIStdout(t *testing.T, args ...string) string {
	t.Helper()
	stdout, _ := RequireCLIOK(t, args...)
	return stdout
}

// RunCLIExpectErr returns stdout followed by stderr and the CLI exit status.
func RunCLIExpectErr(t *testing.T, args ...string) (string, int) {
	t.Helper()
	stdout, stderr, code := RunCLIBuffers(args...)
	return stdout + stderr, code
}
