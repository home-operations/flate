// Package harness provides shared in-process CLI and fixture support for e2e scenarios.
package harness

import (
	"bytes"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/cli"
)

type cliResult struct {
	Stdout string
	Stderr string
	Code   int
}

// RunCLIBuffers runs the CLI in process and returns separate stdout, stderr and exit status.
func RunCLIBuffers(args ...string) cliResult {
	var code int

	var out, err bytes.Buffer
	code = cli.Run(args, &out, &err)
	return cliResult{Stdout: out.String(), Stderr: err.String(), Code: code}
}

type cliOutput struct {
	Stdout string
	Stderr string
}

// RequireCLIOK requires a successful CLI exit and returns separate stdout and stderr.
func RequireCLIOK(t *testing.T, args ...string) cliOutput {
	t.Helper()
	var stdout string
	var stderr string
	runCLIBuffersResult1 := RunCLIBuffers(args...)
	stdout, stderr, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
	if code != 0 {
		t.Fatalf("flate %s exited %d\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "), code, stdout, stderr)
	}
	return cliOutput{Stdout: stdout, Stderr: stderr}
}

// RunCLIStdout requires a successful CLI exit and returns only stdout.
func RunCLIStdout(t *testing.T, args ...string) string {
	t.Helper()
	requireCLIOKResult4 := RequireCLIOK(t, args...)
	stdout, _ := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
	return stdout
}
