package cache

import (
	"errors"
	"io/fs"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestMachineCredentialErr_Idempotent(t *testing.T) {
	t.Parallel()
	assert.FatalIf(t, MachineCredentialErr(nil) != nil || CredentialsUnavailable(nil), "%s", "nil became a failure")

	cause := &fs.PathError{Op: "open", Path: "credential-canary", Err: fs.ErrPermission}
	machine := MachineCredentialErr(cause)
	terminal := &TerminalError{Err: machine, Attempted: true}
	got := MachineCredentialErr(terminal)
	assert.FatalIf(t, !errors.Is(got, terminal) || !errors.Is(got, cause) ||
		strings.Contains(got.Error(), "canary"), "classification lost terminal or typed cause: %v", got)
}
