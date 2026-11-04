package cli

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_UnnamedScope_ErrorBytes(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	root := writeNamedScopeFixture(t)
	want := "reconcile completed with 1 failure(s):\n  Kustomization.kustomize.toolkit.fluxcd.io/flux-system/outside: not found\n  (+1 blocked by failed/missing dependencies)"
	for _, verb := range []string{"get", "test"} {
		t.Run(verb, func(t *testing.T) {
			kind := "ks"
			if verb == "test" {
				kind = "all"
			}
			args := []string{
				verb,
				kind,
				"--path",
				filepath.Join(root, "cluster"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			}
			cli := runCLI(t, args...)
			stderr, code := cli.stderr, cli.code
			assert.Equal(t, code, 1)
			expected := "\n  ✗  Kustomization  flux-system/outside  not found\n         required by flux-system/unrelated\n" + persistenceNotice + "\n  ✗ 1 failed · 1 blocked\n"
			if verb == "test" {
				expected = persistenceNotice
			}
			assert.Equal(t, stderr, expected)
			cmd := New("test")
			cmd.SetContext(t.Context())
			cmd.SetArgs(args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.Execute()
			assert.FatalIf(t, err == nil, "%v", "unnamed failure returned nil")
			expected = want
			if verb == "test" {
				expected = "test failures detected\n" + want
			}
			assert.Equal(t, err.Error(), expected)
		})
	}
}
