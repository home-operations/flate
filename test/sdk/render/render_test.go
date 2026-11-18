package render_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestExternalModule_Render(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs("../../..")
	assert.NoError(t, err)
	module := t.TempDir()
	fixture, err := os.ReadFile("testdata/client_test.go")
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(module, "client_test.go"), fixture, 0o644))
	assert.NoError(
		t,
		os.WriteFile(
			filepath.Join(module, "go.mod"),
			[]byte(
				"module sdkfixture\n\ngo 1.27.0\n\nrequire github.com/home-operations/flate v0.0.0\n\nreplace github.com/home-operations/flate => "+root+"\n",
			),
			0o644,
		),
	)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-buildvcs=true", "-race", "-mod=mod", "-v", ".")
	cmd.Dir = module
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("external SDK module: %v\n%s", err, output)
	}
	t.Logf("external SDK module:\n%s", output)
}
