package render_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/mod/modfile"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestExternalModule_Render(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs("../../..")
	assert.NoError(t, err)
	goMod := filepath.Join(root, "go.mod")
	data, err := os.ReadFile(goMod)
	if err != nil {
		t.Fatalf("read repository go.mod: %v", err)
	}
	parsed, err := modfile.Parse(goMod, data, nil)
	if err != nil {
		t.Fatalf("parse repository go.mod: %v", err)
	}
	if parsed.Go == nil {
		t.Fatal("repository go.mod has no Go directive")
	}
	module := t.TempDir()
	fixture, err := os.ReadFile("testdata/client_test.go")
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(module, "client_test.go"), fixture, 0o644))
	assert.NoError(
		t,
		os.WriteFile(
			filepath.Join(module, "go.mod"),
			[]byte(
				"module sdkfixture\n\ngo "+parsed.Go.Version+
					"\n\nrequire github.com/home-operations/flate v0.0.0"+
					"\n\nreplace github.com/home-operations/flate => "+root+"\n",
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
