package orchestrator

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestComputeChangeSet_FluxExcludedWarning(t *testing.T) {
	t.Setenv("PATH", "")
	before, after := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, before, ".sops.yaml", "old")
	testutil.WriteFile(t, after, ".sops.yaml", "new")
	o := &Orchestrator{cfg: Config{PathOrig: before}, store: store.New()}
	got, err := o.computeChangeSet(after)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, got.Len(), 0)
	warnings := o.store.Warnings()
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one", warnings)
	}
	assert.Equal(t, warnings[0].Category, manifest.WarnPathConfig)
	if !strings.Contains(warnings[0].Message, "Flux-excluded files are not counted") {
		t.Errorf("warning does not explain Flux exclusions: %q", warnings[0].Message)
	}
}
