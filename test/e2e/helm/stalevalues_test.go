package helm

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_StaleValuesWarning(t *testing.T) {
	path := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "stalevalues"), path)

	bOut, bErr, code := harness.RunCLIBuffers("build", "hr", "--path", path)
	if code != 0 {
		t.Fatalf("stale-value detection is advisory and must not fail the render; exit=%d\nstderr:\n%s", code, bErr)
	}
	if !strings.Contains(bOut, "app-cm") || !strings.Contains(bOut, "opaque-cm") {
		t.Errorf("rendered HR output missing:\n%s", bOut)
	}
	for _, want := range []string{"[StaleValues]", "apps/app", "oldUnusedKey"} {
		if !strings.Contains(bErr, want) {
			t.Errorf("build stderr missing %q:\n%s", want, bErr)
		}
	}
	if strings.Contains(bOut, "⚠ warnings") || strings.Count(bErr, "values not used by the chart") != 1 {
		t.Errorf("build warning destination/count: stdout=%s stderr=%s", bOut, bErr)
	}

	tOut, tErr, code := harness.RunCLIBuffers("test", "hr", "--path", path)
	if code != 0 {
		t.Fatalf("stale-value detection must remain advisory; exit=%d stdout=%s stderr=%s", code, tOut, tErr)
	}
	for _, want := range []string{"[StaleValues]", "apps/app", "oldUnusedKey"} {
		if !strings.Contains(tErr, want) {
			t.Errorf("test stderr missing %q:\n%s\nstderr:\n%s", want, tOut, tErr)
		}
	}
	if strings.Contains(tOut, "values not used by the chart") || strings.Count(tErr, "values not used by the chart") != 1 {
		t.Errorf("test warning destination/count: stdout=%s stderr=%s", tOut, tErr)
	}
}
