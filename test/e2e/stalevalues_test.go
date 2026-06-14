package e2e

import (
	"strings"
	"testing"
)

// TestE2E_StaleValuesWarning exercises #744 end-to-end: a HelmRelease pins a
// value (`oldUnusedKey`) that no chart template references. The render still
// succeeds (advisory only). The advisory surfaces in `flate test` — the
// diagnostic command — attributed to that HR, while `flate build` renders the
// same output but stays quiet: warnings and notes never appear in a build, only
// in test. (The advisory's content — opaque sibling silent, referenced values
// never flagged — is unit-covered by orchestrator.TestRender_StaleValuesWarning.)
func TestE2E_StaleValuesWarning(t *testing.T) {
	path := copyTree(t, testdataPath(t, "stalevalues"))

	// build renders the same output but emits no advisory, on stdout or stderr.
	bOut, bErr, code := runCLIBuffers("build", "hr", "--path", path)
	if code != 0 {
		t.Fatalf("stale-value detection is advisory and must not fail the render; exit=%d\nstderr:\n%s", code, bErr)
	}
	if !strings.Contains(bOut, "app-cm") || !strings.Contains(bOut, "opaque-cm") {
		t.Errorf("rendered HR output missing:\n%s", bOut)
	}
	// The advisory footer lived on stderr; it must be gone. (The rendered
	// opaque-cm legitimately contains "oldUnusedKey" in its dumped values, so
	// only stderr — never stdout — is asserted clean.)
	if strings.Contains(bErr, "warnings") || strings.Contains(bErr, "oldUnusedKey") {
		t.Errorf("build stderr must not surface the stale-value advisory:\n%s", bErr)
	}

	// test surfaces the advisory, attributed to the HR whose chart names its values.
	tOut, tErr, _ := runCLIBuffers("test", "hr", "--path", path)
	for _, want := range []string{"warnings", "apps/app", "oldUnusedKey"} {
		if !strings.Contains(tOut, want) {
			t.Errorf("test stdout missing %q:\n%s\nstderr:\n%s", want, tOut, tErr)
		}
	}
}
