package e2e

import (
	"strings"
	"testing"
)

// TestE2E_StaleValuesWarning exercises #744 end-to-end: a HelmRelease pins a
// value (`oldUnusedKey`) that no chart template references. The render still
// succeeds (advisory only), and the unused key surfaces in the stderr warnings
// footer attributed to that HR — while a sibling on a chart that consumes
// .Values wholesale (toYaml) stays silent.
func TestE2E_StaleValuesWarning(t *testing.T) {
	stdout, stderr, code := runCLIBuffers("build", "hr", "--path", copyTree(t, testdataPath(t, "stalevalues")))

	if code != 0 {
		t.Fatalf("stale-value detection is advisory and must not fail the render; exit=%d\nstderr:\n%s", code, stderr)
	}
	// The unused key is flagged, attributed to the HR whose chart names its values.
	for _, want := range []string{"warnings", "apps/app", "oldUnusedKey"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q:\n%s", want, stderr)
		}
	}
	// The opaque-chart sibling pins the same key but must NOT warn (we can't
	// prove it unused when the chart reads .Values wholesale).
	if strings.Contains(stderr, "apps/opaque") {
		t.Errorf("a chart that consumes .Values wholesale must not produce a stale-value warning:\n%s", stderr)
	}
	// Referenced values are never reported.
	if strings.Contains(stderr, "greeting") || strings.Contains(stderr, "replicas") {
		t.Errorf("referenced values must not appear in the warnings footer:\n%s", stderr)
	}
	// Rendered output is unaffected — both HRs produce their ConfigMaps.
	if !strings.Contains(stdout, "app-cm") || !strings.Contains(stdout, "opaque-cm") {
		t.Errorf("rendered HR output missing:\n%s", stdout)
	}
}
