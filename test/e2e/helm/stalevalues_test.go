package helm_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_StaleValuesWarning(t *testing.T) {
	path := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "stalevalues"), path)
	testutil.GitInit(t, path)

	runCLIBuffersResult1 := harness.RunCLIBuffers("build", "hr", "--path", filepath.Join(path, "cluster"))
	bOut, bErr, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
	assert.FatalIf(
		t,
		code != 0,
		"stale-value detection is advisory and must not fail the render; exit=%d\nstderr:\n%s",
		code,
		bErr,
	)
	assert.ErrorIf(
		t,
		!strings.Contains(bOut, "app-cm") || !strings.Contains(bOut, "opaque-cm"),
		"rendered HR output missing:\n%s",
		bOut,
	)
	for _, want := range []string{"[StaleValues]", "apps/app", "oldUnusedKey"} {
		assert.ErrorIf(t, !strings.Contains(bErr, want), "build stderr missing %q:\n%s", want, bErr)
	}
	assert.ErrorIf(
		t,
		strings.Contains(bOut, "⚠ warnings") || strings.Count(bErr, "values not used by the chart") != 1,
		"build warning destination/count: stdout=%s stderr=%s",
		bOut,
		bErr,
	)

	runCLIBuffersResult2 := harness.RunCLIBuffers("test", "hr", "--path", filepath.Join(path, "cluster"))
	tOut, tErr, code := runCLIBuffersResult2.Stdout, runCLIBuffersResult2.Stderr, runCLIBuffersResult2.Code
	assert.FatalIf(
		t,
		code != 0,
		"stale-value detection must remain advisory; exit=%d stdout=%s stderr=%s",
		code,
		tOut,
		tErr,
	)
	for _, want := range []string{"[StaleValues]", "apps/app", "oldUnusedKey"} {
		assert.ErrorIf(t, !strings.Contains(tErr, want), "test stderr missing %q:\n%s\nstderr:\n%s", want, tOut, tErr)
	}
	assert.ErrorIf(t, strings.Contains(tOut, "values not used by the chart") ||
		strings.Count(
			tErr,
			"values not used by the chart",
		) != 1, "test warning destination/count: stdout=%s stderr=%s", tOut, tErr)
}
