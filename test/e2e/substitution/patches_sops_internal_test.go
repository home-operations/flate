package substitution_test

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-global logging.
func TestE2E_SOPSUnreadableActiveSubstitution(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "--concurrency", "2", "-o", "yaml")
	again := harness.RunCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "--concurrency", "2", "-o", "yaml")
	assert.FatalIf(t, again != out, "%s", "SOPS substitution output changed between runs")
	assert.ErrorIf(t, !strings.Contains(out, "secret: null"),
		"expected unavailable MY_SECRET to expand empty during active substitution:\n%s", out)
	assert.ErrorIf(t, !strings.Contains(out, "value: from-cluster-settings"),
		"cleartext cluster-settings value should substitute normally:\n%s", out)
}
