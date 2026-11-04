package scope_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ComponentChangePropagatesToAllConsumers(t *testing.T) {
	src := harness.TestdataPath(t, "components")
	orig := t.TempDir()
	testutil.CopyTree(t, src, orig)
	testutil.GitInit(t, orig)
	current := t.TempDir()
	testutil.CopyTree(t, src, current)
	testutil.GitInit(t, current)

	// Mutate the shared component in only the "current" tree.
	harness.MutateFile(
		t,
		filepath.Join(current, "components", "shared", "shared-cm.yaml"),
		"value: original",
		"value: changed",
	)

	// -o github explicitly: this test asserts github diff-syntax markers,
	// independent of the default style.
	out := harness.RunCLI(
		t,
		"diff",
		"ks",
		"--path",
		filepath.Join(current, "cluster"),
		"--path-orig",
		filepath.Join(orig, "cluster"),
		"-o",
		"github",
	)

	// Coverage must propagate to BOTH consumers, so the
	// original→changed transition should surface at least twice (once
	// per consumer Kustomization). dyff prints value changes as bare
	// `- original` / `+ changed` lines under a `@@ data.value @@`
	// path header.
	{
		got := strings.Count(out, "+ changed")
		assert.ErrorIf(
			t,
			got < 2,
			"coverage did not propagate to both consumers (got %d hits, want >= 2):\n%s",
			got,
			out,
		)
	}
	assert.ErrorIf(t, !strings.Contains(out, "- original"), "removal of baseline value missing from diff:\n%s", out)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_NonSharedChangeDoesNotPropagate(t *testing.T) {
	src := harness.TestdataPath(t, "components")
	orig := t.TempDir()
	testutil.CopyTree(t, src, orig)
	testutil.GitInit(t, orig)
	current := t.TempDir()
	testutil.CopyTree(t, src, current)
	testutil.GitInit(t, current)

	harness.MutateFile(t, filepath.Join(current, "apps", "app-a", "cm.yaml"), "app: app-a", "app: app-a-edited")

	out := harness.RunCLI(
		t,
		"diff",
		"ks",
		"--path",
		filepath.Join(current, "cluster"),
		"--path-orig",
		filepath.Join(orig, "cluster"),
	)

	assert.ErrorIf(t, !strings.Contains(out, "app-a-edited"), "expected app-a change to surface in diff:\n%s", out)
	// app-b must be UNTOUCHED — no app-b-cm body should appear in
	// either diff direction.
	assert.ErrorIf(
		t,
		strings.Contains(out, "name: app-b-cm"),
		"app-b leaked into diff for an app-a-only change (over-broad coverage):\n%s",
		out,
	)
}
