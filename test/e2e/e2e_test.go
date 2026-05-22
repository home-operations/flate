// Package e2e exercises the flate CLI against the testdata fixtures.
//
// Tests run the cobra command tree in-process via cli.Run, capturing
// stdout/stderr into byte buffers. There is no fork/exec: the entire
// program is exercised inside the test binary, which is faster, more
// reliable, and avoids requiring a freshly built binary on disk.
package e2e

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/cli"
)

func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.Run(args, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("flate %s exited %d\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "), code, stdout.String(), stderr.String())
	}
	return stdout.String() + stderr.String()
}

// runCLIStdout returns stdout only — log lines on stderr would
// otherwise pollute payloads that tests parse.
func runCLIStdout(t *testing.T, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.Run(args, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("flate %s exited %d\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "), code, stdout.String(), stderr.String())
	}
	return stdout.String()
}

func runCLIExpectErr(t *testing.T, args ...string) (string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.Run(args, &stdout, &stderr)
	return stdout.String() + stderr.String(), code
}

func testdataPath(t *testing.T, sub string) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for cur := wd; cur != "/" && cur != "."; cur = filepath.Dir(cur) {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return filepath.Join(cur, "testdata", sub)
		}
	}
	t.Fatal("could not locate repo root from " + wd)
	return ""
}

func TestE2E_GetKS(t *testing.T) {
	out := runCLI(t, "get", "ks", "--path", testdataPath(t, "simple"))
	if !strings.Contains(out, "NAMESPACE") || !strings.Contains(out, "NAME") {
		t.Errorf("missing table headers:\n%s", out)
	}
	if !strings.Contains(out, "apps") {
		t.Errorf("expected 'apps' kustomization in output:\n%s", out)
	}
}

func TestE2E_BuildKS(t *testing.T) {
	out := runCLI(t, "build", "ks", "--path", copyTree(t, testdataPath(t, "simple")))
	if !strings.Contains(out, "kind: ConfigMap") {
		t.Errorf("missing ConfigMap:\n%s", out)
	}
	if !strings.Contains(out, "greeting: hello-from-flate") {
		t.Errorf("missing expected value:\n%s", out)
	}
}

func TestE2E_BuildHR(t *testing.T) {
	out := runCLI(t, "build", "hr", "--path", copyTree(t, testdataPath(t, "simple")))
	if !strings.Contains(out, "demo-cm") {
		t.Errorf("missing rendered HR ConfigMap:\n%s", out)
	}
	if !strings.Contains(out, "greeting: hello-from-helm") {
		t.Errorf("values not applied:\n%s", out)
	}
}

func TestE2E_GetAll_JSON(t *testing.T) {
	out := runCLI(t, "get", "all", "--path", testdataPath(t, "simple"), "-o", "json")
	if !strings.Contains(out, `"kustomizations"`) {
		t.Errorf("missing kustomizations in json:\n%s", out)
	}
}

func TestE2E_DiagOK(t *testing.T) {
	out := runCLI(t, "diag", "--path", testdataPath(t, "simple"))
	if !strings.Contains(out, "DIAGNOSTICS OK") {
		t.Errorf("expected OK marker:\n%s", out)
	}
}

func TestE2E_Help(t *testing.T) {
	out := runCLI(t, "--help")
	for _, want := range []string{"build", "diff", "get", "test", "diag"} {
		if !strings.Contains(out, want) {
			t.Errorf("help missing %q:\n%s", want, out)
		}
	}
}

func TestE2E_BadPath(t *testing.T) {
	_, code := runCLIExpectErr(t, "get", "ks", "--path", "/nonexistent-12345")
	if code == 0 {
		t.Errorf("expected non-zero exit for bad path")
	}
}

func TestE2E_TestCommand(t *testing.T) {
	out := runCLI(t, "test", "all", "--path", copyTree(t, testdataPath(t, "simple")))
	if !strings.Contains(out, "PASSED") {
		t.Errorf("expected PASSED in test output:\n%s", out)
	}
}

func TestE2E_DiffNoChange(t *testing.T) {
	p := testdataPath(t, "simple")
	out := runCLI(t, "diff", "ks", "--path", p, "--path-orig", p)
	// No diffs expected — output should be empty or near-empty.
	if strings.Contains(out, "---") && strings.Contains(out, "+++") &&
		strings.Contains(out, "@@") {
		t.Errorf("unexpected diff content for identical paths:\n%s", out)
	}
}

func TestE2E_DiffImagesNoChange(t *testing.T) {
	p := testdataPath(t, "simple")
	out := runCLIStdout(t, "diff", "images", "--path", p, "--path-orig", p, "-o", "json")
	got := strings.TrimSpace(out)
	if got != "[]" && got != "null" {
		t.Errorf("expected empty image diff for identical paths, got: %q", got)
	}
}

func TestE2E_DiffImagesRequiresPathOrig(t *testing.T) {
	out, code := runCLIExpectErr(t, "diff", "images", "--path", testdataPath(t, "simple"))
	if code == 0 {
		t.Fatalf("expected non-zero exit when --path-orig is missing, got 0:\n%s", out)
	}
	if !strings.Contains(out, "--path-orig") {
		t.Errorf("error should mention --path-orig:\n%s", out)
	}
}

// TestE2E_ComponentChangePropagatesToAllConsumers — the fixture has
// two apps (app-a, app-b) consuming components/shared; mutating the
// shared component must show up in both consumers' diffs.
func TestE2E_ComponentChangePropagatesToAllConsumers(t *testing.T) {
	src := testdataPath(t, "components")
	orig := copyTree(t, src)
	current := copyTree(t, src)

	// Mutate the shared component in only the "current" tree.
	target := filepath.Join(current, "components", "shared", "shared-cm.yaml")
	data, err := os.ReadFile(target) //nolint:gosec // target is inside a t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(data), "value: original", "value: changed", 1)
	if mutated == string(data) {
		t.Fatal("fixture sentinel not found; check shared-cm.yaml")
	}
	if err := os.WriteFile(target, []byte(mutated), 0o600); err != nil { //nolint:gosec // target is inside a t.TempDir()
		t.Fatal(err)
	}

	out := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig)

	// Coverage must propagate to BOTH consumers, so the
	// original→changed transition should surface at least twice (once
	// per consumer Kustomization).
	if got := strings.Count(out, "+  value: changed"); got < 2 {
		t.Errorf("coverage did not propagate to both consumers (got %d hits, want >= 2):\n%s", got, out)
	}
	if !strings.Contains(out, "-  value: original") {
		t.Errorf("removal of baseline value missing from diff:\n%s", out)
	}
}

func TestE2E_NonSharedChangeDoesNotPropagate(t *testing.T) {
	src := testdataPath(t, "components")
	orig := copyTree(t, src)
	current := copyTree(t, src)

	target := filepath.Join(current, "apps", "app-a", "cm.yaml")
	data, err := os.ReadFile(target) //nolint:gosec // target is inside a t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(data), "app: app-a", "app: app-a-edited", 1)
	if mutated == string(data) {
		t.Fatal("fixture sentinel not found")
	}
	if err := os.WriteFile(target, []byte(mutated), 0o600); err != nil { //nolint:gosec // target is inside a t.TempDir()
		t.Fatal(err)
	}

	out := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig)

	if !strings.Contains(out, "app-a-edited") {
		t.Errorf("expected app-a change to surface in diff:\n%s", out)
	}
	// app-b must be UNTOUCHED — no app-b-cm body should appear in
	// either diff direction.
	if strings.Contains(out, "name: app-b-cm") {
		t.Errorf("app-b leaked into diff for an app-a-only change (over-broad coverage):\n%s", out)
	}
}

// TestE2E_ParentPatchesPropagateToChildren reproduces the bjw-s
// home-ops cluster-apps pattern: a top-level Flux Kustomization with
// spec.patches that inject postBuild.substituteFrom into every child
// Kustomization. Children carry only inline substitute.* — they rely
// on the parent's patches to wire the ConfigMap reference at render
// time. The fixture's leaf KS references ${MY_VAR}, which only
// resolves if the parent's render-emitted (patched) child KS
// supersedes the statically-loaded one and triggers a fresh reconcile.
//
// A SOPS-encrypted Secret in the same parent kustomize tree exercises
// the secondary fix: render must continue past the SOPS doc so the
// (non-SOPS) patched children still reach the store.
func TestE2E_ParentPatchesPropagateToChildren(t *testing.T) {
	src := testdataPath(t, "parent-patches")
	root := copyTree(t, src)

	// `flate test ks` exits non-zero because cluster-apps reports the
	// SOPS Secret it could not decrypt — that's the intended fail-loud.
	// What matters here is that leaf still PASSED.
	out, _ := runCLIExpectErr(t, "test", "ks", "--path", root)

	if !strings.Contains(out, "Kustomization/flux-system/leaf") {
		t.Fatalf("leaf KS missing from output:\n%s", out)
	}
	leafLine := mustExtractLine(t, out, "Kustomization/flux-system/leaf")
	if !strings.Contains(leafLine, "PASSED") {
		t.Errorf("leaf should pass once parent patches inject substituteFrom; got: %s", leafLine)
	}
	if strings.Contains(out, `variable "MY_VAR" is undefined`) {
		t.Errorf("MY_VAR should be resolved via parent-injected substituteFrom:\n%s", out)
	}
	// cluster-apps itself stays FAILED so users see the SOPS warning,
	// but the failure mode is the collected-and-reported one, not the
	// early-abort that previously dropped all other rendered docs.
	if !strings.Contains(out, "SOPS-encrypted resource(s)") {
		t.Errorf("cluster-apps should still surface a SOPS warning:\n%s", out)
	}
}

func mustExtractLine(t *testing.T, haystack, needle string) string {
	t.Helper()
	for _, line := range strings.Split(haystack, "\n") {
		if strings.Contains(line, needle) && (strings.Contains(line, "PASSED") || strings.Contains(line, "FAILED")) {
			return line
		}
	}
	t.Fatalf("status line for %q not found in:\n%s", needle, haystack)
	return ""
}

// copyTree shallow-copies src into a fresh tempdir, preserving the
// relative path layout. Used so each test can mutate its own copy.
func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		out := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(out, 0o750)
		}
		data, err := os.ReadFile(p) //nolint:gosec // p is supplied by filepath.Walk over a known root
		if err != nil {
			return err
		}
		return os.WriteFile(out, data, 0o600) //nolint:gosec // dst is t.TempDir()
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}
