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

// runCLIOutputOnly captures stdout+stderr regardless of exit code.
// Use for tests that exercise the CLI's *output shape* against partial
// fixtures where reconcile is intentionally incomplete (so the CLI's
// new non-zero-on-failure behavior would otherwise mask the assertion
// the test actually cares about — that get/diff/build produce the
// expected structure on whatever did render). Tests that NEED a clean
// reconcile should keep using runCLI.
func runCLIOutputOnly(t *testing.T, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cli.Run(args, &stdout, &stderr)
	return stdout.String() + stderr.String()
}

// runCLIStdoutOnly is the stdout variant of runCLIOutputOnly.
func runCLIStdoutOnly(t *testing.T, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cli.Run(args, &stdout, &stderr)
	return stdout.String()
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
	out := runCLIOutputOnly(t, "get", "ks", "--path", testdataPath(t, "simple"))
	if !strings.Contains(out, "NAMESPACE") || !strings.Contains(out, "NAME") {
		t.Errorf("missing table headers:\n%s", out)
	}
	if !strings.Contains(out, "apps") {
		t.Errorf("expected 'apps' kustomization in output:\n%s", out)
	}
}

func TestE2E_ResourceSetExpandsIntoChildKustomizations(t *testing.T) {
	// The resourceset/ fixture has one ResourceSet that templates a
	// Namespace + a Kustomization per tenant (frontend, backend). After
	// loading, flate should expose the rendered child Kustomizations
	// via `get ks` alongside the static parent — confirming the
	// load-time expansion pass plumbed each rendered doc through the
	// store.
	out := runCLIOutputOnly(t, "get", "ks", "--path", testdataPath(t, "resourceset"))
	for _, want := range []string{
		"cluster-tenants", // parent KS from cluster/flux-system.yaml
		"apps-frontend",   // emitted by ResourceSet for tenant=frontend
		"apps-backend",    // emitted by ResourceSet for tenant=backend
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in ks listing:\n%s", want, out)
		}
	}
}

func TestE2E_GetKS_YAMLExposesProjectedFields(t *testing.T) {
	out := runCLIOutputOnly(t, "get", "ks", "--path", testdataPath(t, "simple"), "-o", "yaml")
	// Asserts the structured projection includes the new fields:
	// sourceRef block (kind/name/namespace), prune, wait.
	for _, want := range []string{"sourceRef:", "kind: GitRepository", "prune: true", "wait:"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in projected ks YAML:\n%s", want, out)
		}
	}
}

func TestE2E_GetHR_YAMLExposesProjectedFields(t *testing.T) {
	out := runCLIOutputOnly(t, "get", "hr", "--path", testdataPath(t, "simple"), "-o", "yaml")
	// HelmRelease projection should carry sourceRef (chart's resolved
	// ref) and releaseName.
	for _, want := range []string{"sourceRef:", "releaseName:"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in projected hr YAML:\n%s", want, out)
		}
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

// build hr emits identical bytes on repeated runs against the same
// tree. Pins the per-artifact sort that's needed because some Helm
// charts use `range $name, $v := .Values.*` which iterates Go maps
// randomly — without the sort, byte-stable diffs in CI would break.
func TestE2E_BuildHR_Deterministic(t *testing.T) {
	dir := copyTree(t, testdataPath(t, "simple"))
	out1 := runCLIStdout(t, "build", "hr", "--path", dir)
	out2 := runCLIStdout(t, "build", "hr", "--path", dir)
	if out1 != out2 {
		t.Errorf("build hr output differs between runs (length %d vs %d)", len(out1), len(out2))
	}
}

func TestE2E_GetAll_JSON(t *testing.T) {
	out := runCLIOutputOnly(t, "get", "all", "--path", testdataPath(t, "simple"), "-o", "json")
	if !strings.Contains(out, `"kustomizations"`) {
		t.Errorf("missing kustomizations in json:\n%s", out)
	}
}

func TestE2E_Help(t *testing.T) {
	out := runCLI(t, "--help")
	for _, want := range []string{"build", "diff", "get", "test"} {
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
	out := runCLIOutputOnly(t, "diff", "ks", "--path", p, "--path-orig", p)
	// No diffs expected — output should be empty or near-empty.
	if strings.Contains(out, "---") && strings.Contains(out, "+++") &&
		strings.Contains(out, "@@") {
		t.Errorf("unexpected diff content for identical paths:\n%s", out)
	}
}

func TestE2E_DiffImagesNoChange(t *testing.T) {
	p := testdataPath(t, "simple")
	out := runCLIStdoutOnly(t, "diff", "images", "--path", p, "--path-orig", p, "-o", "json")
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
// on the parent's patches to wire the ConfigMap and Secret references
// at render time.
//
//   - leaf references ${MY_VAR} (from cluster-settings ConfigMap),
//     proving that parent-injected substituteFrom reaches the child.
//   - leaf references ${MY_SECRET} (from the SOPS-encrypted
//     cluster-secrets Secret), proving that SOPS-encrypted values are
//     wiped to PLACEHOLDER instead of aborting the run.
func TestE2E_ParentPatchesPropagateToChildren(t *testing.T) {
	src := testdataPath(t, "parent-patches")
	root := copyTree(t, src)

	out := runCLI(t, "test", "ks", "--path", root)

	leafLine := mustExtractLine(t, out, "Kustomization/flux-system/leaf")
	if !strings.Contains(leafLine, "PASSED") {
		t.Errorf("leaf should pass once parent patches inject substituteFrom; got: %s", leafLine)
	}
	clusterLine := mustExtractLine(t, out, "Kustomization/flux-system/cluster-apps")
	if !strings.Contains(clusterLine, "PASSED") {
		t.Errorf("cluster-apps should pass; SOPS values are wiped to placeholder, not fail-loud; got: %s", clusterLine)
	}
	if strings.Contains(out, `is undefined and has no default`) {
		t.Errorf("no postBuild variable should be reported undefined:\n%s", out)
	}
}

// TestE2E_SOPSValueWipedToPlaceholder verifies the rendered output
// actually contains the PLACEHOLDER token where a SOPS-encrypted value
// would have lived. Without the wipe-to-placeholder behavior, this
// substitution would fail the run entirely.
func TestE2E_SOPSValueWipedToPlaceholder(t *testing.T) {
	src := testdataPath(t, "parent-patches")
	root := copyTree(t, src)

	out := runCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "-o", "yaml")

	if !strings.Contains(out, "secret: ..PLACEHOLDER_MY_SECRET..") {
		t.Errorf("expected SOPS-derived MY_SECRET to substitute to its placeholder:\n%s", out)
	}
	if !strings.Contains(out, "value: from-cluster-settings") {
		t.Errorf("cleartext cluster-settings value should substitute normally:\n%s", out)
	}
}

// TestE2E_OrphanedKustomizationIsWarning verifies that a ks.yaml
// living under another Kustomization's spec.path but NOT listed in
// any parent kustomization.yaml is treated as an orphan: the
// reconcile warning is surfaced, the orphan is marked Ready, and
// the overall test does not fail. Mirrors how Flux behaves — Flux
// never sees orphaned files because they aren't in the kustomize
// build output.
func TestE2E_OrphanedKustomizationIsWarning(t *testing.T) {
	src := testdataPath(t, "orphans")
	root := copyTree(t, src)

	out := runCLI(t, "test", "ks", "--path", root)

	// "wired" is referenced and should pass.
	wiredLine := mustExtractLine(t, out, "Kustomization/flux-system/wired")
	if !strings.Contains(wiredLine, "PASSED") {
		t.Errorf("wired should pass: %s", wiredLine)
	}
	// "orphan" should also report PASSED (downgraded), not FAILED.
	orphanLine := mustExtractLine(t, out, "Kustomization/flux-system/orphan")
	if !strings.Contains(orphanLine, "PASSED") {
		t.Errorf("orphan should be downgraded to PASSED: %s", orphanLine)
	}
	// The orphan must surface as a warning so users see the issue.
	if !strings.Contains(out, "resource orphaned") {
		t.Errorf(`expected "resource orphaned" warning in log:\n%s`, out)
	}
}

// TestE2E_SubstituteDisabledAnnotation reproduces the Flux opt-out
// pattern: a ConfigMap embedding a shell script with bash array
// expansions (${ARR[@]}) that the envsubst parser can't handle is
// flagged with kustomize.toolkit.fluxcd.io/substitute: disabled,
// instructing flate (and Flux) to skip substitution on that
// resource. Without this opt-out, envsubst would fail with
// "missing closing brace" and abort the whole Kustomization.
func TestE2E_SubstituteDisabledAnnotation(t *testing.T) {
	src := testdataPath(t, "parent-patches")
	root := copyTree(t, src)

	out := runCLI(t, "test", "ks", "--path", root)

	leafLine := mustExtractLine(t, out, "Kustomization/flux-system/leaf")
	if !strings.Contains(leafLine, "PASSED") {
		t.Errorf("leaf should pass — the script ConfigMap opts out of substitution; got: %s", leafLine)
	}
	if strings.Contains(out, "missing closing brace") {
		t.Errorf("substitute-disabled annotation should prevent the parse error:\n%s", out)
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
