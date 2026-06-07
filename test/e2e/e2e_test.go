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
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/cli"
	"github.com/home-operations/flate/internal/testutil"
)

// runCLIBuffers runs the CLI in-process and returns its captured stdout,
// stderr, and exit code. All the runCLI* helpers build on it.
func runCLIBuffers(args ...string) (stdout, stderr string, code int) {
	var out, err bytes.Buffer
	code = cli.Run(args, &out, &err)
	return out.String(), err.String(), code
}

// requireCLIOK runs the CLI and fails the test on a non-zero exit code,
// returning the captured stdout and stderr separately.
func requireCLIOK(t *testing.T, args ...string) (stdout, stderr string) {
	t.Helper()
	stdout, stderr, code := runCLIBuffers(args...)
	if code != 0 {
		t.Fatalf("flate %s exited %d\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "), code, stdout, stderr)
	}
	return stdout, stderr
}

func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	stdout, stderr := requireCLIOK(t, args...)
	return stdout + stderr
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
	stdout, stderr, _ := runCLIBuffers(args...)
	return stdout + stderr
}

// runCLIStdoutOnly is the stdout variant of runCLIOutputOnly.
func runCLIStdoutOnly(t *testing.T, args ...string) string {
	t.Helper()
	stdout, _, _ := runCLIBuffers(args...)
	return stdout
}

// runCLIStdout returns stdout only — log lines on stderr would
// otherwise pollute payloads that tests parse.
func runCLIStdout(t *testing.T, args ...string) string {
	t.Helper()
	stdout, _ := requireCLIOK(t, args...)
	return stdout
}

func runCLIExpectErr(t *testing.T, args ...string) (string, int) {
	t.Helper()
	stdout, stderr, code := runCLIBuffers(args...)
	return stdout + stderr, code
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

// TestE2E_DiffAutoBaseline_Base pins the --base=<rev> escape hatch
// end-to-end through the CLI: init a real git repo, commit a fixture,
// modify the working tree, then `flate diff ks --path <dir> --base
// HEAD` and assert the diff surfaces the edit. Exercises the full
// auto-baseline plumbing (materialize → synthetic --path-orig →
// orchestrator → diff) without needing a configured upstream.
func TestE2E_DiffAutoBaseline_Base(t *testing.T) {
	clusterPath, repoRoot := initGitFixture(t)
	// Edit cm.yaml in the working tree; HEAD still has the original.
	cm := filepath.Join(repoRoot, "kubernetes", "apps", "cm.yaml")
	body, err := os.ReadFile(cm) //nolint:gosec // dir is t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(body), "value: original", "value: changed", 1)
	if mutated == string(body) {
		t.Fatal("sentinel 'value: original' not found in fixture")
	}
	if err := os.WriteFile(cm, []byte(mutated), 0o600); err != nil { //nolint:gosec // dir is t.TempDir()
		t.Fatal(err)
	}
	out := runCLI(t, "diff", "ks", "--path", clusterPath, "--base", "HEAD")
	if !strings.Contains(out, "changed") {
		t.Errorf("expected 'changed' to surface in diff body:\n%s", out)
	}
	if !strings.Contains(out, "original") {
		t.Errorf("expected baseline 'original' to surface in diff body:\n%s", out)
	}
}

// TestE2E_DiffSelfRefBaselineRenders is the regression guard for the
// changed-only baseline self-alias bug (TalosCluster PR #5386). The flux
// source is a GitRepository named "cluster" — not the bootstrap "flux-
// system" anchor — that is self-referential (its URL matches the repo's own
// remote) and gated behind a missing deploy-key secret, so it resolves only
// through the URL-match self-alias. The baseline render must alias it (using
// the working tree's remotes, even though the materialized baseline tree has
// none), or the whole tree renders empty and every resource shows as a
// wholesale addition instead of the one-line changeset.
func TestE2E_DiffSelfRefBaselineRenders(t *testing.T) {
	repoRoot := initSelfRefGitFixture(t)
	testutil.WriteFileAt(t, filepath.Join(repoRoot, "k8s", "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: changed
`)
	// Isolated cache so the baseline is freshly materialized (the fixture's
	// deterministic commit SHA would otherwise reuse a cached baseline tree).
	out := runCLI(t, "diff", "ks", "--path", repoRoot, "--base", "HEAD",
		"--allow-missing-secrets", "--cache-dir", t.TempDir())
	// The baseline must have rendered the old value, so the diff is a value
	// change. With the bug the baseline is empty and `original` never
	// appears — the whole ConfigMap shows as a wholesale addition.
	if !strings.Contains(out, "original") {
		t.Errorf("baseline rendered empty (whole tree shown as additions, not the changeset):\n%s", out)
	}
	if !strings.Contains(out, "changed") {
		t.Errorf("expected the changed value to surface:\n%s", out)
	}
}

// initSelfRefGitFixture builds a git repo with an origin remote and a flux
// entrypoint sourced from a self-referential GitRepository named "cluster"
// (gated behind a missing secret). Returns the repo root to use as --path.
func initSelfRefGitFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	repo, err := gogit.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin", URLs: []string{"https://github.com/example/cluster.git"},
	}); err != nil {
		t.Fatal(err)
	}
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("k8s/flux/entry.yaml", `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  url: ssh://git@github.com/example/cluster.git
  ref: {branch: main}
  secretRef: {name: deploy-key}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./k8s/apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`)
	writeFile("k8s/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("k8s/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	gitCommitAll(t, repo)
	return dir
}

// TestE2E_DiffImagesRequiresBaselineWhenNoGit pins the error UX when
// no baseline can be auto-detected: --path lives outside a git repo
// AND --path-orig / --base are unset. The error must name both
// alternative flags so the user knows their options. Pre-auto-baseline
// this test asserted "diff requires --path-orig"; with auto-baseline
// the failure mode shifts to "no git repo" — but the suggestion in
// the message still mentions --path-orig so the test contract is
// upgraded, not replaced.
func TestE2E_DiffImagesRequiresBaselineWhenNoGit(t *testing.T) {
	// copyTree to a tempdir with no .git ancestor (t.TempDir's parent
	// chain stops at the OS temp root, which has no .git).
	dir := copyTree(t, testdataPath(t, "simple"))
	out, code := runCLIExpectErr(t, "diff", "images", "--path", dir)
	if code == 0 {
		t.Fatalf("expected non-zero exit with no baseline source, got 0:\n%s", out)
	}
	if !strings.Contains(out, "--path-orig") {
		t.Errorf("error should mention --path-orig as an option:\n%s", out)
	}
	if !strings.Contains(out, "--base") {
		t.Errorf("error should mention --base as an option:\n%s", out)
	}
}

// TestE2E_BootstrapErrorSurfacesNotMasked pins the contract that
// when discovery itself fails (e.g. a ResourceSet template that
// fails to parse, a YAML schema rejection), the actual error reaches
// the user instead of getting drowned under a wall of phantom
// "FAILED (no status reported)" rows from the testrunner running on
// a partial Store. Surfaced by tholinka/home-ops where an
// unimplemented inputStrategy: Permute ResourceSet produced 247
// generic failure rows instead of the actual message. Now that
// Permute is implemented, the test triggers Bootstrap failure with
// a malformed template — same code path through
// runOrchestratorCfg's `res == nil` guard.
func TestE2E_BootstrapErrorSurfacesNotMasked(t *testing.T) {
	dir := t.TempDir()
	// Minimal repo: one Kustomization + one ResourceSet whose template
	// references an undefined function ("nope"). text/template's Parse
	// rejects it; the ResourceSet render returns an error; Bootstrap
	// returns it; Render nils the Result; the CLI must surface the
	// underlying error rather than running the testrunner on partial
	// state.
	if err := os.WriteFile(filepath.Join(dir, "ks.yaml"), []byte(`---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rs.yaml"), []byte(`---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: broken-rs, namespace: flux-system}
spec:
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: << nope >>, namespace: ns}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	out, code := runCLIExpectErr(t, "test", "all", "--path", dir)
	if code == 0 {
		t.Fatalf("expected non-zero exit; got 0:\n%s", out)
	}
	// The template parse error mentions the undefined function.
	if !strings.Contains(out, "nope") {
		t.Errorf("error must mention the underlying Bootstrap failure (template parse rejecting `nope`); got:\n%s", out)
	}
	// Guard against the regression: the testrunner used to run on
	// the partial Store and emit one "FAILED (no status reported)"
	// row per loaded object. Collect-N where N is the partial count
	// is the signature.
	if strings.Contains(out, "no status reported") {
		t.Errorf("Bootstrap failure should NOT trigger testrunner output; got:\n%s", out)
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

	// -o github explicitly: this test asserts github diff-syntax markers,
	// independent of the default style.
	out := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig, "-o", "github")

	// Coverage must propagate to BOTH consumers, so the
	// original→changed transition should surface at least twice (once
	// per consumer Kustomization). dyff prints value changes as bare
	// `- original` / `+ changed` lines under a `@@ data.value @@`
	// path header.
	if got := strings.Count(out, "+ changed"); got < 2 {
		t.Errorf("coverage did not propagate to both consumers (got %d hits, want >= 2):\n%s", got, out)
	}
	if !strings.Contains(out, "- original") {
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

// TestE2E_SkipKindsAppliesToKustomizationOutput is the regression
// test for issue #169 — `--skip-secrets`, `--skip-crds`, and
// `--skip-kinds` were silently ignored for Kustomization-rendered
// docs. helm.TemplateDocs pre-filtered HR output but KS docs reached
// stdout unfiltered.
//
// The fixture contains one Kustomization that emits a ConfigMap, a
// Secret, and a CRD straight from kustomize (no HelmRelease). With
// default flags (--skip-secrets=true, --skip-crds=true) only the
// ConfigMap should reach stdout. Lifting the flags brings everything
// back.
func TestE2E_SkipKindsAppliesToKustomizationOutput(t *testing.T) {
	src := testdataPath(t, "skip-kinds")
	root := copyTree(t, src)

	t.Run("DefaultsDropSecretsAndCRDs", func(t *testing.T) {
		out := runCLIStdout(t, "build", "all", "--path", root, "-o", "yaml")
		if !strings.Contains(out, "name: kept-cm") {
			t.Errorf("ConfigMap should reach output: %s", out)
		}
		if strings.Contains(out, "name: dropped-secret") {
			t.Errorf("--skip-secrets (default true) failed to drop KS-rendered Secret:\n%s", out)
		}
		if strings.Contains(out, "kind: CustomResourceDefinition") {
			t.Errorf("--skip-crds (default true) failed to drop KS-rendered CRD:\n%s", out)
		}
	})

	t.Run("ExplicitFlagsAllowEverything", func(t *testing.T) {
		out := runCLIStdout(t, "build", "all", "--path", root, "-o", "yaml",
			"--skip-secrets=false", "--skip-crds=false")
		for _, want := range []string{"name: kept-cm", "name: dropped-secret", "kind: CustomResourceDefinition"} {
			if !strings.Contains(out, want) {
				t.Errorf("expected %q with --skip-* disabled:\n%s", want, out)
			}
		}
	})

	t.Run("SkipKindsListDropsArbitraryKinds", func(t *testing.T) {
		out := runCLIStdout(t, "build", "all", "--path", root, "-o", "yaml",
			"--skip-secrets=false", "--skip-crds=false",
			"--skip-kinds=ConfigMap")
		if strings.Contains(out, "name: kept-cm") {
			t.Errorf("--skip-kinds=ConfigMap failed to drop KS-rendered ConfigMap:\n%s", out)
		}
		if !strings.Contains(out, "name: dropped-secret") {
			t.Errorf("Secret should remain when only ConfigMap was skipped:\n%s", out)
		}
	})
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

// TestE2E_OrphanedKustomizationNotDiscovered verifies that a ks.yaml
// living under another Kustomization's spec.path but NOT listed in
// any parent kustomization.yaml is INVISIBLE to flate — it never
// loads, never reconciles, never appears in test output. Mirrors how
// Flux + `kustomize build` behave: orphans aren't in the rendered
// output, so they don't exist from the cluster's perspective.
//
// (Previous behavior was to load the orphan via a blind tree walk
// then downgrade its inevitable spec.path failure to a "resource
// orphaned" warning. The graph-driven loader replaces that
// post-hoc rescue with structural invisibility.)
func TestE2E_OrphanedKustomizationNotDiscovered(t *testing.T) {
	src := testdataPath(t, "orphans")
	root := copyTree(t, src)

	out := runCLI(t, "test", "ks", "--path", root)

	// "wired" is referenced and should pass.
	wiredLine := mustExtractLine(t, out, "Kustomization/flux-system/wired")
	if !strings.Contains(wiredLine, "PASSED") {
		t.Errorf("wired should pass: %s", wiredLine)
	}
	// "orphan" must not appear anywhere in the test output — it was
	// never loaded because the kustomize graph doesn't reach it.
	if strings.Contains(out, "flux-system/orphan") {
		t.Errorf("orphan Kustomization should be invisible to graph-driven loader; output contained reference:\n%s", out)
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

// TestE2E_ChangedOnlyHRDependsOnUnchangedDep is the regression test for
// issue #517: in changed-only mode a changed HelmRelease whose
// spec.dependsOn references an UNCHANGED HelmRelease must not fail
// "dependency not found". The dependency's producing Kustomization is
// SKIPPED (unchanged), so its HR is never render-emitted into the store
// — but an unchanged dependency is satisfied by definition for a delta
// check. The dependson/ fixture mirrors the bjw-s layout that surfaced
// the bug: per-app Flux Kustomizations with targetNamespace, bare HRs,
// lidarr dependsOn qbittorrent.
func TestE2E_ChangedOnlyHRDependsOnUnchangedDep(t *testing.T) {
	clusterPath, repoRoot := initGitFixtureFrom(t, testdataPath(t, "dependson"))

	// Edit a value in lidarr's HelmRelease so only lidarr is in changed
	// scope; qbittorrent (its dependsOn target) stays unchanged, so its
	// Kustomization is skipped and the qbittorrent HR is never emitted.
	hr := filepath.Join(repoRoot, "apps", "downloads", "lidarr", "app", "helmrelease.yaml")
	body, err := os.ReadFile(hr) //nolint:gosec // repoRoot is t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(body), "hello-from-lidarr", "hello-from-lidarr-edited", 1)
	if mutated == string(body) {
		t.Fatal("sentinel 'hello-from-lidarr' not found in lidarr helmrelease.yaml")
	}
	if err := os.WriteFile(hr, []byte(mutated), 0o600); err != nil { //nolint:gosec // repoRoot is t.TempDir()
		t.Fatal(err)
	}

	out := runCLIOutputOnly(t, "test", "all", "--path", clusterPath, "--base", "HEAD")

	if strings.Contains(out, "dependency not found") {
		t.Errorf("changed-only HR dependsOn on an unchanged dep must not fail:\n%s", out)
	}
	if lidarr := mustExtractLine(t, out, "HelmRelease/downloads/lidarr"); !strings.Contains(lidarr, "PASSED") {
		t.Errorf("lidarr HR should PASS once the unchanged dep is pruned; got: %s", lidarr)
	}
	// Confirm the test actually exercises the bug: qbittorrent's KS (the
	// dep's producer) must be skipped, otherwise the dep would render and
	// the assertion above would pass trivially.
	if line := mustExtractSkipLine(t, out, "Kustomization/downloads/qbittorrent"); !strings.Contains(line, "SKIPPED") {
		t.Errorf("expected qbittorrent KS SKIPPED (unchanged); got: %s", line)
	}
}

func mustExtractLine(t *testing.T, haystack, needle string) string {
	t.Helper()
	return mustFindLine(t, haystack, needle, "status", func(line string) bool {
		return strings.Contains(line, "PASSED") || strings.Contains(line, "FAILED")
	})
}

// mustExtractSkipLine returns the SKIPPED status line containing needle,
// failing the test if no such line exists. Companion to mustExtractLine
// for the changed-only path where mustExtractLine's PASSED/FAILED filter
// wouldn't match a skipped resource.
func mustExtractSkipLine(t *testing.T, haystack, needle string) string {
	t.Helper()
	return mustFindLine(t, haystack, needle, "SKIPPED", func(line string) bool {
		return strings.Contains(line, "SKIPPED")
	})
}

// mustFindLine returns the first line containing needle that also
// satisfies match, failing the test with a kind-tagged message otherwise.
func mustFindLine(t *testing.T, haystack, needle, kind string, match func(string) bool) string {
	t.Helper()
	for line := range strings.SplitSeq(haystack, "\n") {
		if strings.Contains(line, needle) && match(line) {
			return line
		}
	}
	t.Fatalf("%s line for %q not found in:\n%s", kind, needle, haystack)
	return ""
}

// gitCommitAll stages and commits the whole worktree with a fixed author,
// so every fixture lands one deterministic "init" commit.
func gitCommitAll(t *testing.T, repo *gogit.Repository) {
	t.Helper()
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Add("."); err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Commit("init", &gogit.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	}); err != nil {
		t.Fatal(err)
	}
}

// initGitFixtureFrom copies the fixture tree at src into a fresh tempdir,
// inits a git repo, and commits the whole tree as the baseline. Returns
// (clusterPath, repoRoot): clusterPath is the repoRoot/flux entrypoint
// callers pass as --path; repoRoot is the .git ancestor so the test can
// mutate fixture files after committing. Mirrors initGitFixture but seeds
// from a testdata fixture rather than inline bytes.
func initGitFixtureFrom(t *testing.T, src string) (clusterPath, repoRoot string) {
	t.Helper()
	dir := copyTree(t, src)
	repo, err := gogit.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	gitCommitAll(t, repo)
	return filepath.Join(dir, "flux"), dir
}

// initGitFixture creates a tempdir with a real git repo, commits a
// minimal Flux fixture (one KS + one ConfigMap with a sentinel value),
// and returns (clusterPath, repoRoot). clusterPath is the directory
// callers pass as --path; repoRoot is the .git ancestor (so the test
// can locate fixture files relative to it after committing).
func initGitFixture(t *testing.T) (clusterPath, repoRoot string) {
	t.Helper()
	dir := t.TempDir()
	repo, err := gogit.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("kubernetes/flux/cluster.yaml", `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/x.git"}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	writeFile("kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("kubernetes/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	gitCommitAll(t, repo)
	return filepath.Join(dir, "kubernetes", "flux"), dir
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
