package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

// runCLI drives cli.Run inside the test binary and returns
// (stdout, stderr, exitCode). All tests use this so end-to-end
// coverage of the cobra tree counts against pkg internal/cli.
func runCLI(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}

// writeFixture writes a minimal Flux GitOps tree to dir:
//
//   - kubernetes/flux/cluster.yaml — root Kustomization pointing at apps/
//   - kubernetes/apps/cm.yaml      — one ConfigMap so render produces output
//   - kubernetes/apps/kustomization.yaml — kustomize entry point
//
// Returns the --path the CLI should use. Self-contained so tests don't
// depend on the repo's testdata/ tree.
func writeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	k8s := filepath.Join(root, "kubernetes")
	testutil.WriteFileAt(t, filepath.Join(k8s, "flux", "cluster.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
  labels: {app.kubernetes.io/name: apps}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFileAt(t, filepath.Join(k8s, "apps", "kustomization.yaml"),
		"resources:\n- cm.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(k8s, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  greeting: hi
`)
	return k8s
}

func writeMultiNamespaceFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	k8s := filepath.Join(root, "kubernetes")
	for _, tc := range []struct {
		name      string
		namespace string
		path      string
	}{
		{name: "apps-a", namespace: "alpha", path: "apps-a"},
		{name: "apps-b", namespace: "beta", path: "apps-b"},
	} {
		testutil.WriteFileAt(t, filepath.Join(k8s, "flux", tc.name+".yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: `+tc.name+`
  namespace: `+tc.namespace+`
spec:
  interval: 10m
  path: ./`+tc.path+`
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
		testutil.WriteFileAt(t, filepath.Join(k8s, tc.path, "kustomization.yaml"), "resources:\n- cm.yaml\n")
		testutil.WriteFileAt(t, filepath.Join(k8s, tc.path, "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: `+tc.name+`, namespace: `+tc.namespace+`}
data:
  greeting: hi
`)
	}
	return k8s
}

// TestRun_VersionFlag covers the --version path that cobra wires onto
// the root command. Exit 0, version string echoed to stdout.
func TestRun_VersionFlag(t *testing.T) {
	stdout, _, code := runCLI(t, "--version")
	if code != 0 {
		t.Fatalf("--version exited %d", code)
	}
	if !strings.Contains(stdout, "dev") {
		t.Errorf("expected version string in stdout, got %q", stdout)
	}
}

// TestRun_HelpExits0 covers the "no subcommand" path — cobra prints
// help and exits 0.
func TestRun_HelpExits0(t *testing.T) {
	stdout, _, code := runCLI(t, "--help")
	if code != 0 {
		t.Fatalf("--help exited %d", code)
	}
	for _, want := range []string{"build", "diff", "test", "get"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("--help output missing verb %q: %s", want, stdout)
		}
	}
}

// TestRun_UnknownCommand returns non-zero and a useful error.
func TestRun_UnknownCommand(t *testing.T) {
	_, stderr, code := runCLI(t, "frobnicate")
	if code == 0 {
		t.Fatal("expected non-zero exit for unknown command")
	}
	if !strings.Contains(stderr, "frobnicate") {
		t.Errorf("error should name the unknown command; got %q", stderr)
	}
}

// TestRun_LogLevelFlag exercises the persistent --log-level handler.
// Accepted enum values exit 0 on --help (the PreRunE successfully
// installs the level). The cobra --help fast path doesn't trigger
// PreRunE in older cobra, so we keep the original assertion shape
// for the success values; the rejection case is covered by
// TestRun_LogLevelFlag_RejectsInvalid below using a real command.
func TestRun_LogLevelFlag(t *testing.T) {
	for _, lvl := range []string{"debug", "info", "warn", "error"} {
		_, _, code := runCLI(t, "--log-level", lvl, "--help")
		if code != 0 {
			t.Errorf("--log-level %q exited %d", lvl, code)
		}
	}
}

// TestRun_LogLevelFlag_RejectsInvalid pins the validation fix: an
// invalid --log-level value must fail loudly with a clear message
// rather than silently defaulting to info. Without the fix
// `--log-level dbug` (a common typo) ran at info and the user
// thought debug output was simply quiet.
func TestRun_LogLevelFlag_RejectsInvalid(t *testing.T) {
	_, stderr, code := runCLI(t, "build", "all", "--log-level", "bogus", "--path", ".")
	if code == 0 {
		t.Fatal("expected non-zero exit for invalid --log-level")
	}
	if !strings.Contains(stderr, "invalid --log-level") {
		t.Errorf("expected 'invalid --log-level' in stderr; got %q", stderr)
	}
}

// TestRun_MissingPathErrors covers the runOrchestrator early-error path
// when --path is empty (the verb code defaults --path to "." so we
// can't easily make it empty, but a non-existent dir reliably fails).
func TestRun_MissingPathErrors(t *testing.T) {
	_, stderr, code := runCLI(t, "build", "all", "--path", "/nonexistent/path/here")
	if code == 0 {
		t.Fatal("expected non-zero exit for missing path")
	}
	if !strings.Contains(stderr, "flate error") {
		t.Errorf("error message missing prefix: %q", stderr)
	}
}

// TestRun_BuildAll exercises the full build-all happy path: render
// the fixture, emit YAML, exit 0.
func TestRun_BuildAll(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code != 0 {
		t.Fatalf("build all exited %d: stderr=%s", code, stderr)
	}
	for _, want := range []string{"kind: ConfigMap", "name: hello"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("build output missing %q:\n%s", want, stdout)
		}
	}
}

func TestRun_BuildAll_JSONSingleDocument(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path, "-o", "json")
	if code != 0 {
		t.Fatalf("build all -o json exited %d: stderr=%s", code, stderr)
	}
	var docs []map[string]any
	if err := json.Unmarshal([]byte(stdout), &docs); err != nil {
		t.Fatalf("build all -o json emitted invalid JSON: %v\n%s", err, stdout)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 rendered docs, got %d: %#v", len(docs), docs)
	}
}

// TestRun_BuildKS_RejectsBadOutput exercises requireOutput on the
// build subcommand: build accepts yaml + json, not name.
func TestRun_BuildKS_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "build", "ks", "--path", path, "-o", "name")
	if code == 0 {
		t.Fatal("expected non-zero exit for -o name on build")
	}
	if !strings.Contains(stderr, "not supported") {
		t.Errorf("error message missing 'not supported': %q", stderr)
	}
}

// TestRun_BuildAll_OnlyCRDs exercises the --only-crds gate: a fixture
// without any CRDs should emit nothing but still exit 0.
func TestRun_BuildAll_OnlyCRDs(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path, "--only-crds")
	if code != 0 {
		t.Fatalf("build --only-crds exited %d: %s", code, stderr)
	}
	if strings.Contains(stdout, "kind: ConfigMap") {
		t.Errorf("--only-crds should filter out ConfigMap; got:\n%s", stdout)
	}
}

func TestRun_BuildAll_OnlyCRDs_JSONEmptyArray(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path, "--only-crds", "-o", "json")
	if code != 0 {
		t.Fatalf("build --only-crds -o json exited %d: %s", code, stderr)
	}
	if strings.TrimSpace(stdout) != "[]" {
		t.Errorf("empty JSON render = %q, want []", stdout)
	}
}

// TestRun_GetKS exercises the get-ks command, default table output.
func TestRun_GetKS(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "get", "ks", "--path", path)
	if code != 0 {
		t.Fatalf("get ks exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "NAMESPACE") || !strings.Contains(stdout, "NAME") {
		t.Errorf("table header missing:\n%s", stdout)
	}
	if !strings.Contains(stdout, "apps") {
		t.Errorf("expected ks 'apps' in output:\n%s", stdout)
	}
}

// TestRun_GetKS_NameFilter exercises the positional arg filter.
func TestRun_GetKS_NameFilter(t *testing.T) {
	path := writeFixture(t)
	stdout, _, code := runCLI(t, "get", "ks", "apps", "--path", path)
	if code != 0 {
		t.Fatalf("get ks apps exited %d", code)
	}
	if !strings.Contains(stdout, "apps") {
		t.Errorf("name filter dropped the matching object:\n%s", stdout)
	}
}

func TestRun_GetKS_NameFilter_NoMatch(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "get", "ks", "nonexistent", "--path", path)
	if code == 0 {
		t.Fatal("expected non-zero for nonexistent name on get")
	}
	if !strings.Contains(stderr, "nonexistent") {
		t.Errorf("error should name the typo'd argument: %q", stderr)
	}
}

func TestRun_GetKS_NameFilter_LabelMismatchIsEmpty(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "get", "ks", "apps", "--path", path, "-l", "app.kubernetes.io/name=other")
	if code != 0 {
		t.Fatalf("get ks apps with non-matching label exited %d: %s", code, stderr)
	}
	if strings.Contains(stderr, "no Kustomization named") {
		t.Fatalf("name existed but label filter was reported as a missing name: %s", stderr)
	}
	if strings.Contains(stdout, "apps") {
		t.Errorf("label mismatch should filter out apps:\n%s", stdout)
	}
}

func TestRun_GetKS_NameFilter_NamespaceMismatchFails(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "get", "ks", "apps", "--path", path, "-n", "other")
	if code == 0 {
		t.Fatal("expected non-zero for name outside namespace scope")
	}
	if !strings.Contains(stderr, "apps") {
		t.Errorf("error should name the scoped-out argument: %q", stderr)
	}
}

// TestRun_BuildKS_NameFilter_NoMatch is the error path: typo name on
// build should fail loud instead of rendering an empty target.
func TestRun_BuildKS_NameFilter_NoMatch(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "build", "ks", "nonexistent", "--path", path)
	if code == 0 {
		t.Fatal("expected non-zero for nonexistent name on build")
	}
	if !strings.Contains(stderr, "nonexistent") {
		t.Errorf("error should name the typo'd argument: %q", stderr)
	}
}

// TestRun_GetKS_YAML exercises -o yaml on a list verb.
func TestRun_GetKS_YAML(t *testing.T) {
	path := writeFixture(t)
	stdout, _, code := runCLI(t, "get", "ks", "--path", path, "-o", "yaml")
	if code != 0 {
		t.Fatalf("get ks -o yaml exited %d", code)
	}
	for _, want := range []string{"kind: Kustomization", "name: apps"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("yaml output missing %q:\n%s", want, stdout)
		}
	}
}

// TestRun_GetAll_RejectsBadOutput covers the requireOutput fix we
// just landed: get all must reject -o name.
func TestRun_GetAll_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "get", "all", "--path", path, "-o", "name")
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr, "not supported") {
		t.Errorf("expected validation error, got %q", stderr)
	}
}

// TestRun_GetAll_Yaml emits a key:value cluster summary.
func TestRun_GetAll_Yaml(t *testing.T) {
	path := writeFixture(t)
	stdout, _, code := runCLI(t, "get", "all", "--path", path, "-o", "yaml")
	if code != 0 {
		t.Fatalf("get all exited %d", code)
	}
	if !strings.Contains(stdout, "kustomizations:") {
		t.Errorf("summary missing kustomizations key:\n%s", stdout)
	}
}

func TestRun_GetAll_RespectsNamespace(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	stdout, stderr, code := runCLI(t, "get", "all", "--path", path, "-o", "json", "-n", "alpha")
	if code != 0 {
		t.Fatalf("get all -n alpha exited %d: %s", code, stderr)
	}
	var summary map[string]int
	if err := json.Unmarshal([]byte(stdout), &summary); err != nil {
		t.Fatalf("get all emitted invalid JSON: %v\n%s", err, stdout)
	}
	if summary["kustomizations"] != 1 || summary["helmReleases"] != 0 {
		t.Errorf("namespace-scoped summary = %#v, want 1 KS and 0 HR", summary)
	}
}

// TestRun_GetImages_NameDefault exercises the default name format
// (one image per line).
func TestRun_GetImages_NameDefault(t *testing.T) {
	path := writeFixture(t)
	_, _, code := runCLI(t, "get", "images", "--path", path)
	if code != 0 {
		t.Fatalf("get images exited %d", code)
	}
	// Fixture has no images, but the command should still succeed
	// with an empty list — failing exit would indicate a regression.
}

// TestRun_GetImages_RejectsBadOutput verifies the -o diff rejection.
func TestRun_GetImages_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "get", "images", "--path", path, "-o", "diff")
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr, "not supported") {
		t.Errorf("expected validation error, got %q", stderr)
	}
}

// TestRun_TestAll exercises the report path on the fixture — every
// resource should be PASSED.
func TestRun_TestAll(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "test", "all", "--path", path)
	if code != 0 {
		t.Fatalf("test all exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "PASSED") {
		t.Errorf("expected PASSED in test report:\n%s", stdout)
	}
}

func TestRun_TestAll_RespectsNamespace(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	stdout, stderr, code := runCLI(t, "test", "all", "--path", path, "-n", "alpha")
	if code != 0 {
		t.Fatalf("test all -n alpha exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "Kustomization/alpha/apps-a") {
		t.Errorf("namespace-scoped test output missing alpha KS:\n%s", stdout)
	}
	if strings.Contains(stdout, "Kustomization/beta/apps-b") {
		t.Errorf("namespace-scoped test output included beta KS:\n%s", stdout)
	}
}

func TestRun_TestKS_NameFilter_NoMatch(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "test", "ks", "nonexistent", "--path", path)
	if code == 0 {
		t.Fatal("expected non-zero for nonexistent name on test")
	}
	if !strings.Contains(stderr, "nonexistent") {
		t.Errorf("error should name the typo'd argument: %q", stderr)
	}
}

func TestRun_TestAll_ReturnsStdoutWriteError(t *testing.T) {
	path := writeFixture(t)
	want := errors.New("stdout closed")
	var stderr bytes.Buffer
	code := Run([]string{"test", "all", "--path", path}, failingWriter{err: want}, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero for stdout write failure")
	}
	if !strings.Contains(stderr.String(), want.Error()) {
		t.Errorf("stderr should include writer error %q: %q", want, stderr.String())
	}
}

func TestRun_TestAll_JoinsVisibleFailuresWithRunError(t *testing.T) {
	path := writeFixture(t)
	if err := os.Remove(filepath.Join(path, "apps", "cm.yaml")); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runCLI(t, "test", "all", "--path", path)
	if code == 0 {
		t.Fatal("expected non-zero for failed reconcile")
	}
	if !strings.Contains(stdout, "FAILED") {
		t.Errorf("stdout should still include failed test row:\n%s", stdout)
	}
	if !strings.Contains(stderr, "reconcile completed") {
		t.Errorf("stderr should preserve scoped run error, got %q", stderr)
	}
}

// TestRun_TestAll_RejectsOutput covers test's new -o rejection
// (test only emits plain-text).
func TestRun_TestAll_RejectsOutput(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "test", "all", "--path", path, "-o", "yaml")
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr, "not supported") {
		t.Errorf("expected validation error: %q", stderr)
	}
}

// TestRun_DiffKS_NoOrigErrors covers the diff-without-path-orig path:
// diff must reject when no baseline is supplied.
func TestRun_DiffKS_NoOrigErrors(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "diff", "ks", "--path", path)
	if code == 0 {
		t.Fatal("expected non-zero exit for diff without --path-orig")
	}
	if !strings.Contains(stderr, "path-orig") {
		t.Errorf("error should mention --path-orig: %q", stderr)
	}
}

// TestRun_DiffKS_TwoTreesNoDelta exercises diff between two identical
// trees: should exit 0 with empty diff.
func TestRun_DiffKS_TwoTreesNoDelta(t *testing.T) {
	current := writeFixture(t)
	// Copy fixture into a sibling tempdir to act as --path-orig.
	orig := t.TempDir()
	copyTree(t, current, orig)
	stdout, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig)
	if code != 0 {
		t.Fatalf("identical-tree diff exited %d: %s", code, stderr)
	}
	if strings.Contains(stdout, "@@") {
		t.Errorf("identical tree should produce no hunks:\n%s", stdout)
	}
}

func TestRun_DiffKS_ExplicitDiffOutput(t *testing.T) {
	current := writeFixture(t)
	orig := t.TempDir()
	copyTree(t, current, orig)
	_, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig, "-o", "diff")
	if code != 0 {
		t.Fatalf("diff ks -o diff exited %d: %s", code, stderr)
	}
}

func TestRun_DiffKS_NameFilter_NoMatch(t *testing.T) {
	current := writeFixture(t)
	orig := t.TempDir()
	copyTree(t, current, orig)
	_, stderr, code := runCLI(t, "diff", "ks", "nonexistent", "--path", current, "--path-orig", orig)
	if code == 0 {
		t.Fatal("expected non-zero for nonexistent name on diff")
	}
	if !strings.Contains(stderr, "nonexistent") {
		t.Errorf("error should name the typo'd argument: %q", stderr)
	}
}

// TestRun_DiffImages_NameDefault exercises diff images on identical
// trees — no images either side, no diff hunks.
func TestRun_DiffImages_NameDefault(t *testing.T) {
	current := writeFixture(t)
	orig := t.TempDir()
	copyTree(t, current, orig)
	_, _, code := runCLI(t, "diff", "images", "--path", current, "--path-orig", orig)
	if code != 0 {
		t.Fatalf("diff images exited %d", code)
	}
}

func TestRun_DiffImages_RespectsNamespaceFailures(t *testing.T) {
	current := writeMultiNamespaceFixture(t)
	orig := t.TempDir()
	copyTree(t, current, orig)
	if err := os.RemoveAll(filepath.Join(current, "apps-b")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(orig, "apps-b")); err != nil {
		t.Fatal(err)
	}

	_, stderr, code := runCLI(t, "diff", "images", "--path", current, "--path-orig", orig, "-n", "alpha")
	if code != 0 {
		t.Fatalf("diff images -n alpha should ignore beta reconcile failure, exited %d: %s", code, stderr)
	}
}

// TestBuild_OutputMarkdown pins the build → markdown shape: every
// rendered doc lands under an `### <kind>/...` heading wrapping a
// fenced YAML block (the Layer A MarkdownDocs contract).
func TestBuild_OutputMarkdown(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "-o", "markdown", "--path", path)
	if code != 0 {
		t.Fatalf("build all -o markdown exited %d: %s", code, stderr)
	}
	for _, want := range []string{"```yaml", "### "} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q in markdown output:\n%s", want, stdout)
		}
	}
}

// TestGet_OutputMarkdown pins the get → markdown shape: a GFM pipe
// table with the standard header/separator/data rows the format
// package emits.
func TestGet_OutputMarkdown(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "get", "ks", "-o", "markdown", "--path", path)
	if code != 0 {
		t.Fatalf("get ks -o markdown exited %d: %s", code, stderr)
	}
	for _, want := range []string{"| ", "| --- "} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q in markdown table:\n%s", want, stdout)
		}
	}
}

// TestGetAll_OutputMarkdown covers the cluster-summary path:
// `## Cluster summary` heading + a two-row pipe table.
func TestGetAll_OutputMarkdown(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "get", "all", "-o", "markdown", "--path", path)
	if code != 0 {
		t.Fatalf("get all -o markdown exited %d: %s", code, stderr)
	}
	for _, want := range []string{"## Cluster summary", "Kustomizations", "HelmReleases", "| --- "} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q in cluster summary markdown:\n%s", want, stdout)
		}
	}
}

// TestTest_OutputMarkdown pins the test → markdown shape: the
// pipe-table summary header that testrunner.Report.WriteMarkdown
// emits, plus the per-outcome H2 section for the only outcome the
// fixture can produce (Passed).
func TestTest_OutputMarkdown(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "test", "ks", "-o", "markdown", "--path", path)
	if code != 0 {
		t.Fatalf("test ks -o markdown exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "| Passed | Skipped | Failed | Matched |") {
		t.Errorf("missing summary header:\n%s", stdout)
	}
	if !strings.Contains(stdout, "## Passed") &&
		!strings.Contains(stdout, "## Skipped") &&
		!strings.Contains(stdout, "## Failed") {
		t.Errorf("missing at least one outcome section:\n%s", stdout)
	}
}

// TestDiff_OutputMarkdown drives a non-trivial diff (baseline tree
// has a ConfigMap with different data than current) and asserts the
// Layer B markdown render: the `# Diff` heading, an `### <header>`
// per resource, and the ```diff fence wrapping the dyff body.
func TestDiff_OutputMarkdown(t *testing.T) {
	current := writeFixture(t)
	orig := t.TempDir()
	copyTree(t, current, filepath.Join(orig, "kubernetes"))
	// Mutate the baseline ConfigMap so the diff renders a non-empty
	// hunk. Without the delta `diff.Render` returns the empty-set
	// "_No changes._" body and the ```diff fence assertion misses.
	testutil.WriteFileAt(t, filepath.Join(orig, "kubernetes", "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  greeting: hola
`)
	stdout, stderr, code := runCLI(t, "diff", "ks", "-o", "markdown",
		"--path", current, "--path-orig", filepath.Join(orig, "kubernetes"))
	if code != 0 {
		t.Fatalf("diff ks -o markdown exited %d: %s", code, stderr)
	}
	for _, want := range []string{"# Diff", "### ", "```diff"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q in diff markdown:\n%s", want, stdout)
		}
	}
}

// TestDiff_OutputStyles drives a non-trivial diff and asserts each
// dyff style plus the plain unified diff routes to its renderer when
// selected via -o. Confirms the CLI wiring (requireOutput accepts the
// token, Run renders the body, Render aggregates) end-to-end.
func TestDiff_OutputStyles(t *testing.T) {
	current := writeFixture(t)
	orig := t.TempDir()
	copyTree(t, current, filepath.Join(orig, "kubernetes"))
	testutil.WriteFileAt(t, filepath.Join(orig, "kubernetes", "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  greeting: hola
`)
	origPath := filepath.Join(orig, "kubernetes")

	cases := []struct{ style, want string }{
		{"diff", "@@ -"},                  // unified diff hunk header
		{"github", "@@ data.greeting @@"}, // dyff github diff-syntax
		{"gitlab", "= data.greeting"},     // gitlab `=` path prefix
		{"gitea", "@@ data.greeting @@"},  // gitea diff-syntax
		{"human", "data.greeting"},        // dyff human report
		{"brief", "change detected"},      // dyff one-line summary
	}
	for _, tc := range cases {
		t.Run(tc.style, func(t *testing.T) {
			stdout, stderr, code := runCLI(t, "diff", "ks", "-o", tc.style,
				"--path", current, "--path-orig", origPath)
			if code != 0 {
				t.Fatalf("diff ks -o %s exited %d: %s", tc.style, code, stderr)
			}
			if !strings.Contains(stdout, tc.want) {
				t.Errorf("-o %s output missing %q:\n%s", tc.style, tc.want, stdout)
			}
		})
	}
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		data, err := os.ReadFile(p) //nolint:gosec // p is inside t.TempDir
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600) //nolint:gosec // target is inside t.TempDir
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestBindCommon_DefaultValues sanity-checks the persistent flag set:
// every common flag binds with a known-good default.
func TestBindCommon_DefaultValues(t *testing.T) {
	cmd := New("test")
	build, _, err := cmd.Find([]string{"build", "all"})
	if err != nil {
		t.Fatalf("find build all: %v", err)
	}
	for _, name := range []string{"path", "namespace", "output", "concurrency", "skip-crds", "skip-secrets"} {
		if build.Flags().Lookup(name) == nil {
			t.Errorf("expected common flag %q on `build all`", name)
		}
	}
}

// TestBindHelmFlags_OnReconcilingSubcommands guards the reconcile-first
// contract: even KS-shaped output commands run the full reconcile graph, so
// helm-template flags must be available consistently.
func TestBindHelmFlags_OnReconcilingSubcommands(t *testing.T) {
	cmd := New("test")
	for _, argv := range [][]string{
		{"build", "ks"},
		{"build", "hr"},
		{"build", "all"},
		{"diff", "ks"},
		{"diff", "hr"},
		{"diff", "all"},
		{"get", "all"},
		{"get", "images"},
		{"get", "ks"},
		{"get", "hr"},
		{"test", "all"},
		{"test", "ks"},
		{"test", "hr"},
	} {
		sub, _, err := cmd.Find(argv)
		if err != nil {
			t.Fatalf("find %v: %v", argv, err)
		}
		if sub.Flags().Lookup("kube-version") == nil {
			t.Errorf("%v: missing helm flag kube-version", argv)
		}
	}
}

// TestOutputOrDefault_FallsBackOnTable covers the "table is the
// global default, every verb coerces it to its natural shape" rule.
func TestOutputOrDefault_FallsBackOnTable(t *testing.T) {
	c := &commonFlags{output: "table"}
	if got := c.outputOrDefault("yaml"); got != "yaml" {
		t.Errorf("table → fallback failed: %q", got)
	}
	c.output = "json"
	if got := c.outputOrDefault("yaml"); got != "json" {
		t.Errorf("explicit -o json should win: %q", got)
	}
}

// TestCompareDocs_OrdersByKindNamespaceName pins the sort order
// build uses when emitting multi-doc YAML: (kind, namespace, name)
// lexical, so renders are byte-stable across runs even when the
// underlying maps iterate in random order.
func TestCompareDocs_OrdersByKindNamespaceName(t *testing.T) {
	mkDoc := func(kind, ns, name string) map[string]any {
		return map[string]any{
			"kind":     kind,
			"metadata": map[string]any{"namespace": ns, "name": name},
		}
	}
	cases := []struct {
		a, b map[string]any
		want int
	}{
		{mkDoc("ConfigMap", "a", "x"), mkDoc("Secret", "a", "x"), -1}, // kind wins
		{mkDoc("CM", "a", "x"), mkDoc("CM", "b", "x"), -1},            // ns wins after kind tie
		{mkDoc("CM", "a", "x"), mkDoc("CM", "a", "y"), -1},            // name wins last
		{mkDoc("CM", "a", "x"), mkDoc("CM", "a", "x"), 0},             // identical
	}
	for _, tc := range cases {
		got := compareDocs(tc.a, tc.b)
		if (got < 0) != (tc.want < 0) || (got == 0) != (tc.want == 0) {
			t.Errorf("compareDocs(%v, %v) = %d, want sign of %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestFilterCRDsOnly drops every non-CRD doc.
func TestFilterCRDsOnly(t *testing.T) {
	docs := []map[string]any{
		{"kind": "ConfigMap"},
		{"kind": "CustomResourceDefinition"},
		{"kind": "Secret"},
		{"kind": "CustomResourceDefinition"},
	}
	out := filterCRDsOnly(docs)
	if len(out) != 2 {
		t.Errorf("expected 2 CRDs, got %d: %+v", len(out), out)
	}
	for _, d := range out {
		if d["kind"] != "CustomResourceDefinition" {
			t.Errorf("non-CRD slipped through: %v", d)
		}
	}
}

// TestFilterCRDsOnly_EmptyOnNoCRDs covers the common-case zero-alloc
// path: no CRDs in input → nil out.
func TestFilterCRDsOnly_EmptyOnNoCRDs(t *testing.T) {
	docs := []map[string]any{{"kind": "ConfigMap"}, {"kind": "Secret"}}
	if out := filterCRDsOnly(docs); out != nil {
		t.Errorf("expected nil for no-CRD input, got %+v", out)
	}
}

// TestJoinRunErrors covers the four arms of helpers.joinRunErrors.
func TestJoinRunErrors(t *testing.T) {
	e1 := &dummyErr{"e1"}
	e2 := &dummyErr{"e2"}
	cases := []struct {
		orig, curr error
		wantNil    bool
		wantSub    string
	}{
		{nil, nil, true, ""},
		{e1, nil, false, "orig snapshot"},
		{nil, e2, false, "current snapshot"},
		{e1, e2, false, "both snapshots"},
	}
	for _, tc := range cases {
		got := joinRunErrors(tc.orig, tc.curr)
		if (got == nil) != tc.wantNil {
			t.Errorf("orig=%v curr=%v: nil? = %v, want %v", tc.orig, tc.curr, got == nil, tc.wantNil)
			continue
		}
		if !tc.wantNil && !strings.Contains(got.Error(), tc.wantSub) {
			t.Errorf("orig=%v curr=%v: error %q missing %q", tc.orig, tc.curr, got, tc.wantSub)
		}
		if tc.orig != nil && !strings.Contains(got.Error(), tc.orig.Error()) {
			t.Errorf("orig=%v curr=%v: error %q missing orig detail", tc.orig, tc.curr, got)
		}
		if tc.curr != nil && !strings.Contains(got.Error(), tc.curr.Error()) {
			t.Errorf("orig=%v curr=%v: error %q missing current detail", tc.orig, tc.curr, got)
		}
		if tc.orig != nil && !errors.Is(got, tc.orig) {
			t.Errorf("orig=%v curr=%v: errors.Is missing orig", tc.orig, tc.curr)
		}
		if tc.curr != nil && !errors.Is(got, tc.curr) {
			t.Errorf("orig=%v curr=%v: errors.Is missing current", tc.orig, tc.curr)
		}
	}
}

type dummyErr struct{ s string }

func (d *dummyErr) Error() string { return d.s }

type failingWriter struct {
	err error
}

func (w failingWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}
