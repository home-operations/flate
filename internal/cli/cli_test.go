package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	mustWrite(t, filepath.Join(k8s, "flux", "cluster.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	mustWrite(t, filepath.Join(k8s, "apps", "kustomization.yaml"),
		"resources:\n- cm.yaml\n")
	mustWrite(t, filepath.Join(k8s, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  greeting: hi
`)
	return k8s
}

func mustWrite(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
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
// Just verify it doesn't crash on each accepted value plus the
// default path.
func TestRun_LogLevelFlag(t *testing.T) {
	for _, lvl := range []string{"debug", "info", "warn", "error", "bogus"} {
		_, _, code := runCLI(t, "--log-level", lvl, "--help")
		if code != 0 {
			t.Errorf("--log-level %q exited %d", lvl, code)
		}
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

// TestRun_BuildKS_NameFilter_NoMatch is the error path: typo name on
// build should fail loud (the get verb is permissive — filters
// silently — but build can't render a nonexistent target).
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

// TestBindHelmFlags_OnHRSubcommandOnly guards the rendersHelm() gate:
// `build ks` should NOT carry helm-template flags, but `build hr` and
// `build all` should.
func TestBindHelmFlags_OnHRSubcommandOnly(t *testing.T) {
	cmd := New("test")
	cases := []struct {
		argv         []string
		wantHelmFlag bool
	}{
		{argv: []string{"build", "ks"}, wantHelmFlag: false},
		{argv: []string{"build", "hr"}, wantHelmFlag: true},
		{argv: []string{"build", "all"}, wantHelmFlag: true},
	}
	for _, tc := range cases {
		sub, _, err := cmd.Find(tc.argv)
		if err != nil {
			t.Fatalf("find %v: %v", tc.argv, err)
		}
		got := sub.Flags().Lookup("kube-version") != nil
		if got != tc.wantHelmFlag {
			t.Errorf("%v: helm flag binding = %v, want %v", tc.argv, got, tc.wantHelmFlag)
		}
	}
}

// TestRendersHelm_Pure exercises the unexported predicate directly so
// edge cases stay covered when verb registration shifts.
func TestRendersHelm_Pure(t *testing.T) {
	cases := []struct {
		kinds []string
		want  bool
	}{
		{kinds: []string{"Kustomization"}, want: false},
		{kinds: []string{"HelmRelease"}, want: true},
		{kinds: []string{"Kustomization", "HelmRelease"}, want: true},
		{kinds: nil, want: false},
	}
	for _, tc := range cases {
		if got := rendersHelm(tc.kinds); got != tc.want {
			t.Errorf("rendersHelm(%v) = %v, want %v", tc.kinds, got, tc.want)
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
		{mkDoc("ConfigMap", "a", "x"), mkDoc("Secret", "a", "x"), -1},     // kind wins
		{mkDoc("CM", "a", "x"), mkDoc("CM", "b", "x"), -1},                // ns wins after kind tie
		{mkDoc("CM", "a", "x"), mkDoc("CM", "a", "y"), -1},                // name wins last
		{mkDoc("CM", "a", "x"), mkDoc("CM", "a", "x"), 0},                 // identical
	}
	for _, tc := range cases {
		got := compareDocs(tc.a, tc.b)
		if (got < 0) != (tc.want < 0) || (got == 0) != (tc.want == 0) {
			t.Errorf("compareDocs(%v, %v) = %d, want sign of %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestSortRows_Deterministic covers helpers.sortRows: rows must
// land in (namespace, name) order regardless of input order.
func TestSortRows_Deterministic(t *testing.T) {
	rows := []map[string]string{
		{"namespace": "b", "name": "z"},
		{"namespace": "a", "name": "y"},
		{"namespace": "a", "name": "x"},
	}
	sortRows(rows)
	want := []string{"a/x", "a/y", "b/z"}
	for i, r := range rows {
		got := r["namespace"] + "/" + r["name"]
		if got != want[i] {
			t.Errorf("rows[%d] = %s, want %s", i, got, want[i])
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
	}
}

type dummyErr struct{ s string }

func (d *dummyErr) Error() string { return d.s }
