package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"sigs.k8s.io/yaml"
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
		testutil.WriteFileAt(t, filepath.Join(k8s, "flux", tc.name+".yaml"), testutil.KustomizationYAML(tc.name, tc.namespace, "  interval: 10m\n  path: ./"+tc.path+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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

func TestRun_GroupDefaultsToAll(t *testing.T) {
	elapsed := regexp.MustCompile(`   [0-9]+\.[0-9]+s\n$`)
	for _, group := range []string{"build", "diff", "get", "test"} {
		t.Run(group, func(t *testing.T) {
			path := writeFixture(t)
			flags := []string{"-p", path, "--concurrency", "2"}
			if group == "diff" {
				orig := writeFixture(t)
				testutil.WriteFileAt(t, filepath.Join(path, "apps", "cm.yaml"),
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: changed}\n")
				flags = append(flags, "--path-orig", orig)
			}
			want, wantErr, wantCode := runCLI(t, append([]string{group, "all"}, flags...)...)
			got, gotErr, gotCode := runCLI(t, append([]string{group}, flags...)...)
			if wantCode != 0 || gotCode != wantCode || gotErr != wantErr {
				t.Fatalf("bare: exit %d, stderr %q; all: exit %d, stderr %q", gotCode, gotErr, wantCode, wantErr)
			}
			assert.Diff(t, elapsed.ReplaceAllString(got, ""), elapsed.ReplaceAllString(want, ""))
			_, stderr, code := runCLI(t, group, "frobnicate")
			if code != 1 || !strings.Contains(stderr, `unknown command "frobnicate"`) {
				t.Errorf("unknown subcommand: exit %d, stderr %q", code, stderr)
			}
			help, stderr, code := runCLI(t, group, "--help")
			if code != 0 || stderr != "" {
				t.Fatalf("help: exit %d, stderr %q", code, stderr)
			}
			for _, text := range []string{"Available Commands:", "all", "--path", "defaults to all"} {
				if !strings.Contains(help, text) {
					t.Errorf("help missing %q: %s", text, help)
				}
			}
		})
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

func TestRun_BuildAll(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path)
	if code != 0 {
		t.Fatalf("build all exited %d: stderr=%s", code, stderr)
	}
	for _, want := range []string{"kind: ConfigMap", "name: hello", "greeting: hi"} {
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

// TestRun_BuildKS_RejectsBadOutput exercises the -o enum on the build
// subcommand: build accepts yaml + json, not name.
func TestRun_BuildKS_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "build", "ks", "--path", path, "-o", "name")
	if code == 0 {
		t.Fatal("expected non-zero exit for -o name on build")
	}
	if !strings.Contains(stderr, "must be one of") {
		t.Errorf("error message missing 'must be one of': %q", stderr)
	}
}

// TestRun_BuildAll_OnlyKinds exercises the --only-kinds gate: a fixture
// without any CRDs should emit nothing but still exit 0.
func TestRun_BuildAll_OnlyKinds(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path, "--only-kinds", "CustomResourceDefinition")
	if code != 0 {
		t.Fatalf("build --only-kinds exited %d: %s", code, stderr)
	}
	if strings.Contains(stdout, "kind: ConfigMap") {
		t.Errorf("--only-kinds should filter out ConfigMap; got:\n%s", stdout)
	}
	for _, tc := range []struct {
		name, spec string
		empty      bool
		code       int
	}{
		{name: "filtered match"},
		{name: "suspended match", spec: "  suspend: true\n"},
		{name: "zero document match", empty: true},
		{name: "failed match", spec: "  dependsOn: [{name: missing}]\n", code: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFixture(t)
			appendScopeSpec(t, path, "flux/cluster.yaml", tc.spec)
			if tc.empty {
				testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: []\n")
			}
			stdout, stderr, code := runCLI(t, "build", "ks", "apps", "--path", path,
				"--only-kinds", "CustomResourceDefinition", "--cache-dir", t.TempDir(), "--concurrency", "2")
			if code != tc.code || stdout != "" || strings.Contains(stderr, "no Kustomization named") {
				t.Fatalf("named filtered match: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if tc.code == 1 && !strings.Contains(stderr, "missing") {
				t.Fatalf("matched failure lost: %s", stderr)
			}
		})
	}
	for _, tc := range []struct {
		name string
		args []string
		env  string
		want bool
	}{
		{name: "default", want: true},
		{name: "explicit CRD", args: []string{"--skip-kinds=CustomResourceDefinition"}},
		{name: "explicit empty", args: []string{"--skip-kinds="}, want: true},
		{name: "environment CRD", env: "CustomResourceDefinition"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_SKIP_KINDS", tc.env)
			if tc.env == "" {
				assert.NoError(t, os.Unsetenv("FLATE_SKIP_KINDS"))
			}
			path := writeFixture(t)
			testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [cm.yaml, crd.yaml]\n")
			testutil.WriteFile(t, path, "apps/crd.yaml", "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: widgets.example.test}\nspec: {group: example.test, names: {kind: Widget, plural: widgets}, scope: Namespaced, versions: [{name: v1, served: true, storage: true, schema: {openAPIV3Schema: {type: object}}}]}\n")
			args := append([]string{"build", "all", "--path", path, "--concurrency=2", "--only-kinds=CustomResourceDefinition"}, tc.args...)
			out, diagnostic, code := runCLI(t, args...)
			if code != 0 || strings.Contains(out, "kind: CustomResourceDefinition") != tc.want {
				t.Fatalf("CRD selection: exit=%d want=%t stdout=%s stderr=%s", code, tc.want, out, diagnostic)
			}
		})
	}

}

func TestRun_BuildAll_OnlyKinds_JSONEmptyArray(t *testing.T) {
	path := writeFixture(t)
	stdout, stderr, code := runCLI(t, "build", "all", "--path", path, "--only-kinds", "CustomResourceDefinition", "-o", "json")
	if code != 0 {
		t.Fatalf("build --only-kinds -o json exited %d: %s", code, stderr)
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

// TestRun_GetKS_YAML exercises -o yaml on a list verb.
func TestRun_GetKS_YAML(t *testing.T) {
	path := writeFixture(t)
	testutil.WriteFile(t, path, "nosrc.yaml", testutil.KustomizationYAML("nosrc", "flux-system", "  {path: ./apps, suspend: true}\n"))
	stdout, _, code := runCLI(t, "get", "ks", "--path", path, "-o", "yaml")
	if code != 0 {
		t.Fatalf("get ks -o yaml exited %d", code)
	}
	for _, want := range []string{"kind: Kustomization", "name: apps"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("yaml output missing %q:\n%s", want, stdout)
		}
	}
	for _, output := range []string{"yaml", "json"} {
		stdout, stderr, code := runCLI(t, "get", "ks", "nosrc", "--path", path, "-o", output)
		if code != 0 || !strings.Contains(stdout, "flux-system") || strings.Contains(stdout, `namespace: ""`) || strings.Contains(stdout, `"namespace": ""`) {
			t.Fatalf("absent sourceRef %s: exit=%d stdout=%s stderr=%s", output, code, stdout, stderr)
		}
	}
}

// TestRun_GetAll_RejectsBadOutput pins that get all rejects -o name —
// printCluster only shapes yaml/json.
func TestRun_GetAll_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "get", "all", "--path", path, "-o", "name")
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr, "must be one of") {
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

// TestRun_GetImages_RejectsBadOutput verifies the -o diff rejection.
func TestRun_GetImages_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	_, stderr, code := runCLI(t, "get", "images", "--path", path, "-o", "diff")
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr, "must be one of") {
		t.Errorf("expected validation error, got %q", stderr)
	}
}

// TestRun_TestAll exercises the report path on the fixture — every
// resource should pass (✓ rows, no failures in the summary).
// writeUnreadableSecretFixture writes a tree whose Flux Kustomization
// substituteFroms a Secret flate can't read offline (absent, non-SOPS, no
// producer), so the render emits a WarnUnresolvedSubstitution advisory.
func writeUnreadableSecretFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	k8s := filepath.Join(root, "kubernetes")
	testutil.WriteFileAt(t, filepath.Join(k8s, "flux", "cluster.yaml"), testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: Secret
        name: cluster-secrets
`))
	testutil.WriteFileAt(t, filepath.Join(k8s, "apps", "kustomization.yaml"), "resources:\n- cm.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(k8s, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: flux-system}
data:
  greeting: hi
`)
	return k8s
}

// TestRun_AdvisoriesOnlyInTest pins the contract: a render advisory (here an
// unreadable substituteFrom Secret) surfaces in `flate test` output but NOT in
// `flate build` — build is a data-producing verb, so warnings and deferred
// notes belong to the diagnostic command, never the build's stdout or stderr.
func TestRun_AdvisoriesOnlyInTest(t *testing.T) {
	k8s := writeUnreadableSecretFixture(t)
	const advisory = "could not be read offline"

	bOut, bErr, _ := runCLI(t, "build", "all", "--path", k8s)
	if strings.Contains(bOut, advisory) || strings.Contains(bErr, advisory) {
		t.Errorf("build must not surface the advisory.\nstdout:\n%s\nstderr:\n%s", bOut, bErr)
	}

	tOut, _, _ := runCLI(t, "test", "all", "--path", k8s)
	if !strings.Contains(tOut, advisory) {
		t.Errorf("test must surface the advisory; stdout:\n%s", tOut)
	}
}

func TestRun_TestAll(t *testing.T) {
	path := writeFixture(t)
	testutil.WriteFile(t, path, "flux/hidden.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: ExternalArtifact\nmetadata: {name: hidden, namespace: flux-system}\nstatus: {artifact: {url: 'file://"+path+"'}}\n")
	stdout, _, code := runCLI(t, "test", "all", "--path", path)
	assert.Equal(t, code, 0)
	assert.Equal(t, strings.Contains(stdout, "✓") && strings.Contains(stdout, "passed"), true)
	assert.Equal(t, strings.Contains(stdout, "failed") || strings.Contains(stdout, "hidden"), false)
}

func TestRun_TestAll_RespectsNamespace(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	stdout, stderr, code := runCLI(t, "test", "all", "--path", path, "-n", "alpha")
	if code != 0 {
		t.Fatalf("test all -n alpha exited %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "alpha/apps-a") {
		t.Errorf("namespace-scoped test output missing alpha KS:\n%s", stdout)
	}
	if strings.Contains(stdout, "beta/apps-b") {
		t.Errorf("namespace-scoped test output included beta KS:\n%s", stdout)
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
	if !strings.Contains(stdout, "✗") {
		t.Errorf("stdout should still include failed test row:\n%s", stdout)
	}
	// `flate test` is one consolidated report on stdout: the failing resource
	// named with its real (primary) error on its roster row, plus the verdict.
	// The old separate stderr failure block — which double-printed failures and a
	// second, differently-counted verdict — is gone.
	if !strings.Contains(stdout, "apps") || !strings.Contains(stdout, "kustomize build") {
		t.Errorf("stdout should surface the failing resource and its error, got %q", stdout)
	}
	if !strings.Contains(stdout, "failed") {
		t.Errorf("stdout should include the failure verdict, got %q", stdout)
	}
	if strings.Contains(stderr, "failed") {
		t.Errorf("failures + verdict are consolidated on stdout; stderr must not carry a second verdict, got %q", stderr)
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

func TestRun_RenderPersistenceNoticeInvocationScope(t *testing.T) {
	preservePersistenceNoticeState(t)
	current := writeFixture(t)
	orig := t.TempDir()
	testutil.CopyTree(t, current, orig)
	args := []string{"diff", "ks", "--path", current, "--path-orig", orig, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	renderPersistenceDisabledReason = ""
	stdout, stderr, code := runCLI(t, args...)
	if code != 0 {
		t.Fatalf("identical-tree diff exited %d: %s", code, stderr)
	}
	if strings.Contains(stdout, "@@") {
		t.Errorf("identical tree should produce no hunks:\n%s", stdout)
	}
	renderPersistenceDisabledReason = persistenceReason
	for range 2 {
		got, gotErr, gotCode := runCLI(t, args...)
		assert.Equal(t, gotCode, code)
		assert.Equal(t, got, stdout)
		assert.Equal(t, gotErr, persistenceNotice+stderr)
	}
}

func TestRun_DiffImages_RespectsNamespaceFailures(t *testing.T) {
	current := writeMultiNamespaceFixture(t)
	orig := t.TempDir()
	testutil.CopyTree(t, current, orig)
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

// TestDiff_OutputStyles drives a non-trivial diff and asserts each dyff
// style plus the plain unified diff routes to its renderer when selected
// via -o, end-to-end.
func TestDiff_OutputStyles(t *testing.T) {
	current := writeFixture(t)
	orig := t.TempDir()
	testutil.CopyTree(t, current, filepath.Join(orig, "kubernetes"))
	testutil.WriteFileAt(t, filepath.Join(orig, "kubernetes", "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  greeting: hola
`)
	origPath := filepath.Join(orig, "kubernetes")

	cases := []struct{ style, want string }{
		{"diff", "@@ -"},                      // unified diff hunk header
		{"html", `<table class="view side">`}, // self-contained HTML diff
		{"github", "@@ data.greeting @@"},     // dyff github diff-syntax
		{"gitlab", "= data.greeting"},         // gitlab `=` path prefix
		{"gitea", "@@ data.greeting @@"},      // gitea diff-syntax
		{"human", "data.greeting"},            // dyff human report
		{"brief", "change detected"},          // dyff one-line summary
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

	// The default (no -o) is human — pins the breaking change away from
	// github. Compare the no-flag output to an explicit -o human run.
	def, _, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", origPath)
	if code != 0 {
		t.Fatalf("diff ks (default) exited %d", code)
	}
	human, _, _ := runCLI(t, "diff", "ks", "-o", "human", "--path", current, "--path-orig", origPath)
	if def == "" || def != human {
		t.Errorf("default diff output should equal -o human:\ndefault:\n%s\nhuman:\n%s", def, human)
	}
}

// TestBindCommon_DefaultValues sanity-checks the persistent flag set:
// every common flag binds with a known-good default.
func TestBindCommon_DefaultValues(t *testing.T) {
	t.Parallel()
	assert.Equal(t, buildOrchCfg(commonFlags{}).SourceRetry, sourcecache.RetryConfig{
		Attempts: 3, MinWait: 200 * time.Millisecond, MaxWait: 3 * time.Second, Jitter: 0.1,
	})
	cmd := New("test")
	build, _, err := cmd.Find([]string{"build", "all"})
	if err != nil {
		t.Fatalf("find build all: %v", err)
	}
	for _, name := range []string{"path", "namespace", "output", "concurrency", "skip-kinds"} {
		if build.Flags().Lookup(name) == nil {
			t.Errorf("expected common flag %q on `build all`", name)
		}
	}
	var flags commonFlags
	bindCommon(pflag.NewFlagSet("test", pflag.ContinueOnError), &flags)
	assert.Diff(t, flags.skipKinds, []string{"Secret", "CustomResourceDefinition"})
	opts := buildOrchCfg(flags).HelmOptions
	assert.Equal(t, opts.SkipCRDs, true)
	assert.Equal(t, opts.SkipTests, true)
	assert.Diff(t, opts.SkipKinds, []string{"Secret"})
}

func TestBindCommon_OnReconcilingSubcommands(t *testing.T) {
	t.Parallel()
	cmd := New("test")
	for _, tc := range []struct {
		path, use, short string
		aliases          []string
		maxArgs          int
	}{
		{path: "build", use: "build", short: "Render Flux objects to YAML (defaults to all)"},
		{path: "build all", use: "all", short: "Render all Kustomization and HelmRelease objects"},
		{path: "build ks", use: "ks [name]", short: "Render Kustomizations", aliases: []string{"kustomization", "kustomizations"}, maxArgs: 1},
		{path: "build hr", use: "hr [name]", short: "Render HelmReleases", aliases: []string{"helmrelease", "helmreleases"}, maxArgs: 1},
		{path: "test", use: "test", short: "Report Kustomization + HelmRelease reconcile status (defaults to all)"},
		{path: "test all", use: "all", short: "Validate every Kustomization, HelmRelease, and Flux source CR"},
		{path: "test ks", use: "ks [name]", short: "Validate Kustomizations", aliases: []string{"kustomization", "kustomizations"}, maxArgs: 1},
		{path: "test hr", use: "hr [name]", short: "Validate HelmReleases", aliases: []string{"helmrelease", "helmreleases"}, maxArgs: 1},
	} {
		sub, _, err := cmd.Find(strings.Fields(tc.path))
		assert.NoError(t, err)
		assert.Equal(t, sub.Use, tc.use)
		assert.Equal(t, sub.Short, tc.short)
		assert.Diff(t, sub.Aliases, tc.aliases)
		if sub.Args == nil {
			t.Fatalf("%s: missing argument validator", tc.path)
		}
		assert.NoError(t, sub.Args(sub, nil))
		if err := sub.Args(sub, []string{"one"}); (err == nil) != (tc.maxArgs == 1) {
			t.Errorf("%s: argument limit %d rejected/accepted one name: %v", tc.path, tc.maxArgs, err)
		}
		if err := sub.Args(sub, []string{"one", "two"}); err == nil {
			t.Errorf("%s: accepted two names", tc.path)
		}
	}
	for _, argv := range [][]string{
		{"build"},
		{"build", "ks"},
		{"build", "hr"},
		{"build", "all"},
		{"diff", "ks"},
		{"diff", "hr"},
		{"diff", "all"},
		{"diff", "images"},
		{"diff"},
		{"get"},
		{"get", "all"},
		{"get", "images"},
		{"get", "ks"},
		{"get", "hr"},
		{"test", "all"},
		{"test", "ks"},
		{"test", "hr"},
		{"test"},
	} {
		sub, _, err := cmd.Find(argv)
		if err != nil {
			t.Fatalf("find %v: %v", argv, err)
		}
		for _, binding := range []struct {
			name, typ, shorthand, def string
		}{
			{"kube-version", "string", "", helm.BundledKubeVersion()},
			{"api-versions", "string", "a", ""},
			{"is-upgrade", "bool", "", "false"},
			{"no-hooks", "bool", "", "false"},
			{"show-only", "stringSlice", "s", "[]"},
			{"enable-dns", "bool", "", "false"},
		} {
			flag := sub.Flags().Lookup(binding.name)
			if flag == nil {
				t.Fatalf("%v: missing helm flag %s", argv, binding.name)
			}
			assert.Equal(t, flag.Value.Type(), binding.typ)
			assert.Equal(t, flag.Shorthand, binding.shorthand)
			assert.Equal(t, flag.DefValue, binding.def)
			if !strings.Contains(flag.Usage, envKey(binding.name)) {
				t.Errorf("%v: missing environment usage for %s", argv, binding.name)
			}
		}
		for _, name := range []string{"path", "cache-dir"} {
			if sub.Flags().Lookup(name) == nil {
				t.Errorf("%v: missing common flag %s", argv, name)
			}
		}
		flag := sub.Flags().Lookup("disable-chart-digest-tracking")
		if flag == nil || flag.Value.Type() != "boolfunc" || flag.NoOptDefVal != "true" {
			t.Fatalf("%v: missing boolean override binding", argv)
		}
		if !strings.Contains(flag.Usage, envKey("disable-chart-digest-tracking")) {
			t.Errorf("%v: missing environment usage", argv)
		}
		assert.NoError(t, sub.ParseFlags([]string{"--kube-version=1.31.2", "-a", "fixture.example/v1", "--is-upgrade", "--no-hooks", "-s", "templates/main.yaml", "--enable-dns"}))
		for name, want := range map[string]string{"kube-version": "1.31.2", "api-versions": "fixture.example/v1", "is-upgrade": "true", "no-hooks": "true", "show-only": "[templates/main.yaml]", "enable-dns": "true"} {
			assert.Equal(t, sub.Flags().Lookup(name).Value.String(), want)
		}
		fresh, _, err := New("test").Find(argv)
		assert.NoError(t, err)
		for _, name := range []string{"kube-version", "api-versions", "is-upgrade", "no-hooks", "show-only", "enable-dns"} {
			assert.Equal(t, fresh.Flags().Lookup(name).Value.String(), fresh.Flags().Lookup(name).DefValue)
		}

	}
	maintenance, _, err := cmd.Find([]string{"cache", "gc"})
	assert.NoError(t, err)
	assert.Equal(t, maintenance.Flags().Lookup("path") == nil, true)
	assert.Equal(t, maintenance.Flags().Lookup("cache-dir") != nil, true)
}

func TestCompareDocs_OrdersByIdentity(t *testing.T) {
	t.Parallel()
	mkDoc := func(apiVersion, kind, ns, name string) map[string]any {
		return map[string]any{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata":   map[string]any{"namespace": ns, "name": name},
		}
	}
	cases := []struct {
		name string
		a, b map[string]any
		want int
	}{
		{"kind", mkDoc("v1", "ConfigMap", "a", "x"), mkDoc("v1", "Secret", "a", "x"), -1},
		{"group", mkDoc("a.example/v1", "CM", "z", "x"), mkDoc("z.example/v1", "CM", "a", "x"), -1},
		{"core", mkDoc("v1", "CM", "z", "x"), mkDoc("a.example/v1", "CM", "a", "x"), -1},
		{"namespace", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "b", "x"), -1},
		{"name", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "a", "y"), -1},
		{"same", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "a", "x"), 0},
		{"version", mkDoc("a.example/v1", "CM", "a", "x"), mkDoc("a.example/v2", "CM", "a", "x"), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := compareDocs(tc.a, tc.b)
			if (got < 0) != (tc.want < 0) || (got == 0) != (tc.want == 0) {
				t.Errorf("compareDocs(%v, %v) = %d, want sign of %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestEmissionDocs_OnlyKinds drops every non-CRD doc.
func TestEmissionDocs_OnlyKinds(t *testing.T) {
	t.Parallel()
	docs := []map[string]any{
		{"kind": "ConfigMap"},
		{"kind": "CustomResourceDefinition"},
		{"kind": "Secret"},
		{"kind": "CustomResourceDefinition"},
	}
	out := emissionDocs(docs, &buildFlags{onlyKinds: []string{manifest.KindCustomResourceDefinition}})
	if docs[2]["kind"] != "Secret" {
		t.Errorf("emissionDocs mutated the input: %+v", docs)
	}
	if len(out) != 2 {
		t.Errorf("expected 2 CRDs, got %d: %+v", len(out), out)
	}
	for _, d := range out {
		if d["kind"] != "CustomResourceDefinition" {
			t.Errorf("non-CRD slipped through: %v", d)
		}
	}
}

func TestEmissionDocs_EmptyOnNoCRDs(t *testing.T) {
	t.Parallel()
	docs := []map[string]any{{"kind": "ConfigMap"}, {"kind": "Secret"}}
	if out := emissionDocs(docs, &buildFlags{onlyKinds: []string{manifest.KindCustomResourceDefinition}}); len(out) != 0 {
		t.Errorf("expected no docs for no-CRD input, got %+v", out)
	}
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

// TestRun_Diff_OneSidedFailureSuppressed is the #938 regression: when the
// Kustomization renders on the orig side but fails on the current side, its
// objects must not be diffed as removals. The output leads with a suppression
// note, the failure summary is printed on stderr, and the exit code stays
// non-zero.
func TestRun_Diff_OneSidedFailureSuppressed(t *testing.T) {
	orig := writeFixture(t)
	current := writeFixture(t)
	// Break the current side's kustomize entry point so the KS fails there only.
	testutil.WriteFileAt(t, filepath.Join(current, "apps", "kustomization.yaml"),
		"resources:\n- missing.yaml\n")

	stdout, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig, "-o", "github")
	if code == 0 {
		t.Fatal("expected non-zero exit: the current side failed to render")
	}
	if strings.Contains(stdout, "document removed") || strings.Contains(stdout, "greeting") {
		t.Errorf("the orig-only objects must be suppressed, not diffed as removals:\n%s", stdout)
	}
	if !strings.HasPrefix(stdout, "! Kustomization.kustomize.toolkit.fluxcd.io flux-system/apps: 1 object not rendered on the current side; diff suppressed (") {
		t.Errorf("stdout should lead with the suppression note; got:\n%s", stdout)
	}
	if !strings.Contains(stderr, "flate error: current snapshot: reconcile completed with 1 failure(s)") {
		t.Errorf("stderr should carry the failure summary:\n%s", stderr)
	}
	if strings.Count(stderr, "flate error:") != 1 {
		t.Errorf("failure summary should be printed exactly once:\n%s", stderr)
	}
}

func TestRun_ConventionalIgnore(t *testing.T) {
	path := writeFixture(t)
	testutil.WriteFile(t, path, "flux/extra.yaml", testutil.KustomizationYAML("apps-extra", "flux-system", "  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	testutil.WriteFile(t, path, ".krmignore", "flux/extra.yaml\n")
	stdout, stderr, code := runCLI(t, "get", "ks", "--path", path, "-o", "name")
	if code != 0 || strings.Contains(stdout, "apps-extra") || !strings.Contains(stdout, "apps") {
		t.Fatalf("conventional ignore: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func TestRun_TestNamed_MatchingFailureFromOtherKind(t *testing.T) {
	root := writeFixture(t)
	testutil.WriteFile(t, root, "flux/ghost.yaml", testutil.KustomizationYAML("ghost", "flux-system", "  path: ./missing-ghost\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	stdout, stderr, code := runCLI(t, "test", "hr", "ghost", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2")
	if code != 1 || !strings.Contains(stderr, `no HelmRelease named "ghost"`) || stdout != "" {
		t.Fatalf("other-kind diagnostic satisfied named roster: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func TestRun_GetHRChartVersions(t *testing.T) {
	for _, tc := range []struct{ name, reference, chart, want, wantChart string }{
		{name: "digest", reference: "digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, semver: '>=1.2.0', tag: '1.2.3'", want: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantChart: "apps-chart/"},
		{name: "semver", reference: "semver: '>=1.2.0', tag: '1.2.3'", want: ">=1.2.0", wantChart: "apps-chart/"},
		{name: "tag", reference: "tag: '1.2.3'", want: "1.2.3", wantChart: "apps-chart/"},
		{name: "own version", reference: "tag: '1.2.3'", chart: "chart: {spec: {chart: app, version: '9.9.9', sourceRef: {kind: OCIRepository, name: chart}}}", want: "9.9.9", wantChart: "apps-chart/app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			chart := tc.chart
			if chart == "" {
				chart = "chartRef: {kind: OCIRepository, name: chart}"
			}
			testutil.WriteFile(t, root, "flux.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec: {suspend: true, "+chart+"}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {suspend: true, url: 'oci://example.test/app', ref: {"+tc.reference+"}}\n")
			stdout, stderr, code := runCLI(t, "get", "hr", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json")
			var rows []struct {
				Version   string `json:"version"`
				Chart     string `json:"chart"`
				SourceRef struct {
					Namespace string `json:"namespace"`
				} `json:"sourceRef"`
			}
			if code != 0 || json.Unmarshal([]byte(stdout), &rows) != nil || len(rows) != 1 || rows[0].Version != tc.want || rows[0].Chart != tc.wantChart || rows[0].SourceRef.Namespace != "apps" {
				t.Fatalf("chart version: exit=%d stdout=%s stderr=%s want=%s chart=%s", code, stdout, stderr, tc.want, tc.wantChart)
			}
		})
	}
}

func TestRun_Images_GetAndDiffFormatsMatch(t *testing.T) {
	current, orig := writeFixture(t), writeFixture(t)
	testutil.WriteFile(t, current, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  z: ghcr.io/example/z:2
  a: ghcr.io/example/a:1
  duplicate: ghcr.io/example/z:2
`)
	cache := t.TempDir()
	want := []string{"ghcr.io/example/a:1", "ghcr.io/example/z:2"}
	for _, format := range []string{"name", "yaml", "json"} {
		t.Run(format, func(t *testing.T) {
			flags := []string{"--path", current, "--cache-dir", cache, "--concurrency", "2"}
			if format != "name" {
				flags = append(flags, "-o", format)
			}
			get, stderr, code := runCLI(t, append([]string{"get", "images"}, flags...)...)
			if code != 0 {
				t.Fatalf("get images exited %d: %s", code, stderr)
			}
			diffArgs := append([]string{"diff", "images", "--path-orig", orig}, flags...)
			delta, stderr, code := runCLI(t, diffArgs...)
			if code != 0 {
				t.Fatalf("diff images exited %d: %s", code, stderr)
			}
			assert.Diff(t, delta, get)
			var images []string
			if format == "name" {
				assert.Equal(t, get, strings.Join(want, "\n")+"\n")
				images = strings.Split(strings.TrimSuffix(get, "\n"), "\n")
			} else {
				prefix := "- "
				if format == "json" {
					prefix = "["
				}
				if !strings.HasPrefix(get, prefix) || !strings.HasPrefix(delta, prefix) {
					t.Fatalf("wrong %s wire format: get=%q diff=%q", format, get, delta)
				}
				if err := yaml.Unmarshal([]byte(get), &images); err != nil {
					t.Fatal(err)
				}
			}
			assert.Diff(t, images, want)
		})
	}
	t.Run("empty/default", func(t *testing.T) {
		for _, verb := range []string{"get", "diff"} {
			t.Run(verb, func(t *testing.T) {
				args := []string{verb, "images", "--path", orig, "--cache-dir", cache, "--concurrency", "2"}
				if verb == "diff" {
					args = append(args, "--path-orig", writeFixture(t))
				}
				stdout, stderr, code := runCLI(t, args...)
				if code != 0 || stdout != "" {
					t.Fatalf("empty %s images: exit=%d stdout=%q stderr=%s", verb, code, stdout, stderr)
				}
			})
		}
	})
	t.Run("unchanged/json", func(t *testing.T) {
		stdout, stderr, code := runCLI(t, "diff", "images", "--path", current, "--path-orig", current,
			"--cache-dir", cache, "--concurrency", "2", "-o", "json")
		if code != 0 {
			t.Fatalf("unchanged diff images: exit=%d stderr=%s", code, stderr)
		}
		if got := strings.TrimSpace(stdout); got != "[]" && got != "null" {
			t.Fatalf("unchanged image diff = %q, want [] or null", stdout)
		}
	})
}

func TestRun_Build_PartialRenderAndDocumentWriteFailure(t *testing.T) {
	root := writeMultiNamespaceFixture(t)
	testutil.WriteFile(t, root, "apps-b/kustomization.yaml", "resources: [missing.yaml]\n")
	args := []string{"build", "ks", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json"}
	full, stderr, code := runCLI(t, args...)
	assert.Equal(t, code, 1)
	if !strings.Contains(stderr, "apps-b") || !strings.Contains(full, "apps-a") {
		t.Fatalf("fixture did not preserve the healthy render: stdout=%s stderr=%s", full, stderr)
	}
	limit := len(full) - 1
	writeErr := errors.New("document write failed")
	writer := &outputFailWriter{remaining: limit, err: writeErr}
	var errOut bytes.Buffer
	cmd := New("test")
	cmd.SetArgs(args)
	cmd.SetOut(writer)
	cmd.SetErr(&errOut)
	got := cmd.ExecuteContext(t.Context())
	if _, ok := errors.AsType[*orchestrator.FailuresError](got); !ok || !errors.Is(got, writeErr) {
		t.Fatalf("render or writer error lost: %v", got)
	}
	if _, ok := errors.AsType[reportedError](got); !ok {
		t.Fatalf("completed diagnostic was not marked reported: %v", got)
	}
	assert.Equal(t, writer.buf.String(), full[:limit])
	assert.Equal(t, strings.Count(errOut.String(), writeErr.Error()), 1)
	writer = &outputFailWriter{remaining: limit, err: writeErr}
	errOut.Reset()
	assert.Equal(t, Run(args, writer, &errOut), 1)
	assert.Equal(t, writer.buf.String(), full[:limit])
	assert.Equal(t, strings.Count(errOut.String(), writeErr.Error()), 1)
}

type outputFailWriter struct {
	buf       bytes.Buffer
	remaining int
	err       error
}

func (w *outputFailWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.remaining)
	_, _ = w.buf.Write(p[:n])
	w.remaining -= n
	if n < len(p) {
		return n, w.err
	}
	return n, nil
}

func TestRun_CommandContext(t *testing.T) {
	for _, command := range []string{"build all", "get all", "get hr", "get images", "test all", "diff all", "diff images"} {
		for _, embedded := range []bool{false, true} {
			t.Run(command+"/embedded="+strconv.FormatBool(embedded), func(t *testing.T) {
				root := New("test")
				args := strings.Fields(command)
				if embedded {
					parent := &cobra.Command{Use: "parent", SilenceUsage: true, SilenceErrors: true}
					parent.AddCommand(root)
					root = parent
					args = append([]string{"flate"}, args...)
				}
				args = append(args, "--path", writeFixture(t), "--concurrency=2")
				if strings.HasPrefix(command, "diff") {
					args = append(args, "--path-orig", writeFixture(t))
				}
				root.SetArgs(args)
				root.SetOut(io.Discard)
				root.SetErr(io.Discard)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := root.ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("handler lost cancellation: %v", err)
				}
			})
		}
	}
}

func TestRun_HelmOptions(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "cluster/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  values: {tag: after}
`)
	testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/app/templates/main.yaml", `{{- if or (ne .Capabilities.KubeVersion.Version "v1.31.2") (not (.Capabilities.APIVersions.Has "fixture.example/v1")) (not .Release.IsUpgrade) }}
{{ fail "CLI Helm options did not reach rendering" }}
{{- end }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: main
  annotations:
    fixture/kube: {{ .Capabilities.KubeVersion.Version | quote }}
    fixture/api: {{ .Capabilities.APIVersions.Has "fixture.example/v1" | quote }}
    fixture/upgrade: {{ .Release.IsUpgrade | quote }}
spec:
  selector: {matchLabels: {app: main}}
  template:
    metadata: {labels: {app: main}}
    spec: {containers: [{name: main, image: "fixture/main:{{ .Values.tag }}"}]}
`)
	for _, name := range []string{"extra", "hook"} {
		annotation := ""
		if name == "hook" {
			annotation = `, annotations: {helm.sh/hook: pre-install}`
		}
		testutil.WriteFile(t, root, "charts/app/templates/"+name+".yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: "+name+annotation+"}\nspec: {selector: {matchLabels: {app: "+name+"}}, template: {metadata: {labels: {app: "+name+"}}, spec: {containers: [{name: "+name+", image: 'fixture/"+name+":{{ .Values.tag }}'}]}}}\n")
	}
	original := t.TempDir()
	testutil.CopyTree(t, root, original)
	hr, err := os.ReadFile(filepath.Join(original, "cluster/hr.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, original, "cluster/hr.yaml", strings.ReplaceAll(string(hr), "tag: after", "tag: before"))
	flags := []string{"--path", filepath.Join(root, "cluster"), "--concurrency=2", "--kube-version=1.31.2", "-a", "fixture.example/v1", "--is-upgrade"}
	for _, command := range []string{"build hr", "get all", "get hr", "get images", "test hr", "diff hr", "diff images"} {
		t.Run(command, func(t *testing.T) {
			args := append(strings.Fields(command), flags...)
			args = append(args, "--no-hooks", "-s", "app/templates/main.yaml")
			if strings.HasPrefix(command, "diff") {
				args = append(args, "--path-orig", filepath.Join(original, "cluster"))
			}
			out, diagnostic, code := runCLI(t, args...)
			if code != 0 || out == "" {
				t.Fatalf("Helm options: exit=%d stdout=%s stderr=%s", code, out, diagnostic)
			}
			if command == "build hr" {
				for _, want := range []string{"fixture/kube: v1.31.2", "fixture/api: \"true\"", "fixture/upgrade: \"true\"", "fixture/main:after"} {
					if !strings.Contains(out, want) {
						t.Errorf("missing %q in %s", want, out)
					}
				}
			}
			if strings.HasSuffix(command, "images") && (!strings.Contains(out, "fixture/main") || strings.Contains(out, "fixture/extra") || strings.Contains(out, "fixture/hook")) {
				t.Fatalf("image filtering lost Helm options: %s", out)
			}
		})
	}
	for _, tc := range []struct {
		name        string
		args        []string
		extra, hook bool
	}{
		{name: "hooks included", extra: true, hook: true},
		{name: "hooks excluded", args: []string{"--no-hooks"}, extra: true},
		{name: "show only", args: []string{"-s", "app/templates/main.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"build", "hr"}, flags...)
			out, diagnostic, code := runCLI(t, append(args, tc.args...)...)
			if code != 0 || !strings.Contains(out, "name: main") || strings.Contains(out, "name: extra") != tc.extra || strings.Contains(out, "name: hook") != tc.hook {
				t.Fatalf("template filtering: exit=%d stdout=%s stderr=%s", code, out, diagnostic)
			}
		})
	}
}
