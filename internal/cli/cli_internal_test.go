package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// All CLI tests run in-process so coverage includes the Cobra tree.
func runCLI(t *testing.T, args ...string) cliResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(args, &stdout, &stderr)
	return cliResult{stdout: stdout.String(), stderr: stderr.String(), code: code}
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
	testutil.WriteFile(t, k8s, "kustomization.yaml", "resources: [flux/apps-a.yaml, flux/apps-b.yaml]\n")
	for _, tc := range []struct {
		name      string
		namespace string
		path      string
	}{
		{name: "apps-a", namespace: "alpha", path: "apps-a"},
		{name: "apps-b", namespace: "beta", path: "apps-b"},
	} {
		testutil.WriteFileAt(
			t,
			filepath.Join(k8s, "flux", tc.name+".yaml"),
			testutil.KustomizationYAML(
				tc.name,
				tc.namespace,
				"  interval: 10m\n  path: ./"+tc.path+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
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
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_HelpExits0(t *testing.T) {
	cli := runCLI(t, "--help")
	stdout, code := cli.stdout, cli.code
	assert.FatalIf(t, code != 0, "--help exited %d", code)
	for _, want := range []string{"build", "diff", "test", "get"} {
		assert.ErrorIf(t, !strings.Contains(stdout, want), "--help output missing verb %q: %s", want, stdout)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GroupDefaultsToAll(t *testing.T) {
	elapsed := regexp.MustCompile(` {3}\d+\.\d+s\n$`)
	for _, group := range []string{"build", "diff", "get", "test"} {
		t.Run(group, func(t *testing.T) {
			path := writeFixture(t)
			flags := []string{"-p", path, "--concurrency", "2"}
			if group == "diff" {
				orig := writeFixture(t)
				testutil.WriteFileAt(
					t,
					filepath.Join(path, "apps", "cm.yaml"),
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: changed}\n",
				)
				flags = append(flags, "--path-orig", orig)
			}
			cli := runCLI(t, append([]string{group, "all"}, flags...)...)
			want, wantErr, wantCode := cli.stdout, cli.stderr, cli.code
			cli2 := runCLI(t, append([]string{group}, flags...)...)
			got, gotErr, gotCode := cli2.stdout, cli2.stderr, cli2.code
			assert.FatalIf(
				t,
				wantCode != 0 || gotCode != wantCode || gotErr != wantErr,
				"bare: exit %d, stderr %q; all: exit %d, stderr %q",
				gotCode,
				gotErr,
				wantCode,
				wantErr,
			)
			assert.Diff(t, elapsed.ReplaceAllString(got, ""), elapsed.ReplaceAllString(want, ""))
			cli3 := runCLI(t, group, "frobnicate")
			stderr, code := cli3.stderr, cli3.code
			assert.ErrorIf(
				t,
				code != 1 || !strings.Contains(stderr, `unknown command "frobnicate"`),
				"unknown subcommand: exit %d, stderr %q",
				code,
				stderr,
			)
			cli4 := runCLI(t, group, "--help")
			help, stderr, code := cli4.stdout, cli4.stderr, cli4.code
			assert.FatalIf(t, code != 0 || stderr != "", "help: exit %d, stderr %q", code, stderr)
			for _, text := range []string{"Available Commands:", "all", "--path", "defaults to all"} {
				assert.ErrorIf(t, !strings.Contains(help, text), "help missing %q: %s", text, help)
			}
		})
	}
}

// TestRun_LogLevelFlag_RejectsInvalid pins the validation fix: an
// invalid --log-level value must fail loudly with a clear message
// rather than silently defaulting to info. Without the fix
// `--log-level dbug` (a common typo) ran at info and the user
// thought debug output was simply quiet.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_LogLevelFlag_RejectsInvalid(t *testing.T) {
	cli := runCLI(t, "build", "all", "--log-level", "bogus", "--path", ".")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit for invalid --log-level")
	assert.ErrorIf(
		t,
		!strings.Contains(stderr, "invalid --log-level"),
		"expected 'invalid --log-level' in stderr; got %q",
		stderr,
	)
}

// TestRun_MissingPathErrors covers the runOrchestrators early-error path
// when --path is empty (the verb code defaults --path to "." so we
// can't easily make it empty, but a non-existent dir reliably fails).
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_MissingPathErrors(t *testing.T) {
	cli := runCLI(t, "build", "all", "--path", "/nonexistent/path/here")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit for missing path")
	assert.ErrorIf(t, !strings.Contains(stderr, "flate error"), "error message missing prefix: %q", stderr)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BuildAll(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "build", "all", "--path", path)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "build all exited %d: stderr=%s", code, stderr)
	for _, want := range []string{"kind: ConfigMap", "name: hello", "greeting: hi"} {
		assert.ErrorIf(t, !strings.Contains(stdout, want), "build output missing %q:\n%s", want, stdout)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BuildAll_JSONSingleDocument(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	cli := runCLI(t, "build", "all", "--path", path, "-o", "json")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "build all -o json exited %d: stderr=%s", code, stderr)
	var docs []map[string]any
	{
		err := json.Unmarshal([]byte(stdout), &docs)
		assert.FatalIf(t, err != nil, "build all -o json emitted invalid JSON: %v\n%s", err, stdout)
	}
	assert.FatalIf(
		t,
		len(docs) != 4,
		"expected 2 rendered ConfigMaps and 2 applied Kustomizations, got %d: %#v",
		len(docs),
		docs,
	)
	for i, kind := range []string{manifest.KindConfigMap, manifest.KindConfigMap, manifest.KindKustomization, manifest.KindKustomization} {
		assert.Equal(t, manifest.DocKind(docs[i]), kind)
	}
}

// TestRun_BuildKS_RejectsBadOutput exercises the -o enum on the build
// subcommand: build accepts yaml + json, not name.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BuildKS_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "build", "ks", "--path", path, "-o", "name")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit for -o name on build")
	assert.ErrorIf(t, !strings.Contains(stderr, "must be one of"), "error message missing 'must be one of': %q", stderr)
}

// TestRun_BuildAll_OnlyKinds exercises the --only-kinds gate: a fixture
// without any CRDs should emit nothing but still exit 0.
//

func TestRun_BuildAll_OnlyKinds(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "build", "all", "--path", path, "--only-kinds", "CustomResourceDefinition")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "build --only-kinds exited %d: %s", code, stderr)
	assert.ErrorIf(
		t,
		strings.Contains(stdout, "kind: ConfigMap"),
		"--only-kinds should filter out ConfigMap; got:\n%s",
		stdout,
	)
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
			cli2 := runCLI(t, "build", "ks", "apps", "--path", path,
				"--only-kinds", "CustomResourceDefinition", "--cache-dir", t.TempDir(), "--concurrency", "2")
			stdout, stderr, code := cli2.stdout, cli2.stderr, cli2.code
			assert.FatalIf(
				t,
				code != tc.code || stdout != "" || strings.Contains(stderr, "no Kustomization named"),
				"named filtered match: exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)
			assert.FatalIf(t, tc.code == 1 && !strings.Contains(stderr, "missing"), "matched failure lost: %s", stderr)
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
			testutil.WriteFile(
				t,
				path,
				"apps/crd.yaml",
				"apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: widgets.example.test}\nspec: {group: example.test, names: {kind: Widget, plural: widgets}, scope: Namespaced, versions: [{name: v1, served: true, storage: true, schema: {openAPIV3Schema: {type: object}}}]}\n",
			)
			args := append(
				[]string{"build", "all", "--path", path, "--concurrency=2", "--only-kinds=CustomResourceDefinition"},
				tc.args...)
			cli3 := runCLI(t, args...)
			out, diagnostic, code := cli3.stdout, cli3.stderr, cli3.code
			assert.FatalIf(
				t,
				code != 0 || strings.Contains(out, "kind: CustomResourceDefinition") != tc.want,
				"CRD selection: exit=%d want=%t stdout=%s stderr=%s",
				code,
				tc.want,
				out,
				diagnostic,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BuildAll_OnlyKinds_JSONEmptyArray(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(
		t,
		"build",
		"all",
		"--path",
		path,
		"--only-kinds",
		"CustomResourceDefinition",
		"-o",
		"json",
	)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "build --only-kinds -o json exited %d: %s", code, stderr)
	assert.ErrorIf(t, strings.TrimSpace(stdout) != "[]", "empty JSON render = %q, want []", stdout)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetKS(t *testing.T) {
	for _, tc := range []struct {
		name, path, wantPath string
	}{
		{name: "omitted", wantPath: "./"},
		{name: "root", path: "./", wantPath: "./"},
		{name: "apps", path: "./apps", wantPath: "./apps"},
	} {
		t.Run(tc.name, func(t *testing.T) { runGetKSName(t, &tc) })
	}
}

// TestRun_GetKS_NameFilter exercises the positional arg filter.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetKS_NameFilter(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "get", "ks", "apps", "--path", path)
	stdout, code := cli.stdout, cli.code
	assert.FatalIf(t, code != 0, "get ks apps exited %d", code)
	assert.ErrorIf(t, !strings.Contains(stdout, "apps"), "name filter dropped the matching object:\n%s", stdout)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetKS_NameFilter_LabelMismatchIsEmpty(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "get", "ks", "apps", "--path", path, "-l", "app.kubernetes.io/name=other")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "get ks apps with non-matching label exited %d: %s", code, stderr)
	assert.FatalIf(
		t,
		strings.Contains(stderr, "no Kustomization named"),
		"name existed but label filter was reported as a missing name: %s",
		stderr,
	)
	assert.ErrorIf(t, strings.Contains(stdout, "apps"), "label mismatch should filter out apps:\n%s", stdout)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetKS_NameFilter_NamespaceMismatchFails(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "get", "ks", "apps", "--path", path, "-n", "other")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero for name outside namespace scope")
	assert.ErrorIf(t, !strings.Contains(stderr, "apps"), "error should name the scoped-out argument: %q", stderr)
}

// TestRun_GetKS_YAML exercises -o yaml on a list verb.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetKS_YAML(t *testing.T) {
	path := writeFixture(t)
	testutil.WriteFile(
		t,
		path,
		"nosrc.yaml",
		testutil.KustomizationYAML("nosrc", "flux-system", "  {path: ./apps, suspend: true}\n"),
	)
	cli := runCLI(t, "get", "ks", "--path", path, "-o", "yaml")
	stdout, code := cli.stdout, cli.code
	assert.FatalIf(t, code != 0, "get ks -o yaml exited %d", code)
	for _, want := range []string{"kind: Kustomization", "name: apps"} {
		assert.ErrorIf(t, !strings.Contains(stdout, want), "yaml output missing %q:\n%s", want, stdout)
	}
	for _, output := range []string{"yaml", "json"} {
		cli2 := runCLI(t, "get", "ks", "nosrc", "--path", path, "-o", output)
		stdout, stderr, code := cli2.stdout, cli2.stderr, cli2.code
		assert.FatalIf(
			t,
			code != 0 || !strings.Contains(stdout, "flux-system") || strings.Contains(stdout, `namespace: ""`) ||
				strings.Contains(
					stdout,
					`"namespace": ""`,
				),
			"absent sourceRef %s: exit=%d stdout=%s stderr=%s",
			output,
			code,
			stdout,
			stderr,
		)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "get", "all", "--path", path, "-o", "diff")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit")
	assert.ErrorIf(t, !strings.Contains(stderr, "must be one of"), "expected validation error, got %q", stderr)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_Yaml(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "get", "all", "--path", path, "-o", "yaml")
	stdout, code := cli.stdout, cli.code
	assert.FatalIf(t, code != 0, "get all exited %d", code)
	assert.ErrorIf(t, !strings.Contains(stdout, "kind: Kustomization\n") || !strings.Contains(stdout, "name: apps\n") ||
		strings.Contains(stdout, "kustomizations:"), "object listing missing Kustomization:\n%s", stdout)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_RespectsNamespace(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	cli := runCLI(t, "get", "all", "--path", path, "-o", "json", "-n", "alpha")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "get all -n alpha exited %d: %s", code, stderr)
	var documents []map[string]any
	{
		err := json.Unmarshal([]byte(stdout), &documents)
		assert.FatalIf(t, err != nil, "get all emitted invalid JSON: %v\n%s", err, stdout)
	}
	assert.ErrorIf(
		t,
		len(documents) != 1 || documents[0]["kind"] != "Kustomization" || documents[0]["namespace"] != "alpha" ||
			documents[0]["name"] != "apps-a",
		"namespace-scoped objects = %#v, want alpha/apps-a",
		documents,
	)
}

// TestRun_GetImages_RejectsBadOutput verifies the -o diff rejection.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetImages_RejectsBadOutput(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "get", "images", "--path", path, "-o", "diff")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit")
	assert.ErrorIf(t, !strings.Contains(stderr, "must be one of"), "expected validation error, got %q", stderr)
}

func writeUnreadableSecretFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	k8s := filepath.Join(root, "kubernetes")
	testutil.WriteFileAt(
		t,
		filepath.Join(k8s, "flux", "cluster.yaml"),
		testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: Secret
        name: cluster-secrets
`),
	)
	testutil.WriteFileAt(
		t,
		filepath.Join(k8s, "apps", "producer.yaml"),
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: cluster-secrets, namespace: flux-system}\nspec: {target: {name: cluster-secrets}}\n",
	)
	testutil.WriteFileAt(
		t,
		filepath.Join(k8s, "apps", "kustomization.yaml"),
		"resources:\n- cm.yaml\n- producer.yaml\n",
	)
	testutil.WriteFileAt(t, filepath.Join(k8s, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: flux-system}
data:
  greeting: hi
`)
	return k8s
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_AdvisoryDestinations(t *testing.T) {
	k8s := writeUnreadableSecretFixture(t)

	for _, command := range [][]string{{"build", "all"}, {"get", "all"}, {"get", "ks"}, {"get", "images"}, {"test", "all"}} {
		t.Run(strings.Join(command, "/"), func(t *testing.T) { runAdvisoryDestinationsCase(t, &command, &k8s) })
	}
	cli := runCLI(t, "test", "ks", "absent", "--path", k8s, "--concurrency=2")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 1 || stdout != "" || !strings.Contains(stderr, "no Kustomization named"),
		"absent roster diagnostic: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_TestAll(t *testing.T) {
	path := writeFixture(t)
	testutil.WriteFile(
		t,
		path,
		"flux/hidden.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: ExternalArtifact\nmetadata: {name: hidden, namespace: flux-system}\nstatus: {artifact: {url: 'file://"+path+"'}}\n",
	)
	cli := runCLI(t, "test", "all", "--path", path)
	stdout, code := cli.stdout, cli.code
	assert.Equal(t, code, 1)
	assert.Equal(t, strings.Contains(stdout, "✓") && strings.Contains(stdout, "passed"), true)
	assert.Equal(t, strings.Contains(stdout, "failed") && strings.Contains(stdout, "hidden"), true)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_TestAll_RespectsNamespace(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	cli := runCLI(t, "test", "all", "--path", path, "-n", "alpha")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "test all -n alpha exited %d: %s", code, stderr)
	assert.ErrorIf(
		t,
		!strings.Contains(stdout, "alpha/apps-a"),
		"namespace-scoped test output missing alpha KS:\n%s",
		stdout,
	)
	assert.ErrorIf(
		t,
		strings.Contains(stdout, "beta/apps-b"),
		"namespace-scoped test output included beta KS:\n%s",
		stdout,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_TestAll_ReturnsStdoutWriteError(t *testing.T) {
	path := writeUnreadableSecretFixture(t)
	want := errors.New("stdout closed")
	var stderr bytes.Buffer
	code := Run([]string{"test", "all", "--path", path}, failingWriter{err: want}, &stderr)
	assert.FatalIf(t, code == 0, "%v", "expected non-zero for stdout write failure")
	assert.ErrorIf(
		t,
		!strings.Contains(stderr.String(), want.Error()),
		"stderr should include writer error %q: %q",
		want,
		stderr.String(),
	)
	assert.FatalIf(
		t,
		strings.Count(stderr.String(), "[UnresolvedSubstitution]") != 1,
		"stdout failure skipped warning report: %s",
		stderr.String(),
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_TestAll_JoinsVisibleFailuresWithRunError(t *testing.T) {
	path := writeFixture(t)
	assert.NoError(t, os.Remove(filepath.Join(path, "apps", "cm.yaml")))

	cli := runCLI(t, "test", "all", "--path", path)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero for failed reconcile")
	assert.ErrorIf(t, !strings.Contains(stdout, "✗"), "stdout should still include failed test row:\n%s", stdout)
	// `flate test` is one consolidated report on stdout: the failing resource
	// named with its real (primary) error on its roster row, plus the verdict.
	// The old separate stderr failure block — which double-printed failures and a
	// second, differently-counted verdict — is gone.
	assert.ErrorIf(
		t,
		!strings.Contains(stdout, "apps") || !strings.Contains(stdout, "kustomize build"),
		"stdout should surface the failing resource and its error, got %q",
		stdout,
	)
	assert.ErrorIf(t, !strings.Contains(stdout, "failed"), "stdout should include the failure verdict, got %q", stdout)
	assert.ErrorIf(
		t,
		strings.Contains(stderr, "failed"),
		"failures + verdict are consolidated on stdout; stderr must not carry a second verdict, got %q",
		stderr,
	)
}

// TestRun_DiffKS_NoOrigErrors covers the diff-without-path-orig path:
// diff must reject when no baseline is supplied.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffKS_NoOrigErrors(t *testing.T) {
	path := writeFixture(t)
	cli := runCLI(t, "diff", "ks", "--path", path)
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit for diff without --path-orig")
	assert.ErrorIf(t, !strings.Contains(stderr, "path-orig"), "error should mention --path-orig: %q", stderr)
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_RenderPersistenceNoticeInvocationScope(t *testing.T) {
	preservePersistenceNoticeState(t)
	current := writeFixture(t)
	orig := t.TempDir()
	testutil.CopyTree(t, current, orig)
	args := []string{
		"diff",
		"ks",
		"--path",
		current,
		"--path-orig",
		orig,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	}
	renderPersistenceDisabledReason = ""
	cli := runCLI(t, args...)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "identical-tree diff exited %d: %s", code, stderr)
	assert.ErrorIf(t, strings.Contains(stdout, "@@"), "identical tree should produce no hunks:\n%s", stdout)
	renderPersistenceDisabledReason = persistenceReason
	for range 2 {
		cli2 := runCLI(t, args...)
		got, gotErr, gotCode := cli2.stdout, cli2.stderr, cli2.code
		assert.Equal(t, gotCode, code)
		assert.Equal(t, got, stdout)
		assert.Equal(
			t,
			gotErr,
			strings.Replace(stderr, "warnings (2)\n", "warnings (3)\n    "+persistenceMessage+"\n", 1),
		)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffImages_RespectsNamespaceFailures(t *testing.T) {
	current := writeMultiNamespaceFixture(t)
	orig := t.TempDir()
	testutil.CopyTree(t, current, orig)
	assert.NoError(t, os.RemoveAll(filepath.Join(current, "apps-b")))
	assert.NoError(t, os.RemoveAll(filepath.Join(orig, "apps-b")))

	cli := runCLI(t, "diff", "images", "--path", current, "--path-orig", orig, "-n", "alpha")
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 0,
		"diff images -n alpha should ignore beta reconcile failure, exited %d: %s",
		code,
		stderr,
	)
}

// TestDiff_OutputStyles drives a non-trivial diff and asserts each dyff
// style plus the plain unified diff routes to its renderer when selected
// via -o, end-to-end.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
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
			cli := runCLI(t, "diff", "ks", "-o", tc.style,
				"--path", current, "--path-orig", origPath)
			stdout, stderr, code := cli.stdout, cli.stderr, cli.code
			assert.FatalIf(t, code != 0, "diff ks -o %s exited %d: %s", tc.style, code, stderr)
			assert.ErrorIf(
				t,
				!strings.Contains(stdout, tc.want),
				"-o %s output missing %q:\n%s",
				tc.style,
				tc.want,
				stdout,
			)
		})
	}

	// The default (no -o) is human — pins the breaking change away from
	// github. Compare the no-flag output to an explicit -o human run.
	cli2 := runCLI(t, "diff", "ks", "--path", current, "--path-orig", origPath)
	def, code := cli2.stdout, cli2.code
	assert.FatalIf(t, code != 0, "diff ks (default) exited %d", code)
	cli3 := runCLI(t, "diff", "ks", "-o", "human", "--path", current, "--path-orig", origPath)
	human := cli3.stdout
	assert.ErrorIf(
		t,
		def == "" || def != human,
		"default diff output should equal -o human:\ndefault:\n%s\nhuman:\n%s",
		def,
		human,
	)
}

type cliResult struct {
	stdout, stderr string
	code           int
}
