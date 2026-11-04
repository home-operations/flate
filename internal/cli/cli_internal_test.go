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

// runCLI drives cli.Run inside the test binary and returns
// (stdout, stderr, exitCode). All tests use this so end-to-end
// coverage of the cobra tree counts against pkg internal/cli.
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
	runCLIResult := runCLI(t, "--help")
	stdout, code := runCLIResult.stdout, runCLIResult.code
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
			runCLIResult2 := runCLI(t, append([]string{group, "all"}, flags...)...)
			want, wantErr, wantCode := runCLIResult2.stdout, runCLIResult2.stderr, runCLIResult2.code
			runCLIResult3 := runCLI(t, append([]string{group}, flags...)...)
			got, gotErr, gotCode := runCLIResult3.stdout, runCLIResult3.stderr, runCLIResult3.code
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
			runCLIResult4 := runCLI(t, group, "frobnicate")
			stderr, code := runCLIResult4.stderr, runCLIResult4.code
			assert.ErrorIf(
				t,
				code != 1 || !strings.Contains(stderr, `unknown command "frobnicate"`),
				"unknown subcommand: exit %d, stderr %q",
				code,
				stderr,
			)
			runCLIResult5 := runCLI(t, group, "--help")
			help, stderr, code := runCLIResult5.stdout, runCLIResult5.stderr, runCLIResult5.code
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
	runCLIResult6 := runCLI(t, "build", "all", "--log-level", "bogus", "--path", ".")
	stderr, code := runCLIResult6.stderr, runCLIResult6.code
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
	runCLIResult7 := runCLI(t, "build", "all", "--path", "/nonexistent/path/here")
	stderr, code := runCLIResult7.stderr, runCLIResult7.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit for missing path")
	assert.ErrorIf(t, !strings.Contains(stderr, "flate error"), "error message missing prefix: %q", stderr)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BuildAll(t *testing.T) {
	path := writeFixture(t)
	runCLIResult8 := runCLI(t, "build", "all", "--path", path)
	stdout, stderr, code := runCLIResult8.stdout, runCLIResult8.stderr, runCLIResult8.code
	assert.FatalIf(t, code != 0, "build all exited %d: stderr=%s", code, stderr)
	for _, want := range []string{"kind: ConfigMap", "name: hello", "greeting: hi"} {
		assert.ErrorIf(t, !strings.Contains(stdout, want), "build output missing %q:\n%s", want, stdout)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BuildAll_JSONSingleDocument(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	runCLIResult9 := runCLI(t, "build", "all", "--path", path, "-o", "json")
	stdout, stderr, code := runCLIResult9.stdout, runCLIResult9.stderr, runCLIResult9.code
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
	runCLIResult10 := runCLI(t, "build", "ks", "--path", path, "-o", "name")
	stderr, code := runCLIResult10.stderr, runCLIResult10.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit for -o name on build")
	assert.ErrorIf(t, !strings.Contains(stderr, "must be one of"), "error message missing 'must be one of': %q", stderr)
}

// TestRun_BuildAll_OnlyKinds exercises the --only-kinds gate: a fixture
// without any CRDs should emit nothing but still exit 0.
//

func TestRun_BuildAll_OnlyKinds(t *testing.T) {
	path := writeFixture(t)
	runCLIResult11 := runCLI(t, "build", "all", "--path", path, "--only-kinds", "CustomResourceDefinition")
	stdout, stderr, code := runCLIResult11.stdout, runCLIResult11.stderr, runCLIResult11.code
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
			runCLIResult12 := runCLI(t, "build", "ks", "apps", "--path", path,
				"--only-kinds", "CustomResourceDefinition", "--cache-dir", t.TempDir(), "--concurrency", "2")
			stdout, stderr, code := runCLIResult12.stdout, runCLIResult12.stderr, runCLIResult12.code
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
			runCLIResult13 := runCLI(t, args...)
			out, diagnostic, code := runCLIResult13.stdout, runCLIResult13.stderr, runCLIResult13.code
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
	runCLIResult14 := runCLI(
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
	stdout, stderr, code := runCLIResult14.stdout, runCLIResult14.stderr, runCLIResult14.code
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
	runCLIResult15 := runCLI(t, "get", "ks", "apps", "--path", path)
	stdout, code := runCLIResult15.stdout, runCLIResult15.code
	assert.FatalIf(t, code != 0, "get ks apps exited %d", code)
	assert.ErrorIf(t, !strings.Contains(stdout, "apps"), "name filter dropped the matching object:\n%s", stdout)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetKS_NameFilter_LabelMismatchIsEmpty(t *testing.T) {
	path := writeFixture(t)
	runCLIResult16 := runCLI(t, "get", "ks", "apps", "--path", path, "-l", "app.kubernetes.io/name=other")
	stdout, stderr, code := runCLIResult16.stdout, runCLIResult16.stderr, runCLIResult16.code
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
	runCLIResult17 := runCLI(t, "get", "ks", "apps", "--path", path, "-n", "other")
	stderr, code := runCLIResult17.stderr, runCLIResult17.code
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
	runCLIResult18 := runCLI(t, "get", "ks", "--path", path, "-o", "yaml")
	stdout, code := runCLIResult18.stdout, runCLIResult18.code
	assert.FatalIf(t, code != 0, "get ks -o yaml exited %d", code)
	for _, want := range []string{"kind: Kustomization", "name: apps"} {
		assert.ErrorIf(t, !strings.Contains(stdout, want), "yaml output missing %q:\n%s", want, stdout)
	}
	for _, output := range []string{"yaml", "json"} {
		runCLIResult19 := runCLI(t, "get", "ks", "nosrc", "--path", path, "-o", output)
		stdout, stderr, code := runCLIResult19.stdout, runCLIResult19.stderr, runCLIResult19.code
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
	runCLIResult20 := runCLI(t, "get", "all", "--path", path, "-o", "diff")
	stderr, code := runCLIResult20.stderr, runCLIResult20.code
	assert.FatalIf(t, code == 0, "%v", "expected non-zero exit")
	assert.ErrorIf(t, !strings.Contains(stderr, "must be one of"), "expected validation error, got %q", stderr)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_Yaml(t *testing.T) {
	path := writeFixture(t)
	runCLIResult21 := runCLI(t, "get", "all", "--path", path, "-o", "yaml")
	stdout, code := runCLIResult21.stdout, runCLIResult21.code
	assert.FatalIf(t, code != 0, "get all exited %d", code)
	assert.ErrorIf(t, !strings.Contains(stdout, "kind: Kustomization\n") || !strings.Contains(stdout, "name: apps\n") ||
		strings.Contains(stdout, "kustomizations:"), "object listing missing Kustomization:\n%s", stdout)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_RespectsNamespace(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	runCLIResult22 := runCLI(t, "get", "all", "--path", path, "-o", "json", "-n", "alpha")
	stdout, stderr, code := runCLIResult22.stdout, runCLIResult22.stderr, runCLIResult22.code
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
	runCLIResult23 := runCLI(t, "get", "images", "--path", path, "-o", "diff")
	stderr, code := runCLIResult23.stderr, runCLIResult23.code
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
	runCLIResult24 := runCLI(t, "test", "ks", "absent", "--path", k8s, "--concurrency=2")
	stdout, stderr, code := runCLIResult24.stdout, runCLIResult24.stderr, runCLIResult24.code
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
	runCLIResult25 := runCLI(t, "test", "all", "--path", path)
	stdout, code := runCLIResult25.stdout, runCLIResult25.code
	assert.Equal(t, code, 1)
	assert.Equal(t, strings.Contains(stdout, "✓") && strings.Contains(stdout, "passed"), true)
	assert.Equal(t, strings.Contains(stdout, "failed") && strings.Contains(stdout, "hidden"), true)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_TestAll_RespectsNamespace(t *testing.T) {
	path := writeMultiNamespaceFixture(t)
	for _, tc := range []struct {
		name, roster, counts string
		selection            []string
	}{
		{name: "namespace", selection: []string{"all", "-n", "alpha"}, roster: "✓  Kustomization  alpha/apps-a", counts: "✓ 1 passed"},

		{name: "named", selection: []string{"ks", "apps-a"}, roster: "✓  Kustomization  alpha/apps-a", counts: "✓ 1 passed"},

		{
			name: "all",
			selection: []string{
				"all",
			},
			roster: "✓  Kustomization  alpha/apps-a\n  ✓  Kustomization  beta/apps-b",
			counts: "✓ 2 passed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"test"}, tc.selection...)
			args = append(args, "--path", path, "--concurrency", "2", "--cache-dir", t.TempDir())
			var priorWarnings string
			for run := range 2 {
				runCLIResult26 := runCLI(t, args...)
				stdout, stderr, code := runCLIResult26.stdout, runCLIResult26.stderr, runCLIResult26.code
				if code != 0 {
					t.Fatalf("scoped test failed: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				roster, verdict, _ := strings.Cut(strings.TrimSpace(stdout), "\n\n")
				counts, _, _ := strings.Cut(strings.TrimSpace(verdict), "   ")
				assert.Equal(t, roster, tc.roster)
				assert.Equal(t, counts, tc.counts)
				if run != 0 {
					assert.Equal(t, stderr, priorWarnings)
				}
				priorWarnings = stderr
			}
		})
	}
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

	runCLIResult27 := runCLI(t, "test", "all", "--path", path)
	stdout, stderr, code := runCLIResult27.stdout, runCLIResult27.stderr, runCLIResult27.code
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
	runCLIResult28 := runCLI(t, "diff", "ks", "--path", path)
	stderr, code := runCLIResult28.stderr, runCLIResult28.code
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
	runCLIResult29 := runCLI(t, args...)
	stdout, stderr, code := runCLIResult29.stdout, runCLIResult29.stderr, runCLIResult29.code
	assert.FatalIf(t, code != 0, "identical-tree diff exited %d: %s", code, stderr)
	assert.ErrorIf(t, strings.Contains(stdout, "@@"), "identical tree should produce no hunks:\n%s", stdout)
	renderPersistenceDisabledReason = persistenceReason
	for range 2 {
		runCLIResult30 := runCLI(t, args...)
		got, gotErr, gotCode := runCLIResult30.stdout, runCLIResult30.stderr, runCLIResult30.code
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
	for _, side := range []struct{ root, version string }{{orig, "v1"}, {current, "v2"}} {
		for _, app := range []struct{ path, namespace string }{{"apps-a", "alpha"}, {"apps-b", "beta"}} {
			testutil.WriteFile(
				t,
				side.root,
				app.path+"/cm.yaml",
				"apiVersion: v1\nkind: Pod\n"+
					"metadata: {name: image, namespace: "+app.namespace+"}\nspec:\n"+
					"  containers: [{name: app, image: example.test/"+app.namespace+":"+side.version+"}]\n",
			)
		}
	}
	for _, tc := range []struct {
		name, want string
		flags      []string
	}{
		{name: "namespace/additions", flags: []string{"-n", "alpha"}, want: "example.test/alpha:v2\n"},

		{
			name: "namespace/removals",
			flags: []string{
				"-n",
				"alpha",
				"--include-removed",
			},
			want: "example.test/alpha:v1\nexample.test/alpha:v2\n",
		},

		{name: "all", want: "example.test/alpha:v2\nexample.test/beta:v2\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{
				"diff",
				"images",
				"--path",
				current,
				"--path-orig",
				orig,
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			args = append(args, tc.flags...)
			var priorWarnings string
			for run := range 2 {
				runCLIResult31 := runCLI(t, args...)
				stdout, stderr, code := runCLIResult31.stdout, runCLIResult31.stderr, runCLIResult31.code
				assert.Equal(t, code, 0)
				assert.Equal(t, stdout, tc.want)
				if run != 0 {
					assert.Equal(t, stderr, priorWarnings)
				}
				priorWarnings = stderr
			}
		})
	}
	if err := os.RemoveAll(filepath.Join(current, "apps-b")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(orig, "apps-b")); err != nil {
		t.Fatal(err)
	}
	appendScopeSpec(t, current, "flux/apps-b.yaml", "  postBuild: {substitute: {TRIGGER: changed}}\n")

	runCLIResult32 := runCLI(
		t,
		"diff",
		"images",
		"--path",
		current,
		"--path-orig",
		orig,
		"-n",
		"alpha",
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	)
	stdout, stderr, code := runCLIResult32.stdout, runCLIResult32.stderr, runCLIResult32.code
	if code != 0 || stdout != "example.test/alpha:v2\n" || strings.Contains(stderr, "beta") ||
		strings.Contains(stderr, "apps-b") {
		t.Fatalf("unrelated failure changed scoped images: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	runCLIResult33 := runCLI(
		t,
		"diff",
		"images",
		"--path",
		current,
		"--path-orig",
		orig,
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	)
	stdout, stderr, code = runCLIResult33.stdout, runCLIResult33.stderr, runCLIResult33.code
	if code != 1 || stdout != "example.test/alpha:v2\n" || !strings.Contains(stderr, "beta/apps-b") {
		t.Fatalf("full image diff lost admitted failure: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
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
			runCLIResult34 := runCLI(t, "diff", "ks", "-o", tc.style,
				"--path", current, "--path-orig", origPath)
			stdout, stderr, code := runCLIResult34.stdout, runCLIResult34.stderr, runCLIResult34.code
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
	runCLIResult35 := runCLI(t, "diff", "ks", "--path", current, "--path-orig", origPath)
	def, code := runCLIResult35.stdout, runCLIResult35.code
	assert.FatalIf(t, code != 0, "diff ks (default) exited %d", code)
	runCLIResult36 := runCLI(t, "diff", "ks", "-o", "human", "--path", current, "--path-orig", origPath)
	human := runCLIResult36.stdout
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
