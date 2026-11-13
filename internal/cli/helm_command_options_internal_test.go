package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_HelmOptions(t *testing.T) {
	root := writeHelmOptionsFixture(
		t,
		`{{- if or (ne .Capabilities.KubeVersion.Version "v1.31.2") (not (.Capabilities.APIVersions.Has "fixture.example/v1")) (or .Release.IsUpgrade (not .Release.IsInstall)) }}
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
`,
	)
	for _, name := range []string{"extra", "hook"} {
		annotation := ""
		if name == "hook" {
			annotation = `, annotations: {helm.sh/hook: pre-install}`
		}
		testutil.WriteFile(
			t,
			root,
			"charts/app/templates/"+name+".yaml",
			"apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: "+name+annotation+"}\nspec: {selector: {matchLabels: {app: "+name+"}}, template: {metadata: {labels: {app: "+name+"}}, spec: {containers: [{name: "+name+", image: 'fixture/"+name+":{{ .Values.tag }}'}]}}}\n",
		)
	}
	original := t.TempDir()
	testutil.CopyTree(t, root, original)
	hr, err := os.ReadFile(filepath.Join(original, "cluster", "hr.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, original, "cluster/hr.yaml", strings.ReplaceAll(string(hr), "tag: after", "tag: before"))
	flags := []string{
		"--path",
		filepath.Join(root, "cluster"),
		"--concurrency=2",
		"--kube-version=1.31.2",
		"-a",
		"fixture.example/v1",
	}
	for _, command := range []string{"build hr", "get all", "get hr", "get images", "test hr", "diff hr", "diff images"} {
		t.Run(command, func(t *testing.T) { runHelmOptionsCommand(t, &command, &flags, &original) })
	}
	for _, tc := range []struct {
		name         string
		disableHooks bool
		extra, hook  bool
	}{
		{name: "hooks included", extra: true, hook: true},
		{name: "hooks excluded", disableHooks: true, extra: true},
	} {
		t.Run(tc.name, func(t *testing.T) { runHelmOptionsName(t, &flags, &root, &tc) })
	}
}

func runHelmOptionsCommand(t *testing.T, command *string, flags *[]string, original *string) {
	t.Helper()

	args := append(strings.Fields((*command)), (*flags)...)
	if strings.HasPrefix((*command), "diff") {
		args = append(args, "--path-orig", filepath.Join((*original), "cluster"))
	}
	cli := runCLI(t, args...)
	out, diagnostic, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 0 || out == "",
		"Helm options: exit=%d stdout=%s stderr=%s",
		code,
		out,
		diagnostic,
	)
	if (*command) == "build hr" {
		for _, want := range []string{"fixture/kube: v1.31.2", "fixture/api: \"true\"", "fixture/upgrade: \"false\"", "fixture/main:after"} {
			assert.ErrorIf(t, !strings.Contains(out, want), "missing %q in %s", want, out)
		}
	}
	assert.FatalIf(t, strings.HasSuffix((*command), "images") &&
		(!strings.Contains(out, "fixture/main") || !strings.Contains(out, "fixture/extra") || !strings.Contains(out, "fixture/hook")), "image filtering lost Helm options: %s", out)
}

func runHelmOptionsName(t *testing.T, flags *[]string, root *string, tc *struct {
	name         string
	disableHooks bool
	extra        bool
	hook         bool
},
) {
	t.Helper()

	args := append([]string{"build", "hr"}, (*flags)...)
	local := t.TempDir()
	testutil.CopyTree(t, (*root), local)
	if tc.disableHooks {
		path := filepath.Join(local, "cluster", "hr.yaml")
		body, err := os.ReadFile(path)
		assert.NoError(t, err)
		testutil.WriteFileAt(t, path, string(body)+"  install: {disableHooks: true}\n")
	}
	args = append(args, "--path", filepath.Join(local, "cluster"))
	cli := runCLI(t, args...)
	out, diagnostic, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 0 || !strings.Contains(out, "name: main") || strings.Contains(out, "name: extra") != tc.extra ||
			strings.Contains(
				out,
				"name: hook",
			) != tc.hook,
		"template filtering: exit=%d stdout=%s stderr=%s",
		code,
		out,
		diagnostic,
	)
}

func writeHelmOptionsFixture(t *testing.T, template string) string {
	t.Helper()
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
	testutil.WriteFile(t, root, "charts/app/templates/main.yaml", template)
	return root
}
