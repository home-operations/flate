package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
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
				{
					err := root.ExecuteContext(ctx)
					assert.FatalIf(t, !errors.Is(err, context.Canceled), "handler lost cancellation: %v", err)
				}
			})
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
	testutil.WriteFile(
		t,
		root,
		"charts/app/templates/main.yaml",
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
	hr, err := os.ReadFile(filepath.Join(original, "cluster/hr.yaml"))
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

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ConciseFailureDiagnostics(t *testing.T) {
	current, orig := writeFixture(t), writeFixture(t)
	testutil.WriteFile(
		t,
		current,
		"flux/cluster.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild: {substitute: {flate-campaign: value}}\n",
		),
	)
	for _, command := range []string{"test", "build", "diff"} {
		t.Run(command, func(t *testing.T) { runConciseFailureDiagnosticsCommand(t, &command, &current, &orig) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ConciseEntryFailureDiagnostics(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [missing.yaml]\n")
	var first string
	for _, level := range []string{"error", "debug"} {
		_, stderr, code := runCLI(
			t,
			"build",
			"all",
			"--path",
			root,
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
			"--log-level",
			level,
		)
		assert.FatalIf(t, code != 1, "exit=%d stderr=%s", code, stderr)
		found := false
		for row := range strings.SplitSeq(stderr, "\n") {
			if !strings.Contains(row, "✗") || !strings.Contains(row, "missing.yaml") {
				continue
			}
			found = true
			assert.FatalIf(t, first != "" && first != row, "debug changed entry failure: %q != %q", row, first)
			first = row
		}
		assert.FatalIf(t, !found || !strings.Contains(stderr, "missing.yaml"), "missing entry failure: %s", stderr)
		for _, message := range []string{"orchestrator: entry failed", "cli: invocation failed"} {
			assert.FatalIf(
				t,
				strings.Contains(stderr, message) != (level == "debug"),
				"diagnostic %q at %s: %s",
				message,
				level,
				stderr,
			)
		}
	}
}

func runGetKSName(t *testing.T, tc *struct {
	name     string
	path     string
	wantPath string
},
) {
	t.Helper()

	root := t.TempDir()
	pathField := ""
	if tc.path != "" {
		pathField = "  path: " + tc.path + "\n"
	}
	files := map[string]string{
		"kustomization.yaml": "resources: [cluster.yaml]\n",
		"cluster.yaml": testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  interval: 10m\n"+pathField+"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
		"apps/kustomization.yaml": "resources: [cm.yaml]\n",
		"apps/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\n",
	}
	for name, content := range files {
		testutil.WriteFile(t, root, name, content)
	}
	for _, command := range []string{"get ks", "get", "get all"} {
		t.Run(command, func(t *testing.T) {
			runGetKSNameCommand(t, &command, &root, struct {
				tc **struct {
					name     string
					path     string
					wantPath string
				}
				files *map[string]string
			}{tc: &tc, files: &files})
		})
	}
}

func runAdvisoryDestinationsCase(t *testing.T, command *[]string, k8s *string) {
	t.Helper()
	const advisory = "could not be read offline"

	var firstOut, firstErr string
	for run := range 2 {
		args := append(slices.Clone((*command)), "--path", (*k8s), "--concurrency", "2")
		if (*command)[0] != "test" {
			args = append(args, "-o", "json")
		}
		stdout, stderr, code := runCLI(t, args...)
		assert.Equal(t, code, 0)
		diagnostic, data := stderr, stdout
		assert.Equal(t, strings.Count(diagnostic, advisory), 1)
		assert.Equal(t, strings.Count(diagnostic, "[UnresolvedSubstitution]"), 1)
		assert.Equal(t, strings.Contains(data, advisory), false)
		normalized := stdout
		if (*command)[0] == "test" {
			normalized = regexp.MustCompile(`   [0-9]+\.[0-9]+s\n$`).ReplaceAllString(stdout, "")
		} else {
			assert.FatalIf(t, !json.Valid([]byte(stdout)), "invalid payload: %s", stdout)
		}
		if run == 0 {
			firstOut, firstErr = normalized, stderr
		} else {
			assert.Equal(t, normalized, firstOut)
			assert.Equal(t, stderr, firstErr)
		}
	}
	for _, level := range []string{"debug", "info", "warn", "error"} {
		args := append(slices.Clone((*command)), "--path", (*k8s), "--concurrency=2", "--log-level", level)
		stdout, stderr, code := runCLI(t, args...)
		assert.FatalIf(t, code != 0 || strings.Count(stderr, "[UnresolvedSubstitution]") != 1 ||
			strings.Count(stderr, advisory) != 1 ||
			strings.Contains(stdout, advisory) ||
			strings.Contains(
				stderr,
				"level=WARN",
			), "%s destination: exit=%d stdout=%s stderr=%s", level, code, stdout, stderr)
	}
}

func runImagesGetAndDiffFormatsMatchFormat(t *testing.T, current *string, cache *string, capturedInputs struct {
	format *string
	orig   *string
	want   *[]string
},
) {
	t.Helper()

	flags := make([]string, 0, 8)
	flags = append(flags, "--path", (*current), "--cache-dir", (*cache), "--concurrency", "2")
	flags = append(flags, "-o", (*capturedInputs.format))
	get, stderr, code := runCLI(t, append([]string{"get", "images"}, flags...)...)
	assert.FatalIf(t, code != 0, "get images exited %d: %s", code, stderr)
	diffArgs := append([]string{"diff", "images", "--path-orig", (*capturedInputs.orig)}, flags...)
	delta, stderr, code := runCLI(t, diffArgs...)
	assert.FatalIf(t, code != 0, "diff images exited %d: %s", code, stderr)
	assert.Diff(t, delta, get)
	var images []string
	if (*capturedInputs.format) == "name" {
		assert.Equal(t, get, strings.Join((*capturedInputs.want), "\n")+"\n")
		images = strings.Split(strings.TrimSuffix(get, "\n"), "\n")
	} else {
		prefix := "- "
		wire := "- " + strings.Join((*capturedInputs.want), "\n- ") + "\n"
		if (*capturedInputs.format) == "json" {
			prefix = "["
			wire = "[\n  \"" + strings.Join((*capturedInputs.want), "\",\n  \"") + "\"\n]\n"
		}
		assert.FatalIf(
			t,
			!strings.HasPrefix(get, prefix) || !strings.HasPrefix(delta, prefix),
			"wrong %s wire format: get=%q diff=%q",
			(*capturedInputs.format),
			get,
			delta,
		)
		assert.Equal(t, get, wire)
		assert.NoError(t, yaml.Unmarshal([]byte(get), &images))
	}
	assert.Diff(t, images, (*capturedInputs.want))
}

func runImagesGetAndDiffFormatsMatchNoiseGet(t *testing.T, noise *string, cache *string, want *[]string) {
	t.Helper()

	stdout, stderr, code := runCLI(
		t,
		"get",
		"images",
		"--path",
		(*noise),
		"--cache-dir",
		(*cache),
		"--concurrency",
		"2",
	)
	assert.FatalIf(t, code != 0, "noise get images: exit=%d stderr=%s", code, stderr)
	assert.Equal(t, stdout, "IMAGE\n"+strings.Join((*want), "\n")+"\n")
}

func runImagesGetAndDiffFormatsMatchName(t *testing.T, tc *struct {
	name           string
	current        string
	orig           string
	includeRemoved bool
	want           string
}, cache *string,
) {
	t.Helper()

	args := []string{
		"diff",
		"images",
		"--path",
		tc.current,
		"--path-orig",
		tc.orig,
		"--cache-dir",
		(*cache),
		"--concurrency",
		"2",
	}
	if tc.includeRemoved {
		args = append(args, "--include-removed")
	}
	stdout, stderr, code := runCLI(t, args...)
	assert.FatalIf(t, code != 0, "diff images: exit=%d stderr=%s", code, stderr)
	assert.Equal(t, stdout, tc.want)
}

func runImagesGetAndDiffFormatsMatchEmptyDefault(t *testing.T, orig *string, cache *string) {
	t.Helper()

	for _, verb := range []string{"get", "diff"} {
		t.Run(verb, func(t *testing.T) { runImagesGetAndDiffFormatsMatchEmptyDefaultVerb(t, &verb, &orig, &cache) })
	}
}

func runImagesGetAndDiffFormatsMatchUnchangedJSON(t *testing.T, current *string, cache *string) {
	t.Helper()

	stdout, stderr, code := runCLI(t, "diff", "images", "--path", (*current), "--path-orig", (*current),
		"--cache-dir", (*cache), "--concurrency", "2", "-o", "json")
	assert.FatalIf(t, code != 0, "unchanged diff images: exit=%d stderr=%s", code, stderr)
	{
		got := strings.TrimSpace(stdout)
		assert.FatalIf(t, got != "[]" && got != "null", "unchanged image diff = %q, want [] or null", stdout)
	}
}

func runHelmOptionsCommand(t *testing.T, command *string, flags *[]string, original *string) {
	t.Helper()

	args := append(strings.Fields((*command)), (*flags)...)
	if strings.HasPrefix((*command), "diff") {
		args = append(args, "--path-orig", filepath.Join((*original), "cluster"))
	}
	out, diagnostic, code := runCLI(t, args...)
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
		path := filepath.Join(local, "cluster/hr.yaml")
		body, err := os.ReadFile(path)
		assert.NoError(t, err)
		testutil.WriteFileAt(t, path, string(body)+"  install: {disableHooks: true}\n")
	}
	args = append(args, "--path", filepath.Join(local, "cluster"))
	out, diagnostic, code := runCLI(t, args...)
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

func runConciseFailureDiagnosticsCommand(t *testing.T, command *string, current *string, orig *string) {
	t.Helper()

	var first string
	for _, level := range []string{"error", "debug"} {
		args := []string{
			(*command),
			"all",
			"--path",
			(*current),
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
			"--log-level",
			level,
		}
		if (*command) == "diff" {
			args = append(args, "--path-orig", (*orig))
		}
		stdout, stderr, code := runCLI(t, args...)
		assert.FatalIf(t, code != 1, "exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		report := stderr
		if (*command) == "test" {
			report = stdout
		}
		rows := strings.Split(report, "\n")
		found := false
		for _, row := range rows {
			if !strings.Contains(row, "✗") || !strings.Contains(row, "flux-system/apps") {
				continue
			}
			found = true
			assert.FatalIf(t, strings.Count(row, "flux-system/apps") != 1 ||
				strings.Count(row, "invalid substitute variable flate-campaign") != 1 ||
				strings.Contains(row, "flux error") ||
				strings.Contains(row, "invalid substitute reference"), "failure row: %s", row)
			assert.FatalIf(t, first != "" && row != first, "debug changed the report: %q != %q", row, first)
			first = row
		}
		assert.FatalIf(t, !found, "missing failure row: %s", report)
		assert.FatalIf(
			t,
			strings.Contains(stderr, "must match") != (level == "debug"),
			"debug details at %s: %s",
			level,
			stderr,
		)
	}
}

func runGetKSNameCommand(t *testing.T, command *string, root *string, capturedInputs struct {
	tc **struct {
		name     string
		path     string
		wantPath string
	}
	files *map[string]string
},
) {
	t.Helper()

	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			args := append(
				strings.Fields((*command)),
				"--path",
				(*root),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
				"-o",
				format,
			)
			stdout, stderr, code := runCLI(t, args...)
			assert.FatalIf(t, code != 0, "exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			if format == "table" {
				name, width := "apps", 8
				if (*command) != "get ks" {
					name, width = "Kustomization/apps", 22
				}
				want := fmt.Sprintf(
					"NAMESPACE      %-*sPATH\nflux-system    %-*s%s\n",
					width,
					"NAME",
					width,
					name,
					(*capturedInputs.tc).wantPath,
				)
				assert.Equal(t, stdout, want)
			} else {
				groups := decodeGetDocuments(t, stdout, format, []string{""})
				assert.Diff(t, groups[0].Documents, []map[string]any{
					{
						"kind":      "Kustomization",
						"name":      "apps",
						"namespace": "flux-system",
						"path":      (*capturedInputs.tc).path,
						"sourceRef": map[string]any{
							"kind":      "GitRepository",
							"name":      "flux-system",
							"namespace": "flux-system",
						},
						"prune": false,
						"wait":  false,
					},
				})
			}
			for name, want := range *capturedInputs.files {
				got, err := os.ReadFile(filepath.Join((*root), name))
				assert.NoError(t, err)
				assert.Equal(t, string(got), want)
			}
		})
	}
}

func runImagesGetAndDiffFormatsMatchEmptyDefaultVerb(t *testing.T, verb *string, orig **string, cache **string) {
	t.Helper()

	args := []string{(*verb), "images", "--path", (*(*orig)), "--cache-dir", (*(*cache)), "--concurrency", "2"}
	if (*verb) == "diff" {
		args = append(args, "--path-orig", writeFixture(t))
	}
	stdout, stderr, code := runCLI(t, args...)
	want := ""
	if (*verb) == "get" {
		want = "IMAGE\n"
	}
	assert.FatalIf(
		t,
		code != 0 || stdout != want,
		"empty %s images: exit=%d stdout=%q stderr=%s",
		(*verb),
		code,
		stdout,
		stderr,
	)
	for _, format := range []string{"name", "yaml", "json"} {
		out, diagnostics, status := runCLI(t, append(args, "-o", format)...)
		expected := ""
		if format != "name" {
			expected = "[]\n"
			if (*verb) == "get" {
				expected = "null\n"
			}
		}
		assert.FatalIf(
			t,
			status != 0 || out != expected,
			"empty %s/%s: exit=%d stdout=%q stderr=%s",
			(*verb),
			format,
			status,
			out,
			diagnostics,
		)
	}
}
