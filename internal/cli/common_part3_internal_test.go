package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestBindCommon_AllowMissingKinds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		args, want []string
	}{
		{name: "default"},
		{name: "Secret", args: []string{"--allow-missing-kinds=Secret"}, want: []string{manifest.KindSecret}},
		{name: "ConfigMap", args: []string{"--allow-missing-kinds=ConfigMap"}, want: []string{manifest.KindConfigMap}},
		{name: "CRD", args: []string{"--allow-missing-kinds=CustomResourceDefinition"}, want: []string{manifest.KindCustomResourceDefinition}},
		{name: "repeated CSV and duplicates", args: []string{"--allow-missing-kinds=Secret,ConfigMap", "--allow-missing-kinds=Secret,CustomResourceDefinition"}, want: []string{manifest.KindSecret, manifest.KindConfigMap, manifest.KindSecret, manifest.KindCustomResourceDefinition}},
		{name: "explicit empty", args: []string{"--allow-missing-kinds="}, want: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var flags commonFlags
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			bindCommon(fs, &flags)
			assert.NoError(t, fs.Parse(tc.args))
			assert.NoError(t, flags.loadInputs(t.Context()))
			assert.Diff(t, buildOrchCfg(&flags).AllowMissingKinds, tc.want)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_AllowMissingKindsValidation(t *testing.T) {
	root := substitutionFixture(t, "", "literal")
	for _, arg := range []string{"secret", "Unknown", "true", "Secret,,ConfigMap", "Secret,"} {
		t.Run(arg, func(t *testing.T) {
			_, diagnostic, code := runCLI(
				t,
				"build",
				"all",
				"--path",
				root,
				"--supply",
				filepath.Join(root, "absent.yaml"),
				"--allow-missing-kinds="+arg,
			)
			assert.FatalIf(t, code != 1 || !strings.Contains(diagnostic, "--allow-missing-kinds") ||
				strings.Contains(diagnostic, "--supply"), "validation after input I/O: exit=%d %s", code, diagnostic)
		})
	}
	_, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--allow-missing-kinds")
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(diagnostic, "needs an argument"),
		"bare selection accepted: exit=%d %s",
		code,
		diagnostic,
	)
}

func TestEnv_AllowMissingKindsPrecedence(t *testing.T) {
	root := substitutionFixture(t, "", "literal")
	for _, tc := range []struct {
		name, environment string
		flags             []string
		fail              bool
	}{
		{name: "CSV", environment: "Secret,ConfigMap,CustomResourceDefinition"},
		{name: "invalid", environment: "secret", fail: true},
		{name: "CLI replaces environment", environment: "invalid", flags: []string{"--allow-missing-kinds=Secret"}},
		{name: "empty replaces environment", environment: "invalid", flags: []string{"--allow-missing-kinds="}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_ALLOW_MISSING_KINDS", tc.environment)
			args := append([]string{"build", "all", "--path", root, "--concurrency=2"}, tc.flags...)
			_, diagnostic, code := runCLI(t, args...)
			assert.FatalIf(t, (code != 0) != tc.fail, "exit=%d %s", code, diagnostic)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_FlagInventory(t *testing.T) {
	names := map[string]bool{}
	for _, route := range []string{"", "build all", "get ks", "get images", "test all", "diff all", "diff images", "cache gc", "completion fish"} {
		args := append(strings.Fields(route), "--help")
		help, diagnostic, code := runCLI(t, args...)
		assert.FatalIf(t, code != 0, "help %q: %s", route, diagnostic)
		for line := range strings.SplitSeq(help, "\n") {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			token := fields[0]
			if len(fields) > 1 && strings.HasSuffix(token, ",") {
				token = fields[1]
			}
			if after, ok := strings.CutPrefix(token, "--"); ok {
				names[after] = true
			}
		}
		assert.FatalIf(t, strings.Contains(help, "--base diff"), "incorrect base metavar: %s", help)
	}
	if len(names) != 30 {
		t.Fatalf("distinct flags=%d: %v", len(names), slices.Sorted(maps.Keys(names)))
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ProviderCredentialPolicy(t *testing.T) {
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }),
	)
	t.Cleanup(server.Close)
	root := substitutionFixture(t, "", "local")
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, source.yaml, remote.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"source.yaml",
		fmt.Sprintf(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: cloud, namespace: apps}\nspec: {provider: aws, insecure: true, url: 'oci://%s/image'}\n",
			strings.TrimPrefix(server.URL, "https://"),
		),
	)
	testutil.WriteFile(
		t,
		root,
		"remote.yaml",
		testutil.KustomizationYAML(
			"remote",
			"apps",
			"  path: ./remote\n  sourceRef: {kind: OCIRepository, name: cloud}\n",
		),
	)
	registry := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, registry, `{"auths":{}}`)
	for _, tc := range []struct {
		name, selection string
		want            int
	}{
		{name: "default", want: 1}, {name: "ConfigMap", selection: "ConfigMap", want: 1}, {name: "Secret", selection: "Secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, diagnostic, code := runCLI(
				t,
				"build",
				"all",
				"--path",
				root,
				"--registry-config",
				registry,
				"--allow-missing-kinds="+tc.selection,
				"--concurrency=2",
			)
			assert.FatalIf(
				t,
				code != tc.want || !strings.Contains(out, "value: local"),
				"provider blocked unrelated output: exit=%d %s %s",
				code,
				out,
				diagnostic,
			)
			assert.FatalIf(
				t,
				code == 0 && !strings.Contains(diagnostic, "SourceCredentialsUnavailable"),
				"missing source warning: %s",
				diagnostic,
			)
		})
	}
}

func runSelectedProgressLifecycleName(t *testing.T, name *string) {
	t.Helper()

	oldBar, oldLogger, original := barSink, slog.Default(), os.Stderr
	var raw *os.File
	defer func() {
		captured := os.Stderr
		os.Stderr = original //nolint:reassign // The fixture restores process stderr after testing dependency-log capture.
		if captured != original && captured != raw {
			_ = captured.Close()
		}
		if raw != nil {
			{
				err := raw.Close()
				assert.ErrorIf(t, err != nil, "%v", err)
			}
		}
		barSink = oldBar
		slog.SetDefault(oldLogger)
	}()
	var out progressOutput
	barSink = &barWriter{out: &out}
	slog.SetDefault(slog.New(slog.NewTextHandler(barSink, &slog.HandlerOptions{Level: slog.LevelDebug})))
	path := filepath.Join(t.TempDir(), "absent")
	switch *name {
	case "fatal-construction":
		path = ""
	case "fatal-bootstrap":
	default:
		path = writeFixture(t)
	}
	if (*name) == "partial-render" {
		testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [missing.yaml]\n")
	}
	expected := original
	if (*name) == "render-warning" {
		testutil.WriteFile(
			t,
			path,
			"apps/kustomization.yaml",
			"resources: [cm.yaml]\ncommonLabels: {progress-warning: test}\n",
		)
		f, err := os.CreateTemp(t.TempDir(), "stderr")
		assert.NoError(t, err)
		raw = f
		os.Stderr = raw //nolint:reassign // The fixture redirects process stderr to exercise dependency-log capture.
		expected = raw
	}
	flags := commonFlags{path: path, concurrency: 2}
	var selected *selectedInvocation
	err := runSelected(t.Context(), selectedRequest{
		flags: flags, roots: flags.roots("", runnableKinds...), stdout: io.Discard, stderr: io.Discard,
	}, func(inv *selectedInvocation) selectedPayload {
		selected = inv
		return selectedPayload{}
	})
	switch *name {
	case "fatal-construction", "fatal-bootstrap":
		assert.FatalIf(
			t,
			err == nil || selected != nil,
			"fatal run exposed selection=%v, err=%v",
			selected,
			err,
		)
	case "partial-render":
		assert.FatalIf(t, err == nil || selected == nil || len(selected.groups) != 1 ||
			len(selected.groups[0].sides[0].Failed) == 0, "partial run lost selection: %+v", selected)
	default:
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			selected == nil || len(selected.groups) != 1 || len(selected.groups[0].sides[0].Objects) == 0,
			"%v",
			"successful render returned nil",
		)
	}
	if (*name) == "render-warning" {
		const warning = "# Warning: 'commonLabels' is deprecated."
		{
			got := out.String()
			assert.ErrorIf(t, !strings.Contains(got, warning), "render warning missing from sink: %q", got)
		}
		got, err := os.ReadFile(raw.Name())
		assert.NoError(t, err)
		assert.ErrorIf(
			t,
			strings.Contains(string(got), warning),
			"render warning leaked to raw stderr: %q",
			got,
		)
	}
	assert.ErrorIf(t, os.Stderr != expected, "%v", "shared helper left stderr captured")
	assert.ErrorIf(t, progressProgram(t, barSink) != nil, "%v", "shared helper left progress attached")
}

func renderTreesInputWarningOnlyCase(t *testing.T, warning *struct {
	Resource manifest.NamedResource
	Subject  manifest.DiagnosticIdentity
	Related  []manifest.NamedResource
	Category string
	Message  string
	Detail   []string
	Count    int
}, root *string, unavailable *bool,
) {
	t.Helper()

	baseWarning, headWarning := (*warning), (*warning)
	baseWarning.Message, headWarning.Message = "base before failure", "head before failure"
	base := &store.SourceArtifact{
		LocalPath: (*root),
		FS:        os.DirFS(filepath.Join((*root), "missing")),
		Warnings:  []manifest.Warning{baseWarning},
	}
	head := &store.SourceArtifact{
		LocalPath: (*root),
		FS:        os.DirFS((*root)),
		Warnings:  []manifest.Warning{headWarning},
	}
	wantErr := os.ErrNotExist
	if *unavailable {
		base.FS = nil
		wantErr = manifest.ErrInput
	}
	runs, err := orchestrator.RenderTrees(t.Context(), []orchestrator.Tree{
		{
			ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
			Path:            (*root),
			Input:           base,
			BaseInput:       head,
			ExternalChanges: change.NewSet(nil),
		},
		{
			ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			Path:            (*root),
			Input:           head,
			BaseInput:       base,
			ExternalChanges: change.NewSet(nil),
		},
	}, orchestrator.Config{Concurrency: 2, HeadOnly: true})
	assert.FatalIf(t, !errors.Is(err, wantErr), "baseline discovery error lost: %v", err)
	var warnings []manifest.Warning
	for _, run := range runs {
		assert.FatalIf(t, run.Orchestrator != nil || run.Result == nil ||
			len(
				run.Result.Manifests,
			)+len(
				run.Result.Failed,
			) != 0, "failed preparation exposed content: %+v", run)
		for wIndex := range run.Result.Warnings {
			w := run.Result.Warnings[wIndex]
			if w.Category == manifest.WarnBaselineStatusUnavailable {
				warnings = append(warnings, w)
			}
		}
	}
	assert.Diff(t, warnings, []manifest.Warning{baseWarning, headWarning})
	roots := []manifest.NamedResource{testutil.KustomizationID("", "")}
	sides, selectedErr, remainder := orchestrator.Select(runs, err, roots, nil)
	assert.FatalIf(
		t,
		!errors.Is(selectedErr, wantErr) || !errors.Is(remainder, wantErr) || len(sides) != len(runs),
		"preparation projection = %+v %v %v",
		sides,
		selectedErr,
		remainder,
	)
	var stderr bytes.Buffer
	finished := finishSelected(
		&selectedRequest{roots: roots, stderr: &stderr},
		&selectedInvocation{
			groups: []selectedGroup{{cluster: ".", sides: sides, err: selectedErr, remainder: remainder}},
		},
		&selectedPayload{},
	)
	if _, reported := errors.AsType[reportedError](finished); !reported {
		assert.NoError(t, (output.Encoder{Writer: &stderr}).Errors(finished))
	}
	for wIndex := range []manifest.Warning{baseWarning, headWarning} {
		w := []manifest.Warning{baseWarning, headWarning}[wIndex]
		assert.Equal(t, strings.Count(stderr.String(), w.Message), 1)
	}
	if !(*unavailable) {
		cause, ok := errors.AsType[*os.PathError](err)
		assert.FatalIf(
			t,
			!ok || strings.Count(stderr.String(), cause.Error()) != 1,
			"baseline discovery cause was duplicated: %v\n%s",
			err,
			stderr.String(),
		)
	}
}
