package scope_test

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_SkipKindsAppliesToKustomizationOutput(t *testing.T) {
	root := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "skip-kinds"), root)
	for _, tc := range []struct {
		name                        string
		args                        []string
		env                         map[string]string
		wantCM, wantSecret, wantCRD bool
	}{
		{name: "defaults", wantCM: true},
		{name: "empty skip", args: []string{"--skip-kinds", ""}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "replacement skip", args: []string{"--skip-kinds", "ConfigMap"}, wantSecret: true, wantCRD: true},
		{name: "only CRDs", args: []string{"--only-kinds", "CustomResourceDefinition"}, wantCRD: true},
		{name: "only Secret", args: []string{"--only-kinds", "Secret"}, wantSecret: true},
		{name: "only multiple", args: []string{"--only-kinds", "ConfigMap,Secret"}, wantCM: true, wantSecret: true},
		{name: "empty only", args: []string{"--only-kinds", ""}, wantCM: true},
		{name: "exact kind", args: []string{"--only-kinds", "secret"}},
		{name: "exact exclusion", args: []string{"--skip-kinds", "secret,customresourcedefinition"}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "env empty only", env: map[string]string{"FLATE_ONLY_KINDS": ""}, wantCM: true},
		{name: "argv overrides empty env skip", args: []string{"--skip-kinds", "Secret,CustomResourceDefinition"}, env: map[string]string{"FLATE_SKIP_KINDS": ""}, wantCM: true},
		{name: "explicit skip wins", args: []string{"--skip-kinds", "Secret,CustomResourceDefinition", "--only-kinds", "CustomResourceDefinition"}},
		{name: "env skip wins", args: []string{"--only-kinds", "CustomResourceDefinition"}, env: map[string]string{"FLATE_SKIP_KINDS": "CustomResourceDefinition"}},
		{name: "env only", env: map[string]string{"FLATE_ONLY_KINDS": "Secret"}, wantSecret: true},
		{name: "env empty skip", env: map[string]string{"FLATE_SKIP_KINDS": ""}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "argv overrides env skip", args: []string{"--skip-kinds", ""}, env: map[string]string{"FLATE_SKIP_KINDS": "ConfigMap"}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "argv empty only overrides env", args: []string{"--only-kinds", ""}, env: map[string]string{"FLATE_ONLY_KINDS": "Secret"}, wantCM: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			args := append(
				[]string{
					"build",
					"ks",
					"apps",
					"--path",
					root,
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
					"--concurrency",
					"2",
				},
				tc.args...)
			out := harness.RunCLIStdout(t, args...)
			for kind, want := range map[string]bool{"ConfigMap": tc.wantCM, "Secret": tc.wantSecret, "CustomResourceDefinition": tc.wantCRD} {
				{
					got := strings.Contains(out, "kind: "+kind+"\n")
					assert.ErrorIf(t, got != want, "kind %s present=%t, want %t: %s", kind, got, want, out)
				}
			}
		})
	}
}
