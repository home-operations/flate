package cli

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
			assert.NoError(t, flags.loadInputs(t.Context(), nil))
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
