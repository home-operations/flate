package scope_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_Diff_IgnoredRequiredSubstitution(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		testutil.WriteFile(
			t,
			root,
			"flux/consumer.yaml",
			"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n interval: 10m\n path: ./app\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n postBuild:\n  substituteFrom: [{kind: ConfigMap, name: vars}]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/vars.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata:\n FLAG: old\n",
		)
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumed, namespace: apps}\ndata:\n flag: ${FLAG}\n",
		)
		ignore := ""
		if side == 1 {
			ignore = "vars.yaml\n"
		}
		testutil.WriteFile(t, root, "flux/.krmignore", ignore)
		testutil.GitInit(t, root)
	}
	for side, root := range roots {
		stdout, stderr, code := harness.RunCLIBuffers(
			"build",
			"all",
			"--path",
			filepath.Join(root, "flux"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
			"--no-progress",
		)
		assert.Equal(t, code, side)
		switch {
		case side == 0:

			assert.FatalIf(
				t,
				!strings.Contains(stdout, "flag: old"),
				"full oracle: exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)

		case !strings.Contains(stderr, "vars") || !strings.Contains(stderr, "consumer"):

			t.Fatalf("full oracle: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
	}
	for _, named := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("named=%t/reverse=%t", named, reverse),
				func(t *testing.T) { e2EDiffIgnoredRequiredSubstitutionCase(t, &roots, &reverse, &named) },
			)
		}
	}
}

func e2EDiffIgnoredRequiredSubstitutionCase(t *testing.T, roots *[2]string, reverse *bool, named *bool) {
	t.Helper()

	current, original := roots[1], roots[0]
	if *reverse {
		current, original = original, current
	}
	args := []string{
		"diff",
		"all",
		"--path",
		filepath.Join(current, "flux"),
		"--path-orig",
		filepath.Join(original, "flux"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"--no-progress",
		"-o",
		"diff",
	}
	if *named {
		args[1] = "ks"
		args = append(args, "consumer")
	}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "vars") || !strings.Contains(stderr, "consumer"),
		"required-data failure lost: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
	failingSide := "current snapshot"
	if *reverse {
		failingSide = "orig snapshot"
	}
	assert.Equal(t, strings.Contains(stderr, failingSide), true)
	t.Logf("exit=%d diagnostics=%s", code, stderr)
	repeatedOutput, repeatedDiagnostic, repeatedExit := harness.RunCLIBuffers(args...)
	assert.Equal(t, repeatedOutput, stdout)
	assert.Equal(t, repeatedDiagnostic, stderr)
	assert.Equal(t, repeatedExit, code)
}
