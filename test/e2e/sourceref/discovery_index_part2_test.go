package sourceref_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2ESourceRefMissingNonGitKind(t *testing.T, kind *string) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	consumer := func(interval string) string {
		if (*kind) == "Bucket" {
			return testutil.KustomizationYAML(
				"consumer",
				"apps",
				"  interval: "+interval+"\n  path: ./decoy\n  sourceRef: {kind: Bucket, name: missing, namespace: sources}\n",
			)
		}
		chart := "  chartRef: {kind: OCIRepository, name: missing, namespace: sources}\n"
		if (*kind) == "HelmRepository" {
			chart = "  chart:\n    spec:\n      chart: ./decoy\n      sourceRef: {kind: HelmRepository, name: missing, namespace: sources}\n"
		}
		return "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer, namespace: apps}\nspec:\n  interval: " + interval + "\n" + chart
	}
	testutil.WriteFile(t, root, "entry/consumer.yaml", consumer("1m"))
	testutil.WriteFile(t, root, "Chart.yaml", "apiVersion: v2\nname: decoy\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, ".editorconfig", "root = true\n")
	testutil.WriteFile(
		t,
		root,
		"templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-payload}\n",
	)
	testutil.WriteFile(t, root, "decoy/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"decoy/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-payload}\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"initial",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	orig := t.TempDir()
	testutil.CopyTree(t, root, orig)
	testutil.WriteFile(t, root, "entry/consumer.yaml", consumer("2m"))
	for _, sides := range [][2]string{{root, orig}, {orig, root}} {
		e2ESourceRefMissingNonGitKindLoop(t, struct {
			sides *[2]string
			kind  **string
		}{sides: &sides, kind: &kind})
	}
}

func e2ESourceRefMissingNonGitKindLoop(t *testing.T, loopInputs struct {
	sides *[2]string
	kind  **string
},
) {
	t.Helper()

	for _, mode := range []string{"full", "changed", "diff"} {
		var previous [2]string
		for run := range 2 {
			args := []string{
				"build",
				"all",
				"--path",
				filepath.Join(loopInputs.sides[0], "entry"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--no-progress",
			}
			if mode != "full" {
				args = append(args, "--path-orig", filepath.Join(loopInputs.sides[1], "entry"))
			}
			if mode == "diff" {
				args[0] = "diff"
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			assert.FatalIf(t, code != 1 || !strings.Contains(stderr, (*(*loopInputs.kind))) ||
				!strings.Contains(
					stderr,
					"sources/missing",
				), "%s %s exit=%d stdout=%s stderr=%s", (*(*loopInputs.kind)), mode, code, stdout, stderr)
			assert.FatalIf(t, strings.Contains(stdout, "consumer-payload") ||
				strings.Contains(
					stderr,
					"chart illegally contains content",
				), "missing source used decoy bytes: stdout=%s stderr=%s", stdout, stderr)
			current := [2]string{stdout, stderr}
			assert.FatalIf(
				t,
				run == 1 && current != previous,
				"%s %s output changed between two cold runs",
				(*(*loopInputs.kind)),
				mode,
			)
			previous = current
		}
	}
}
