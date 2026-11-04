package scope_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2EChangedOnlyLocalChartInputsName(t *testing.T, invalid, committed *bool, elapsed **regexp.Regexp) {
	t.Helper()

	root, orig := t.TempDir(), t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux/kustomization.yaml",
		"resources: [hr.yaml, control.yaml, unrelated.yaml]\n",
	)
	release := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
	testutil.WriteFile(t, root, "flux/hr.yaml", release)
	testutil.WriteFile(
		t,
		root,
		"flux/control.yaml",
		strings.ReplaceAll(
			strings.ReplaceAll(release, "demo", "control"),
			"namespace: apps",
			"namespace: other",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated.yaml",
		testutil.KustomizationYAML("unrelated", "other", "  path: ./unrelated\n"),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unrelated/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n",
	)
	template := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: surviving, namespace: apps}\ndata: {value: unchanged}\n"
	for _, chart := range []string{"demo", "control"} {
		testutil.WriteFile(
			t,
			root,
			"charts/"+chart+"/Chart.yaml",
			"apiVersion: v2\nname: "+chart+"\nversion: 1.0.0\n",
		)
		testutil.WriteFile(
			t,
			root,
			"charts/"+chart+"/templates/cm.yaml",
			strings.ReplaceAll(template, "surviving", chart+"-surviving"),
		)
	}
	if *invalid {
		testutil.WriteFile(
			t,
			root,
			"charts/demo/templates/cm.yaml",
			"{{ fail \"surviving template is invalid\" }}\n",
		)
	}
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/extra.yaml",
		strings.ReplaceAll(template, "surviving", "deleted"),
	)
	options := &git.CommitOptions{
		Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
	}
	base := testutil.GitCommitAll(t, repo, "base", options)
	testutil.CopyTree(t, root, orig)
	assert.NoError(t, os.Remove(filepath.Join(root, "charts", "demo", "templates", "extra.yaml")))
	baseline := []string{"--path-orig", filepath.Join(orig, "flux")}
	if *committed {
		testutil.GitCommitAll(t, repo, "delete template", options)
		baseline = []string{"--base", base.String()}
	}
	commands := []string{"build", "test", "get"}
	if *invalid {
		commands = []string{"test"}
	}
	for _, command := range commands {
		t.Run(command, func(t *testing.T) {
			e2EChangedOnlyLocalChartInputsNameCommand(t, &command, &root, struct {
				baseline *[]string
				invalid  **bool
				elapsed  ***regexp.Regexp
			}{baseline: &baseline, invalid: &invalid, elapsed: &elapsed})
		})
	}
}

func e2EChangedOnlyLocalChartInputsNameCommand(t *testing.T, command *string, root *string, capturedInputs struct {
	baseline *[]string
	invalid  **bool
	elapsed  ***regexp.Regexp
},
) {
	t.Helper()

	args := make([]string, 0, 11+len(*capturedInputs.baseline))
	args = append(args, (*command), "all")
	if (*command) == "get" {
		args = append(args[:0], (*command), "hr", "-o", "name")
	}
	args = append(
		args,
		"--path",
		filepath.Join((*root), "flux"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	)
	args = append(args, (*capturedInputs.baseline)...)
	runCLIBuffersResult1 := harness.RunCLIBuffers(args...)
	out, stderr, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
	wantCode, want := 0, "apps/demo"
	if (*command) == "get" {
		want = "demo\n"
	}
	assert.ErrorIf(
		t,
		(*command) == "test" && !(*(*capturedInputs.invalid)) && !strings.Contains(out, "1 passed"),
		"selected release was not checked: %s",
		out,
	)
	if (*command) == "build" {
		want = "name: demo-surviving"
	}
	if *(*capturedInputs.invalid) {
		wantCode = 1
		assert.ErrorIf(
			t,
			!strings.Contains(out+stderr, "surviving template is invalid"),
			"selected release failure was skipped: stdout=%q stderr=%s",
			out,
			stderr,
		)
	}
	assert.ErrorIf(
		t,
		code != wantCode || !strings.Contains(out, want),
		"deleted template lost its release: code=%d stdout=%q stderr=%s",
		code,
		out,
		stderr,
	)
	for _, excluded := range []string{"deleted", "unrelated", "control"} {
		assert.ErrorIf(
			t,
			strings.Contains(out, excluded),
			"selected unrelated or deleted output %q: %s",
			excluded,
			out,
		)
	}
	runCLIBuffersResult2 := harness.RunCLIBuffers(args...)
	second, _, secondCode := runCLIBuffersResult2.Stdout, runCLIBuffersResult2.Stderr, runCLIBuffersResult2.Code
	assert.ErrorIf(t, secondCode != code ||
		(*(*capturedInputs.elapsed)).ReplaceAllString(
			second,
			"",
		) != (*(*capturedInputs.elapsed)).ReplaceAllString(
			out,
			"",
		), "changed-only output differs between runs: %d/%q versus %d/%q", code, out, secondCode, second)
}
