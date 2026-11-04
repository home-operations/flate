package helm_test

import (
	"os"
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

func e2EHelmSourceRootValuesFilesChangedOnlyCase(t *testing.T, operation *string, chartRef *bool) {
	t.Helper()

	dir := sourceValuesFixture(t, "[./config/prod.yaml]", (*operation) != "strict delete", (*chartRef))
	valuesPath := filepath.Join(dir, "config", "prod.yaml")
	if (*operation) == "add" {
		assert.NoError(t, os.Remove(valuesPath))
	}
	repo := testutil.GitInit(t, dir)
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	want := "production"
	switch *operation {
	case "edit":
		testutil.WriteFile(t, dir, "config/prod.yaml", "greeting: edited\n")
		want = "edited"
	case "add":
		testutil.WriteFile(t, dir, "config/prod.yaml", "greeting: production\n")
	case "delete", "strict delete":
		assert.NoError(t, os.Remove(valuesPath))
		want = "default"
	}
	args := []string{
		"build", "all", "--path", filepath.Join(dir, "cluster"), "--base", "HEAD",
		"--concurrency", "2", "--cache-dir", t.TempDir(),
	}
	if (*operation) == "strict delete" {
		runCLIExpectErrResult1 := harness.RunCLIExpectErr(t, args...)
		out, code := runCLIExpectErrResult1.Output, runCLIExpectErrResult1.Code
		assert.Equal(t, code, 1)
		assert.FatalIf(
			t,
			!strings.Contains(out, "config/prod.yaml"),
			"missing selected-file failure:\n%s",
			out,
		)
		return
	}
	out := harness.RunCLIStdout(t, args...)
	assert.FatalIf(
		t,
		!strings.Contains(out, "name: demo-values") || !strings.Contains(out, "greeting: "+want),
		"selected release did not render %q:\n%s",
		want,
		out,
	)
	assert.FatalIf(t, strings.Contains(out, "name: sibling-values"), "unchanged sibling rendered:\n%s", out)
	assert.Equal(t, harness.RunCLIStdout(t, args...), out)
	args[0] = "test"
	status := harness.RunCLI(t, args...)
	for _, kind := range []string{"Kustomization", "HelmRelease"} {
		found := false
		for line := range strings.SplitSeq(status, "\n") {
			if strings.Contains(line, kind) && strings.Contains(line, "apps/demo") &&
				strings.Contains(line, "✓") {
				found = true
			}
		}
		assert.ErrorIf(t, !found, "selected %s did not pass:\n%s", kind, status)
	}
	harness.MustExtractSkipLine(t, status, "apps/sibling")
}
