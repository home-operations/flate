package scope_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_UnclaimedPathWithoutHistory(t *testing.T) {
	root := t.TempDir()
	assert.NoError(t, os.CopyFS(root, os.DirFS(harness.TestdataPath(t, "unclaimed-change"))))
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&gogit.CommitOptions{
			Author: &object.Signature{Name: "test", Email: "test@example.invalid", When: time.Unix(0, 0)},
		},
	)
	testutil.WriteFile(t, root, "unclaimed/notes.txt", "after\n")
	requireCLIOKResult1 := harness.RequireCLIOK(
		t,
		"diff",
		"all",
		"--base",
		"HEAD",
		"-p",
		filepath.Join(root, "flux"),
		"-o",
		"diff",
		"--no-progress",
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	)
	first, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
	requireCLIOKResult2 := harness.RequireCLIOK(
		t,
		"diff",
		"all",
		"--base",
		"HEAD",
		"-p",
		filepath.Join(root, "flux"),
		"-o",
		"diff",
		"--no-progress",
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	)
	second, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
	assert.Equal(t, first, "")
	assert.Equal(t, second, first)
}
