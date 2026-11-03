package scope

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

func TestE2E_UnclaimedPathWithoutHistory(t *testing.T) {
	root := t.TempDir()
	assert.NoError(t, os.CopyFS(root, os.DirFS(harness.TestdataPath(t, "unclaimed-change"))))
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(t, repo, "fixture", &gogit.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.invalid", When: time.Unix(0, 0)}})
	testutil.WriteFile(t, root, "unclaimed/notes.txt", "after\n")
	first, _ := harness.RequireCLIOK(t, "diff", "all", "--base", "HEAD", "-p", filepath.Join(root, "flux"), "-o", "diff", "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
	second, _ := harness.RequireCLIOK(t, "diff", "all", "--base", "HEAD", "-p", filepath.Join(root, "flux"), "-o", "diff", "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
	assert.Equal(t, first, "")
	assert.Equal(t, second, first)
}
