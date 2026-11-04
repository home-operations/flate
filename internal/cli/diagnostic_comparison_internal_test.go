package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiagnosticOnlyComparison(t *testing.T) {
	root := writeFixture(t)
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "work.txt", "before")
	pin := testutil.GitCommitAll(
		t,
		repo,
		"base",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
	)
	commit, err := repo.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("work.txt")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "work.txt", "after")
	testutil.GitCommitAll(t, repo, "head", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	oid := entry.Hash.String()
	assert.NoError(t, os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])))
	testutil.WriteFile(t, root, ".git/index", "invalid index")
	for _, command := range [][]string{{"diff", "all"}, {"diff", "images"}, {"test", "ks", "named"}, {"build", "all"}, {"get", "all"}} {
		t.Run(strings.Join(command, "/"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append(slices.Clone(command), "--path", root, "--base", pin.String(), "--concurrency", "2")
			code := Run(args, &stdout, &stderr)
			assert.FatalIf(
				t,
				code != 1 || stdout.Len() != 0 || strings.Count(stderr.String(), "Git status unavailable") != 1 ||
					strings.Count(stderr.String(), "missing objects") != 1 ||
					strings.Contains(
						stderr.String(),
						`no Kustomization named`,
					),
				"diagnostic-only %v = exit %d, stdout %q, stderr %q",
				command,
				code,
				stdout.String(),
				stderr.String(),
			)
		})
	}
	for _, tc := range []struct {
		name       string
		test, pair bool
	}{{name: "data"}, {name: "test", test: true}, {name: "diff", pair: true}} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			rootID := testutil.KustomizationID("", "")
			if tc.test {
				rootID.Name = "named"
			}
			err := runSelected(t.Context(), selectedRequest{
				flags: commonFlags{path: root, base: pin.String(), concurrency: 2},
				roots: []manifest.NamedResource{rootID},
				pair:  tc.pair, stdout: &stdout, stderr: &stderr, test: tc.test,
			}, func(*selectedInvocation) selectedPayload { called = true; return selectedPayload{} })
			assert.FatalIf(
				t,
				err == nil || called || !strings.Contains(stderr.String(), "Git status unavailable"),
				"incomplete input invoked product=%v: %v, %s",
				called,
				err,
				stderr.String(),
			)
		})
	}
}
