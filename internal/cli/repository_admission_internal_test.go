package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_FormatZeroRepositoryRoot(t *testing.T) {
	preservePersistenceNoticeState(t)
	for _, tc := range []struct {
		name, extension string
	}{
		{"worktreeConfig", "worktreeConfig = true"},
		{"partialClone", "partialClone = origin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "flux/app.yaml", testutil.KustomizationYAML("app", "flux-system", `  path: ./app
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`))
			testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"app/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extension, namespace: apps}\ndata: {value: before}\n",
			)
			base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{
				Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
			})
			testutil.WriteFile(
				t,
				root,
				".git/config",
				"[core]\nrepositoryformatversion = 0\nbare = false\n[extensions]\n"+tc.extension+"\n[commit]\ngpgsign = false\n",
			)
			testutil.WriteFile(
				t,
				root,
				"app/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extension, namespace: apps}\ndata: {value: after}\n",
			)
			const want = `---
apiVersion: v1
data:
  value: after
kind: ConfigMap
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: app
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: extension
  namespace: apps
`
			for _, mode := range []string{"ordinary", "changed-only"} {
				t.Run(mode, func(t *testing.T) {
					args := []string{
						"build", "ks", "--path", filepath.Join(root, "flux"),
						"--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress",
					}
					if mode == "changed-only" {
						args = append(args, "--base", base.String())
					}
					for range 2 {
						result := runCLI(t, args...)
						if tc.name == "partialClone" {
							assert.Equal(t, result.code, 1)
							assert.Equal(t, result.stdout, "")
							cause := fmt.Sprintf("open repository %q: %s: partialclone", root, git.ErrUnknownExtension)
							assert.FatalIf(
								t,
								!strings.Contains(result.stderr, cause),
								"selected-open cause missing: %s",
								result.stderr,
							)
							continue
						}
						assert.FatalIf(t, result.code != 0, "exit=%d stderr=%s", result.code, result.stderr)
						assert.Diff(t, result.stdout, want)
					}
				})
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SelectedAncestorOpenFailure(t *testing.T) {
	preservePersistenceNoticeState(t)
	for _, mode := range []string{"ordinary", "baseline", "before directory", "current directory"} {
		t.Run(mode, func(t *testing.T) {
			broken := t.TempDir()
			testutil.WriteFile(t, broken, ".git", "gitdir: "+filepath.Join(broken, "missing-metadata")+"\n")
			scan := filepath.Join(broken, "flux")
			assert.NoError(t, os.Mkdir(scan, 0o750))
			args := []string{"build", "all", "--path", scan, "--cache-dir", t.TempDir(), "--concurrency", "2"}
			switch mode {
			case "baseline":
				args = append(args, "--base", "HEAD")
			case "before directory":
				args[3] = writeFixture(t)
				args = append(args, "--path-orig", scan)
			case "current directory":
				args = append(args, "--path-orig", writeFixture(t))
			}
			result := runCLI(t, args...)
			assert.Equal(t, result.code, 1)
			assert.Equal(t, result.stdout, "")
			cause := fmt.Sprintf("open repository %q: %s", broken, git.ErrRepositoryNotExists)
			assert.FatalIf(t, !strings.Contains(result.stderr, cause), "selected-open cause missing: %s", result.stderr)
		})
	}
}
