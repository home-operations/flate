package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffSelfSourceBranch(t *testing.T) {
	for _, checkout := range []string{"dirty", "branch", "detached", "nested"} {
		t.Run(checkout, func(t *testing.T) {
			root := writeFixture(t)
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [cluster.yaml, source.yaml]\n")
			testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: home-kubernetes, namespace: flux-system}
spec:
  url: https://example.invalid/home-ops.git
  ref: {branch: main}
  ignore: |
    /ignored
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: supplier, namespace: flux-system}
spec:
  url: https://example.invalid/home-ops.git
  ref: {name: refs/remotes/origin/main}
`)
			testutil.WriteFile(t, root, "flux/cluster.yaml", testutil.KustomizationYAML(
				"apps",
				"flux-system",
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: home-kubernetes}\n",
			)+"---\n"+testutil.KustomizationYAML(
				"fixed-apps",
				"flux-system",
				"  path: ./fixed-apps\n  sourceRef: {kind: GitRepository, name: supplier}\n",
			))
			testutil.WriteFile(t, root, "fixed-apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"fixed-apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixed, namespace: apps}\ndata: {greeting: hi}\n",
			)

			scan := filepath.Join(root, "flux")
			if checkout == "nested" {
				scan = filepath.Join(scan, "config")
				assert.NoError(t, os.MkdirAll(scan, 0o755))
				assert.NoError(
					t,
					os.Rename(filepath.Join(root, "flux", "source.yaml"), filepath.Join(scan, "source.yaml")),
				)
				assert.NoError(
					t,
					os.Rename(filepath.Join(root, "flux", "cluster.yaml"), filepath.Join(root, "flux", "apps.yaml")),
				)
				testutil.WriteFile(
					t,
					scan,
					"cluster.yaml",
					testutil.KustomizationYAML(
						"cluster",
						"flux-system",
						"  path: ./flux\n  sourceRef: {kind: GitRepository, name: home-kubernetes}\n",
					),
				)
				testutil.WriteFile(t, scan, "kustomization.yaml", "resources: [cluster.yaml, source.yaml]\n")
				testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [apps.yaml, config]\n")
			}

			repo := testutil.GitInit(t, root)
			_, err := repo.CreateRemote(
				&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.invalid/home-ops.git"}},
			)
			assert.NoError(t, err)
			author := &object.Signature{Name: "t", Email: "t@e"}
			base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: author})
			assert.NoError(
				t,
				repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), base)),
			)
			assert.NoError(
				t,
				repo.Storer.SetReference(
					plumbing.NewHashReference(plumbing.NewRemoteReferenceName("origin", "main"), base),
				),
			)

			wt, err := repo.Worktree()
			assert.NoError(t, err)
			assert.NoError(
				t,
				wt.Checkout(
					&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName("pull-request"), Create: true},
				),
			)
			testutil.WriteFile(t, root, "branch.txt", "pull request\n")
			testutil.GitCommitAll(t, repo, "branch", &git.CommitOptions{Author: author})

			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: hola}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"fixed-apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixed, namespace: apps}\ndata: {greeting: hola}\n",
			)

			if checkout != "dirty" {
				head := testutil.GitCommitAll(t, repo, "change", &git.CommitOptions{Author: author})
				if checkout == "detached" {
					assert.NoError(t, wt.Checkout(&git.CheckoutOptions{Hash: head}))
					assert.NoError(t, repo.Storer.RemoveReference(plumbing.NewBranchReferenceName("main")))
				}
			}

			result := runCLI(t, "diff", "all", "--path", scan, "--base", base.String(),
				"--concurrency", "2", "-o", "json")
			assert.FatalIf(t, result.code != 0, "diff exited %d: %s", result.code, result.stderr)

			var want bytes.Buffer
			assert.NoError(t, json.Indent(&want, []byte(`{"changes":[{
"parent":{"group":"kustomize.toolkit.fluxcd.io","kind":"Kustomization","namespace":"flux-system","name":"apps","path":"apps"},
"apiVersion":"v1","kind":"ConfigMap","namespace":"apps","name":"hello","status":"changed",
"old":{"apiVersion":"v1","data":{"greeting":"hi"},"kind":"ConfigMap","metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"apps","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":"hello","namespace":"apps"}},
"new":{"apiVersion":"v1","data":{"greeting":"hola"},"kind":"ConfigMap","metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"apps","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":"hello","namespace":"apps"}},
"oldChart":"","newChart":""}],"suppressed":[]}`), "", "  "))
			want.WriteByte('\n')
			assert.Diff(t, result.stdout, want.String())
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffSelfSourceGeneratedMembership(t *testing.T) {
	for _, status := range []string{"added", "removed"} {
		t.Run(status, func(t *testing.T) {
			root := writeFixture(t)
			assert.NoError(t, os.Remove(filepath.Join(root, "apps", "kustomization.yaml")))
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [cluster.yaml, source.yaml]\n")
			testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.invalid/home-ops.git
  ref: {branch: main}
`)

			extra := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra, namespace: apps}\ndata: {value: retained}\n"
			if status == "removed" {
				testutil.WriteFile(t, root, "apps/extra.yaml", extra)
			}

			repo := testutil.GitInit(t, root)
			_, err := repo.CreateRemote(
				&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.invalid/home-ops.git"}},
			)
			assert.NoError(t, err)
			author := &object.Signature{Name: "t", Email: "t@e"}
			base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: author})
			assert.NoError(
				t,
				repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), base)),
			)

			if status == "added" {
				testutil.WriteFile(t, root, "apps/extra.yaml", extra)
			} else {
				assert.NoError(t, os.Remove(filepath.Join(root, "apps", "extra.yaml")))
			}

			result := runCLI(
				t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", base.String(), "-o", "json",
			)
			assert.FatalIf(t, result.code != 0, "diff exited %d: %s", result.code, result.stderr)

			present := `{"apiVersion":"v1","data":{"value":"retained"},"kind":"ConfigMap","metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"apps","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":"extra","namespace":"apps"}}`
			oldDocument, newDocument := "null", present
			if status == "removed" {
				oldDocument, newDocument = present, "null"
			}

			expected := fmt.Sprintf(`{"changes":[{
"parent":{"group":"kustomize.toolkit.fluxcd.io","kind":"Kustomization","namespace":"flux-system","name":"apps","path":"apps"},
"apiVersion":"v1","kind":"ConfigMap","namespace":"apps","name":"extra","status":%q,
"old":%s,"new":%s,"oldChart":"","newChart":""}],"suppressed":[]}`, status, oldDocument, newDocument)

			var want bytes.Buffer
			assert.NoError(t, json.Indent(&want, []byte(expected), "", "  "))
			want.WriteByte('\n')
			assert.Diff(t, result.stdout, want.String())
		})
	}
}
