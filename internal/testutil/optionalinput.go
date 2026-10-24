package testutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// CrossSourceOptionalInputs creates identical bootstrap trees with distinct local selected-source histories.
func CrossSourceOptionalInputs(tb testing.TB) [2]string {
	tb.Helper()
	var roots [2]string
	for side := range roots {
		root := tb.TempDir()
		roots[side] = root
		repo := GitInit(tb, root)
		if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/cluster.git"}}); err != nil {
			tb.Fatal(err)
		}
		WriteFile(tb, root, "inputs/kustomization.yaml", "resources: []\n")
		if side == 0 {
			WriteFile(tb, root, "inputs/kustomization.yaml", "resources: [optional.yaml]\n")
			WriteFile(tb, root, "inputs/optional.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: available}\n")
		}
		options := &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}}
		selected := GitCommitAll(tb, repo, "selected supplier", options)
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("selected"), selected)); err != nil {
			tb.Fatal(err)
		}
		if err := os.RemoveAll(filepath.Join(root, "inputs")); err != nil {
			tb.Fatal(err)
		}
		WriteFile(tb, root, "flux/sources.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://example.test/cluster.git}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: supplier, namespace: flux-system}\nspec: {url: https://example.test/cluster.git, ref: {branch: selected}}\n")
		WriteFile(tb, root, "flux/inputs.yaml", KustomizationYAML("inputs", "flux-system", "  path: ./inputs\n  sourceRef: {kind: GitRepository, name: supplier}\n"))
		WriteFile(tb, root, "flux/app.yaml", KustomizationYAML("app", "flux-system", "  path: ./apps/app\n  sourceRef: {kind: GitRepository, name: cluster}\n  dependsOn: [{name: inputs}]\n  postBuild:\n    substitute: {SENTINEL: active}\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n"))
		WriteFile(tb, root, "apps/app/kustomization.yaml", "resources: [output.yaml]\n")
		WriteFile(tb, root, "apps/app/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: flux-system}\ndata: {value: '${VALUE:-fallback}', sentinel: '${SENTINEL}'}\n")
		GitCommitAll(tb, repo, "bootstrap", options)
	}
	return roots
}
