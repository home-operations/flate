package scope_test

import (
	"path/filepath"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
)

type gitFixture struct {
	path string
	root string
}

func initGitFixture(t *testing.T) gitFixture {
	t.Helper()
	dir := t.TempDir()
	repo := testutil.GitInit(t, dir)
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("kubernetes/flux/cluster.yaml", "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/x.git"}
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`))
	writeFile("kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("kubernetes/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	return gitFixture{path: filepath.Join(dir, "kubernetes", "flux"), root: dir}
}
