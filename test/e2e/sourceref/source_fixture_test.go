package sourceref_test

import (
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

type sourceFixture struct {
	root       string
	repository *gogit.Repository
	before     plumbing.Hash
	after      plumbing.Hash
}

func sourceRefFixture(t *testing.T) sourceFixture {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(&config.RemoteConfig{
			Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
		})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	testutil.WriteFile(t, root, "flux/entry.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
  ignore: '/ignored.txt'
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`))
	testutil.WriteFile(t, root, "ignored.txt", "ignored by the source artifact\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	var hashes [2]plumbing.Hash
	for i, version := range []string{"v1.0.0", "v2.0.0"} {
		testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: `+version+"\n")
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
		)
		head, err := repo.Head()
		assert.NoError(t, err)
		hashes[i] = head.Hash()
		assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(
			plumbing.NewTagReferenceName(version), head.Hash())))
	}
	return sourceFixture{root: root, repository: repo, before: hashes[0], after: hashes[1]}
}
