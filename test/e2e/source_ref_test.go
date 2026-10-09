package e2e

import (
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestE2E_SourceRef_NonHEADTag(t *testing.T) {
	root := t.TempDir()
	repo := gitInit(t, root)
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
	}); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "flux/entry.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	for _, version := range []string{"v1.0.0", "v2.0.0"} {
		testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: `+version+"\n")
		gitCommitAll(t, repo)
		head, err := repo.Head()
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Storer.SetReference(plumbing.NewHashReference(
			plumbing.NewTagReferenceName(version), head.Hash())); err != nil {
			t.Fatal(err)
		}
	}
	out, stderr := requireCLIOK(t, "build", "all", "--path", root+"/flux",
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: v2.0.0") {
		t.Fatalf("expected pinned v1.0.0 content:\n%s\nstderr:\n%s", out, stderr)
	}
}
