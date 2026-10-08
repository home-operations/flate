package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestE2E_Diff_InTreeCache(t *testing.T) {
	for _, source := range []string{"implicit", "self"} {
		t.Run(source, func(t *testing.T) {
			for _, state := range []string{"unchanged", "changed"} {
				t.Run(state, func(t *testing.T) {
					repoRoot := t.TempDir()
					repo := gitInit(t, repoRoot)
					sourceName := "flux-system"
					if source == "self" {
						sourceName = "cluster"
						if _, err := repo.CreateRemote(&config.RemoteConfig{
							Name: "origin", URLs: []string{"https://github.com/example/cluster.git"},
						}); err != nil {
							t.Fatal(err)
						}
					}
					cluster := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: ` + sourceName + `, namespace: flux-system}
`
					if source == "self" {
						cluster = `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: https://github.com/example/cluster.git
---
` + cluster
					}
					testutil.WriteFile(t, repoRoot, "flux/cluster.yaml", cluster)
					testutil.WriteFile(t, repoRoot, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
					testutil.WriteFile(t, repoRoot, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  greeting: hola
`)
					gitCommitAll(t, repo)
					head, err := repo.Head()
					if err != nil {
						t.Fatal(err)
					}
					if err := repo.Storer.SetReference(plumbing.NewHashReference(
						plumbing.NewBranchReferenceName("cache-base"), head.Hash())); err != nil {
						t.Fatal(err)
					}
					if state == "changed" {
						mutateFile(t, filepath.Join(repoRoot, "apps", "cm.yaml"), "greeting: hola", "greeting: hi")
					}
					t.Chdir(repoRoot)
					layouts := []struct {
						name string
						path string
					}{
						{name: "external", path: filepath.Join(t.TempDir(), "cache")},
						{name: "relative", path: "./.cache"},
						{name: "absolute", path: filepath.Join(repoRoot, ".absolute-cache")},
					}
					var control string
					for _, layout := range layouts {
						t.Run(layout.name, func(t *testing.T) {
							if _, err := os.Stat(layout.path); !os.IsNotExist(err) {
								t.Fatalf("cache must start absent: %v", err)
							}
							for _, warmth := range []string{"fresh", "warm", "repeat"} {
								t.Run(warmth, func(t *testing.T) {
									args := []string{"diff", "all", "--base", "cache-base", "--concurrency", "2",
										"-o", "diff", "--cache-dir", layout.path}
									if source == "self" {
										args = append(args, "--path", "flux")
									}
									out, stderr := requireCLIOK(t, args...)
									if state == "unchanged" && out != "" {
										t.Errorf("unchanged stdout must be empty:\n%s\nstderr:\n%s", out, stderr)
									}
									if state == "changed" && (!strings.Contains(out, "-  greeting: hola") ||
										!strings.Contains(out, "+  greeting: hi")) {
										t.Errorf("expected greeting removal and addition:\n%s\nstderr:\n%s", out, stderr)
									}
									if layout.name == "external" && warmth == "fresh" {
										control = out
									}
									if out != control {
										t.Errorf("stdout differs from external cache:\nwant:\n%s\ngot:\n%s", control, out)
									}
									if _, err := os.Stat(layout.path); err != nil {
										t.Fatalf("cache was not populated: %v", err)
									}
								})
							}
						})
					}
				})
			}
		})
	}
}
