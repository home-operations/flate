package scope

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_Diff_InTreeCache(t *testing.T) {
	for _, source := range []string{"implicit", "self"} {
		t.Run(source, func(t *testing.T) {
			for _, state := range []string{"unchanged", "changed", "suspended_current", "suspended_orig"} {
				t.Run(state, func(t *testing.T) {
					repoRoot := t.TempDir()
					repo := testutil.GitInit(t, repoRoot)
					sourceName := "flux-system"
					if source == "self" {
						sourceName = "cluster"
						if _, err := repo.CreateRemote(&config.RemoteConfig{
							Name: "origin", URLs: []string{"https://github.com/example/cluster.git"},
						}); err != nil {
							t.Fatal(err)
						}
					}
					cluster := testutil.KustomizationYAML("apps", "flux-system", "  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: "+sourceName+", namespace: flux-system}\n")
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
					if state == "suspended_orig" {
						cluster = strings.Replace(cluster, "  path: ./apps", "  suspend: true\n  path: ./apps", 1)
					}
					testutil.WriteFile(t, repoRoot, "flux/cluster.yaml", cluster)
					testutil.WriteFile(t, repoRoot, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
					testutil.WriteFile(t, repoRoot, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  greeting: hola
`)
					testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
					head, err := repo.Head()
					if err != nil {
						t.Fatal(err)
					}
					if err := repo.Storer.SetReference(plumbing.NewHashReference(
						plumbing.NewBranchReferenceName("cache-base"), head.Hash())); err != nil {
						t.Fatal(err)
					}
					if state == "changed" {
						harness.MutateFile(t, filepath.Join(repoRoot, "apps", "cm.yaml"), "greeting: hola", "greeting: hi")
					}
					switch state {
					case "suspended_current":
						harness.MutateFile(t, filepath.Join(repoRoot, "flux", "cluster.yaml"), "  path: ./apps", "  suspend: true\n  path: ./apps")
					case "suspended_orig":
						harness.MutateFile(t, filepath.Join(repoRoot, "flux", "cluster.yaml"), "  suspend: true\n", "")
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
					warmths := []string{"fresh", "warm", "repeat"}
					if source != "self" || state != "changed" {
						layouts = layouts[:1]
						warmths = warmths[:1]
					}
					var control string
					for _, layout := range layouts {
						t.Run(layout.name, func(t *testing.T) {
							if _, err := os.Stat(layout.path); !os.IsNotExist(err) {
								t.Fatalf("cache must start absent: %v", err)
							}
							for _, warmth := range warmths {
								t.Run(warmth, func(t *testing.T) {
									args := []string{"diff", "all", "--base", "cache-base", "--concurrency", "2",
										"-o", "diff", "--cache-dir", layout.path}
									if source == "self" {
										args = append(args, "--path", "flux")
									}
									out, stderr := harness.RequireCLIOK(t, args...)
									if state == "unchanged" && out != "" {
										t.Errorf("unchanged stdout must be empty:\n%s\nstderr:\n%s", out, stderr)
									}
									if state == "changed" && (!strings.Contains(out, "-  greeting: hola") ||
										!strings.Contains(out, "+  greeting: hi")) {
										t.Errorf("expected greeting removal and addition:\n%s\nstderr:\n%s", out, stderr)
									}
									if side, suspended := strings.CutPrefix(state, "suspended_"); suspended {
										if !strings.Contains(stderr, "suspended on "+side) || !strings.Contains(stderr, "diff suppressed") || out != "" {
											t.Fatalf("suspension must disclose withheld output without pruning: stdout=%q stderr=%q", out, stderr)
										}
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
