package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestBootstrap_LocalSourceUsesSelectedCache(t *testing.T) {
	for _, kind := range []string{"configured-directory", "shared-cache"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			repo, err := git.PlainInit(root, false)
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
			wt, err := repo.Worktree()
			if err != nil {
				t.Fatal(err)
			}
			for i, value := range []string{"A", "B"} {
				testutil.WriteFile(t, root, "value.txt", value)
				if _, err := wt.Add("."); err != nil {
					t.Fatal(err)
				}
				hash, err := wt.Commit(value, &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), hash)); err != nil {
						t.Fatal(err)
					}
				}
			}
			cacheRoot := t.TempDir()
			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root,
				SelfURLs: []string{"git://fixture.invalid/cluster"}, CacheDir: cacheRoot, Concurrency: 2}
			if kind == "shared-cache" {
				cacheRoot = t.TempDir()
				cfg.SourceCache = source.NewCache(cacheroot.New(cacheRoot))
			}
			o, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			id := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
			artifact := o.Store().GetArtifact(id).(*store.SourceArtifact)
			if !strings.HasPrefix(artifact.LocalPath, cacheRoot+string(filepath.Separator)) {
				t.Fatalf("selected cache %s did not reach discovery: %+v", cacheRoot, artifact)
			}
			if content, err := os.ReadFile(filepath.Join(artifact.LocalPath, "value.txt")); err != nil || string(content) != "A" {
				t.Fatalf("committed artifact = %q, %v", content, err)
			}
		})
	}
}
