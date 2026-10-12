package orchestrator

import (
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_DefaultGitCache(t *testing.T) {
	for _, mode := range []string{"memory", "disk", "injected"} {
		t.Run(mode, func(t *testing.T) {
			remote := t.TempDir()
			repo, err := git.PlainInit(remote, false)
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, remote, "app/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, remote, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\ndata: {value: committed}\n")
			wt, err := repo.Worktree()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Add("."); err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Commit("source", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}}); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: flux-system}\nspec:\n  url: file://"+filepath.ToSlash(remote)+"\n  interval: 10m\n")
			for _, name := range []string{"first", "second"} {
				testutil.WriteFile(t, root, name+".yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: "+name+", namespace: flux-system}\nspec: {path: ./app, sourceRef: {kind: GitRepository, name: remote}}\n")
			}
			cfg := Config{Path: root, Concurrency: 2}
			if mode == "disk" {
				cfg.CacheDir = t.TempDir()
			}
			if mode == "injected" {
				cfg.Cache, err = sourcecache.New(t.Context(), cacheroot.New(t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = cfg.Cache.Close() })
			}
			o, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(o.Stop)
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := o.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"first", "second"} {
				id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
				artifact, ok := o.Store().GetArtifact(id).(*store.KustomizationArtifact)
				if !ok || len(artifact.Manifests) != 1 {
					t.Fatalf("remote render %s = %v", name, artifact)
				}
			}
		})
	}
}

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
				cfg.CacheDir = cacheRoot
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
			if artifact.FS == nil || artifact.Key == "" {
				t.Fatalf("selected cache %s did not reach discovery: %+v", cacheRoot, artifact)
			}
			if content, err := fs.ReadFile(artifact.FS, "value.txt"); err != nil || string(content) != "A" {
				t.Fatalf("committed artifact = %q, %v", content, err)
			}
		})
	}
}
