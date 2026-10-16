package orchestrator

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_DefaultGitCache(t *testing.T) {
	for _, mode := range []string{"memory", "disk", "injected"} {
		t.Run(mode, func(t *testing.T) {
			remote := t.TempDir()
			repo := testutil.GitInit(t, remote)
			testutil.WriteFile(t, remote, "app/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, remote, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\ndata: {value: committed}\n")
			testutil.GitCommitAll(t, repo, "source", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
			root := t.TempDir()
			testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: flux-system}\nspec:\n  url: file://"+filepath.ToSlash(remote)+"\n  interval: 10m\n")
			for _, name := range []string{"first", "second"} {
				testutil.WriteFile(t, root, name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  {path: ./app, sourceRef: {kind: GitRepository, name: remote}}\n"))
			}
			cfg := Config{Path: root, Concurrency: 2}
			if mode == "disk" {
				cfg.CacheDir = t.TempDir()
			}
			if mode == "injected" {
				cfg.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
			}
			o, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := o.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"first", "second"} {
				id := testutil.KustomizationID("flux-system", name)
				artifact, ok := o.Store().GetArtifact(id).(*store.Emission)
				if !ok || len(artifact.Manifests) != 1 {
					t.Fatalf("remote render %s = %v", name, artifact)
				}
			}
		})
	}
}

func TestBootstrap_LocalSourceUsesSelectedCache(t *testing.T) {
	for _, kind := range []string{"configured-directory", "injected-disk", "injected-memory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
			for i, value := range []string{"A", "B"} {
				testutil.WriteFile(t, root, "value.txt", value)
				hash := testutil.GitCommitAll(t, repo, value, &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
				if i == 0 {
					if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), hash)); err != nil {
						t.Fatal(err)
					}
				}
			}
			selectedRoot := t.TempDir()
			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root,
				SelfURLs: []string{"git://fixture.invalid/cluster"}, CacheDir: selectedRoot, Concurrency: 2}
			if kind != "configured-directory" {
				ownerRoot := ""
				if kind == "injected-disk" {
					ownerRoot = t.TempDir()
				}
				cfg.Cache = cachetest.New(t, ownerRoot, sourcecache.New)
			}
			o, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			if o.hrc.Helm == nil || o.ksc.Trees == nil || (cfg.Cache != nil && o.cache != cfg.Cache) {
				t.Fatal("renderers did not receive the selected cache before discovery")
			}
			if cfg.Cache != nil {
				entries, err := os.ReadDir(selectedRoot)
				if err != nil || len(entries) != 0 {
					t.Fatalf("injected owner created independent cache backing: entries=%v err=%v", entries, err)
				}
			}
			id := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
			artifact := o.Store().GetArtifact(id).(*store.SourceArtifact)
			if artifact.FS == nil || artifact.Key == "" {
				t.Fatalf("selected cache %s did not reach discovery: %+v", selectedRoot, artifact)
			}
			if content, err := fs.ReadFile(artifact.FS, "value.txt"); err != nil || string(content) != "A" {
				t.Fatalf("committed artifact = %q, %v", content, err)
			}
		})
	}
}
