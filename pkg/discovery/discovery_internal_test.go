package discovery

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDiscardPinnedWorkingTreeFiles_PreservesSources(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resolved bool
	}{
		{name: "resolved", resolved: true},
		{name: "referenced"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			st := store.New()
			repository := &manifest.GitRepository{
				Name: "cluster", Namespace: "flux-system", URL: "git://fixture.invalid/cluster",
				Reference: &manifest.GitRepositoryRef{Tag: "pinned"},
			}
			other := &manifest.GitRepository{Name: "other", Namespace: "flux-system"}
			ks := &manifest.Kustomization{
				Name: "root", Namespace: "flux-system", Path: "./",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: repository.Name, Namespace: repository.Namespace},
			}
			cm := &manifest.ConfigMap{Name: "dirty", Namespace: "flux-system"}
			artifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalRoot: root, LocalPath: t.TempDir(), Key: "pinned"}
			artifact.FS = os.DirFS(artifact.LocalPath)
			d := discoverer{
				cfg: Config{Store: st}, loader: loader.New(st),
				resolvedSources: map[manifest.NamedResource]*manifest.GitRepository{other.Named(): other},
			}
			d.loader.SourceRoot = root
			if tc.resolved {
				d.resolvedSources[repository.Named()] = repository
			}
			for _, obj := range []manifest.BaseManifest{repository, other, ks, cm} {
				st.AddObject(obj)
				file := obj.Named().Name + ".yaml"
				d.loader.Index.Records[obj.Named()] = manifest.DiscoveryRecord{File: file, Path: filepath.Join(root, file)}
			}
			r := d.loader.Index.Records[ks.Named()]
			r.Refs = []manifest.NamedResource{repository.Named()}
			d.loader.Index.Records[ks.Named()] = r
			for _, repo := range []*manifest.GitRepository{repository, other} {
				st.SetArtifact(repo.Named(), artifact)
				st.UpdateStatus(repo.Named(), store.StatusReady, "pinned")
			}
			d.discardPinnedWorkingTreeFiles(root)
			for _, repo := range []*manifest.GitRepository{repository, other} {
				id := repo.Named()
				status, ok := st.GetStatus(id)
				if st.GetObject(id) != repo || st.GetArtifact(id) != artifact || !ok || status.Status != store.StatusReady {
					t.Fatalf("source %s lost its resolved object, artifact or status", id)
				}
				if _, ok := d.loader.Index.Lookup(id); !ok || filesForTest(d.loader.Index)[id] == "" {
					t.Fatalf("source %s lost discovery metadata", id)
				}
			}
			if st.GetObject(ks.Named()) != ks || len(d.loader.Index.Records[ks.Named()].Refs) != 1 {
				t.Fatal("root Kustomization lost its object or source reference")
			}
			if _, ok := d.loader.Index.Lookup(cm.Named()); ok || st.GetObject(cm.Named()) != nil || filesForTest(d.loader.Index)[cm.Named()] != "" {
				t.Fatal("working-tree object survived the pinned root claim")
			}
			testutil.WriteFile(t, artifact.LocalPath, "source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', ref: {tag: other}}
`)
			d.loader.PreferExisting = true
			if _, err := d.loader.Load(t.Context(), artifact.LocalPath); err != nil {
				t.Fatal(err)
			}
			if st.GetObject(repository.Named()) != repository || st.GetArtifact(repository.Named()) != artifact {
				t.Fatal("committed subtree replaced the resolved source")
			}

		})
	}
}

func TestLoadAt_IgnoreFileAndMissingDirectory(t *testing.T) {
	t.Run("mounted symlink", func(t *testing.T) {
		root := t.TempDir()
		testutil.WriteFile(t, root, "host/kustomization.yaml", "resources: []\n")
		if err := os.Symlink(filepath.Join(root, "host"), filepath.Join(root, "apps")); err != nil {
			t.Fatal(err)
		}
		input := fstest.MapFS{
			"kustomization.yaml":      {Data: []byte("resources: [ks.yaml]\n")},
			"ks.yaml":                 {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner}\nspec: {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n")},
			"apps/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
			"apps/child.yaml":         {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: child}\nspec: {path: ./output, sourceRef: {kind: GitRepository, name: source}}\n")},
		}
		st := store.New()
		source := &manifest.GitRepository{Name: "source"}
		st.AddObject(source)
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, FS: input})
		st.SetArtifact(manifest.BootstrapSourceID, st.GetArtifact(source.Named()))
		d := discoverer{cfg: Config{Path: root, Store: st}, loader: loader.New(st)}
		if err := d.loadManifests(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "child"}
		if r, ok := d.loader.Index.Lookup(id); !ok || r.File != "apps/child.yaml" || st.GetObject(id) == nil {
			t.Fatalf("mounted child redirected by host symlink: %+v present=%t", r, ok)
		}
	})

	for _, backing := range []string{"disk", "memory"} {
		for _, scenario := range []string{"missing-directory", "file", "missing-ignore"} {
			t.Run(backing+"/"+scenario, func(t *testing.T) {
				root := t.TempDir()
				st := store.New()
				d := discoverer{cfg: Config{Store: st}, loader: loader.New(st)}
				d.loader.IgnoreFile = filepath.Join(root, "override.krmignore")
				target := filepath.Join(root, "missing")
				if scenario == "missing-ignore" {
					target = root
				} else {
					testutil.WriteFile(t, root, "override.krmignore", "")
				}
				if scenario == "file" {
					testutil.WriteFile(t, root, "missing", "content")
				}
				if backing == "memory" {
					input := fstest.MapFS{}
					if scenario != "missing-ignore" {
						input["override.krmignore"] = &fstest.MapFile{}
					}
					if scenario == "file" {
						input["missing"] = &fstest.MapFile{Data: []byte("content")}
					}
					st.AddObject(&manifest.GitRepository{Name: "source"})
					st.SetArtifact(manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "source"}, &store.SourceArtifact{LocalPath: root, FS: input})
				}
				total := 0
				err := d.loadAt(t.Context(), target, &total)
				if scenario == "missing-ignore" {
					if !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("missing explicit ignore error = %v", err)
					}
				} else if err != nil || total != 0 {
					t.Fatalf("missing followed path: loaded=%d, error=%v", total, err)
				}
			})
		}
	}
}
