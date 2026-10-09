package discovery

import (
	"path/filepath"
	"testing"

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
				SourceKind: manifest.KindGitRepository, SourceName: repository.Name, SourceNamespace: repository.Namespace,
			}
			cm := &manifest.ConfigMap{Name: "dirty", Namespace: "flux-system"}
			artifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalRoot: root, LocalPath: t.TempDir()}
			d := discoverer{
				cfg: Config{Store: st}, loader: loader.New(st),
				sourceFiles:     map[manifest.NamedResource]string{},
				sourceRefs:      map[manifest.NamedResource][]manifest.NamedResource{},
				resolvedSources: map[manifest.NamedResource]*manifest.GitRepository{other.Named(): other},
			}
			if tc.resolved {
				d.resolvedSources[repository.Named()] = repository
			}
			d.loader.Existence = loader.NewExistenceIndex()
			for _, obj := range []manifest.BaseManifest{repository, other, ks, cm} {
				st.AddObject(obj)
				file := obj.Named().Name + ".yaml"
				d.sourceFiles[obj.Named()] = file
				d.loader.Existence.Record(obj.Named(), filepath.Join(root, file))
			}
			d.sourceRefs[ks.Named()] = []manifest.NamedResource{repository.Named()}
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
				if _, ok := d.loader.Existence.Get(id); !ok || d.sourceFiles[id] == "" {
					t.Fatalf("source %s lost discovery metadata", id)
				}
			}
			if st.GetObject(ks.Named()) != ks || len(d.sourceRefs[ks.Named()]) != 1 {
				t.Fatal("root Kustomization lost its object or source reference")
			}
			if _, ok := d.loader.Existence.Get(cm.Named()); ok || st.GetObject(cm.Named()) != nil || d.sourceFiles[cm.Named()] != "" {
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
