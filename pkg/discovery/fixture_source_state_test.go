package discovery_test

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDiscovery_FixtureLatePinnedSources(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"apps-first-flux-system", "apps-first-cluster", "sources-first-flux-system", "sources-first-cluster"} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(
				t,
				"source-pins",
				"source-ref--pinned-source-discovered-after-consumer-"+variant,
			)
			st := store.New()
			result, err := discovery.Run(
				t.Context(),
				discovery.Config{Path: filepath.Join(fixture.Repo, "flux"), Store: st},
			)
			assert.NoError(t, err)
			artifact, ok := st.GetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "pinned"}, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(
				t,
				!ok || artifact.LocalPath == fixture.Repo,
				"expected committed source artifact, got %+v",
				artifact,
			)
			for _, file := range []struct{ name, path string }{{"hello", "cm.yaml"}, {"deleted", "deleted.yaml"}, {"extra", "extra.yaml"}} {
				id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: file.name}
				record, indexed := result.Index.Lookup(id)
				assert.FatalIf(
					t,
					!indexed || record.Path != filepath.Join(artifact.LocalPath, "apps", file.path) ||
						filepath.Join(fixture.Repo, filepath.FromSlash(record.File)) != record.Path,
					"pinned discovery metadata for %s: indexed=%t path=%q file=%q",
					id,
					indexed,
					record.Path,
					record.File,
				)
			}
		})
	}
}

func TestDiscovery_FixtureHeldErrorsAcrossNamespaces(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"external-source", "working-tree-source"} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "source-errors", "source-ref--held-error-across-namespaces-"+variant)
			l := loader.New(store.New())
			l.SourceRoot = fixture.Repo
			_, err := l.Load(t.Context(), filepath.Join(fixture.Repo, "broken"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			l.Admit(fixture.Repo)
			assert.FatalIf(
				t,
				!errors.Is(l.EntryFailure(t.Context(), fixture.Repo), manifest.ErrInput),
				"expected working-tree decode error",
			)
			st := store.New()
			_, err = discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(fixture.Repo, "flux"), Store: st})
			assert.NoError(t, err)
			for _, namespace := range []string{"alpha", "beta"} {
				ks, ok := st.Get[*manifest.Kustomization](testutil.KustomizationID(namespace, "apps"))
				assert.FatalIf(
					t,
					!ok || ks.SourceRef.Namespace != "" || ks.Path != "./broken",
					"expected implicit source in consumer namespace, got %+v",
					ks,
				)
				for ref := range manifest.References(ks, manifest.ReferenceRelations) {
					if ref.Use == "source" {
						assert.Equal(t, ref.Namespace, namespace)
					}
				}
			}
			id := manifest.NamedResource{
				Group:     "source.toolkit.fluxcd.io",
				Kind:      manifest.KindGitRepository,
				Namespace: "alpha",
				Name:      "cluster",
			}
			artifact, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(
				t,
				!ok || artifact.LocalRoot != fixture.Repo || artifact.LocalPath == fixture.Repo || artifact.Key == "" ||
					artifact.FS == nil,
				"expected alpha committed source artifact, got %+v",
				artifact,
			)
			pinned, err := fs.ReadFile(artifact.FS, "broken/cm.yaml")
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				!strings.Contains(string(pinned), "value: pinned"),
				"alpha lost committed bytes: %q",
				pinned,
			)
			if variant == "working-tree-source" {
				id.Namespace = "beta"
				beta, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
				assert.FatalIf(
					t,
					!ok || beta.LocalRoot != fixture.Repo || beta.LocalPath == fixture.Repo || beta.LocalPath == artifact.LocalPath || beta.Key != "" ||
						beta.FS == nil,
					"expected beta working-tree artifact, got %+v",
					beta,
				)
				current, err := fs.ReadFile(beta.FS, "broken/cm.yaml")
				assert.NoError(t, err)
				assert.Equal(t, string(current), "metadata: {name: broken\n")
			}
		})
	}
}

func TestDiscovery_FixtureInheritedHeldErrors(t *testing.T) {
	t.Parallel()
	for _, variant := range []struct {
		name, namespace string
		pinned          bool
	}{
		{name: "explicit-source-namespace", namespace: "flux-system"},
		{name: "implicit-source-namespace", namespace: "shifted"},
		{name: "pinned-inherited-namespace", namespace: "shifted", pinned: true},
		{name: "external-source-in-other-inherited-namespace", namespace: "shifted"},
		{name: "pinned-namespace-free-source", namespace: "shifted", pinned: true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(
				t,
				"source-errors",
				"source-ref--held-error-survives-namespace-inheritance-"+variant.name,
			)
			l := loader.New(store.New())
			l.SourceRoot = fixture.Repo
			_, err := l.Load(t.Context(), filepath.Join(fixture.Repo, "held"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			l.Admit(fixture.Repo)
			assert.FatalIf(
				t,
				!errors.Is(l.EntryFailure(t.Context(), fixture.Repo), manifest.ErrInput),
				"expected held consumer decode error",
			)
			st := store.New()
			_, err = discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(fixture.Repo, "flux"), Store: st})
			assert.NoError(t, err)
			ks, ok := st.Get[*manifest.Kustomization](testutil.KustomizationID("shifted", "a-held"))
			assert.FatalIf(
				t,
				!ok || ks.Path != "./held" || ks.SourceRef.Name != "unknown",
				"expected surviving inherited consumer, got %+v",
				ks,
			)
			var sourceRef manifest.NamedResource
			for ref := range manifest.References(ks, manifest.ReferenceRelations) {
				if ref.Use == "source" {
					sourceRef = ref.NamedResource
					break
				}
			}
			assert.Equal(t, sourceRef.Namespace, variant.namespace)
			if variant.pinned {
				artifact, ok := st.GetArtifact(sourceRef, store.ArtifactContent).(*store.SourceArtifact)
				assert.FatalIf(
					t,
					!ok || artifact.LocalRoot != fixture.Repo || artifact.LocalPath == fixture.Repo,
					"expected genuine inherited source pin, got %+v",
					artifact,
				)
			}
		})
	}
}
