package source

import (
	"path/filepath"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

func TestController_FixtureLocalSourcesNeverFetch(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run("missing/"+kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			consumer := &manifest.Kustomization{
				Name: "consumer", Namespace: "apps", Path: "./payload",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: "shared", Namespace: "sources"},
			}
			st.AddObject(consumer)
			result, err := runDiscovery(t.Context(), discovery.Config{Path: root, Store: st})
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			id := consumer.SourceID()
			artifact, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.Equal(t, ok, true)
			assert.Equal(t, artifact.InputAlias, true)
			child, resolved, err := result.Admission.Resolve(t.Context(), id, st.GetObject(id), nil)
			assert.NoError(t, err)
			assert.Equal(t, child.Obj == st.GetObject(id), true)
			assert.Equal(t, resolved == artifact, true)
			fetcher := &fakeFetcher{}
			controller, isolated := newController(t, map[string]src.Fetcher{kind: fetcher})
			isolated.AddObject(child.Obj)
			isolated.SetArtifact(id, artifact)
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
				Tasks: controller.Tasks, Store: isolated, ID: id, ReconcileNode: controller.ReconcileNode,
			})
			assert.Equal(t, info.Status, store.StatusReady)
			assert.Equal(t, fetcher.calls, 0)
		})
	}
	for _, variant := range []string{
		"nonhead-renders-preserve-checkout-tag",
		"nonhead-renders-preserve-checkout-commit",
		"nonhead-renders-preserve-checkout-semver",
		"nonhead-renders-preserve-checkout-head",
		"nonhead-renders-preserve-checkout-no-ref",
		"source-ref--pinned-followed-source-inherits-namespace-sub-omitted",
		"source-ref--pinned-root-preserves-source",
		"source-ref--pinned-source-discovered-after-consumer-apps-first-cluster",
		"source-ref--pinned-source-discovered-after-consumer-sources-first-flux-system",
	} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "source-pins", variant)
			st := store.New()
			_, err := runDiscovery(t.Context(), discovery.Config{Path: filepath.Join(fixture.Repo, "flux"), Store: st})
			assert.NoError(t, err)
			fetcher := &fakeFetcher{}
			var admitted int
			for _, source := range st.ListAs[*manifest.GitRepository]("source.toolkit.fluxcd.io", manifest.KindGitRepository) {
				artifact, ok := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
				if !ok || artifact.FS == nil {
					continue
				}
				admitted++
				controller, isolated := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: fetcher})
				isolated.AddObject(source)
				isolated.SetArtifact(source.Named(), artifact)
				info := controllertest.DispatchToFixpoint(
					t.Context(),
					t,
					controllertest.Dispatch{
						Tasks:         controller.Tasks,
						Store:         isolated,
						ID:            source.Named(),
						ReconcileNode: controller.ReconcileNode,
					},
				)
				assert.FatalIf(
					t,
					info.Status != store.StatusReady,
					"admitted source %s status = %+v",
					source.Named(),
					info,
				)
			}
			assert.FatalIf(t, admitted == 0, "fixture supplied no admitted source")
			assert.Equal(t, fetcher.calls, 0)
		})
	}
}
