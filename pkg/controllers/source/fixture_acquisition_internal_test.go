package source

import (
	"path/filepath"
	"testing"

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
			_, err := discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(fixture.Repo, "flux"), Store: st})
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
