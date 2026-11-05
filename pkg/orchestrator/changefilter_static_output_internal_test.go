package orchestrator

import (
	"context"
	"iter"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPrepareNeededHistory_StaticOutput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		static   bool
		mismatch bool
		calls    int
	}{
		{name: "static", static: true},
		{name: "rendered", calls: 1},
		{name: "different input", static: true, mismatch: true, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := new(store.SourceArtifact)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			probe := new(historyProbeController)
			o := &Orchestrator{
				cfg:   treeConfig{Input: input},
				store: store.New(),
				index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner.Named(): {Declaration: owner, StaticOutput: tc.static},
				}},
				controllers: base.Controllers{Kustomization: probe},
			}
			o.store.SetArtifact(manifest.BootstrapSourceID, input)
			if tc.mismatch {
				o.store.SetArtifact(manifest.BootstrapSourceID, new(store.SourceArtifact))
			}
			o.store.AddObject(owner)
			o.loader = loader.New(o.store)
			history := map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{}
			assert.NoError(t, o.prepareNeededHistory(t.Context(), []manifest.NamedResource{owner.Named()}, history))
			assert.Equal(t, probe.calls, tc.calls)
			assert.Equal(t, len(history), 0)
		})
	}
}

type historyProbeController struct {
	reconcileController

	calls int
}

func (*historyProbeController) Owns(manifest.NamedResource) bool { return true }

func (c *historyProbeController) PreviousRender(
	context.Context,
	manifest.BaseManifest,
	*store.ReadSet,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	c.calls++
	return nil, false
}
