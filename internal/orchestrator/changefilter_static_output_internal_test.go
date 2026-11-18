package orchestrator

import (
	"context"
	"iter"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestPrepareNeededHistory_StaticOutput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		static   bool
		mismatch bool
		absent   bool
		calls    int
	}{
		{name: "static", static: true},
		{name: "rendered", calls: 1},
		{name: "different input", static: true, mismatch: true, calls: 1},
		{name: "no configured input", static: true, absent: true, calls: 1},
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
			if tc.absent {
				o.cfg.Input = nil
			}
			o.store.SetArtifact(manifest.BootstrapSourceID, input)
			if tc.mismatch {
				o.store.SetArtifact(manifest.BootstrapSourceID, new(store.SourceArtifact))
			}
			o.store.AddObject(owner)

			o.loader = loader.New(o.store)
			o.loader.Index = o.index
			var err error
			o.selection, err = selection.New(
				o.store,
				o.loader,
				selection.Request{Roots: []manifest.NamedResource{owner.Named()}},
			)
			assert.NoError(t, err)
			var reads store.ReadSet
			reads.Init(o.store, owner.Named(), nil)
			defer reads.Retire()
			reads.RequireData(owner.Named())
			result, accepted, err := o.prepareAdmission(t.Context(), selection.AdmissionOptions{
				Request: selection.PrepareRequest{
					Invocation:  true,
					Reads:       &reads,
					Preparation: &o.admissionPreparation,
				},
				Stage: selection.Construction,
			})
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			_, accepted, err = o.prepareAdmission(t.Context(), selection.AdmissionOptions{
				Request: selection.PrepareRequest{Invocation: true}, Previous: &result, Stage: selection.Construction,
			})
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			defer o.admissionPreparation.Retire()
			history := map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{}
			for id, receipt := range o.history {
				if receipt.Records != nil {
					history[id] = receipt
				}
				assert.Equal(t, receipt.Complete, false)
			}

			assert.Equal(t, probe.calls, tc.calls)
			assert.Equal(t, len(history), 0)
			assert.Equal(t, len(o.history), tc.calls)
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
	schedule.Result,
) base.History {
	c.calls++
	return base.History{Records: nil, Complete: false}
}
