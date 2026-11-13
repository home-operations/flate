package orchestrator

import (
	"context"
	"iter"
	"maps"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

type publicationHistoryController struct {
	historyProbeController

	t            *testing.T
	store        *store.Store
	availability string
	published    *bool
	rejected     manifest.NamedResource
	accepted     manifest.NamedResource
}

func (c *publicationHistoryController) PreviousRender(
	_ context.Context,
	object manifest.BaseManifest,
	previous schedule.Result,
) base.History {
	c.calls++
	_, path, _ := object.SourcePath()
	prior, _ := previous.Assessment.(*base.Execution)
	assert.Equal(c.t, prior != nil, c.calls != 1)
	if c.calls == 1 {
		assert.Equal(c.t, path, "before")
		assert.Equal(c.t, *c.published, false)
	} else {
		assert.Equal(c.t, path, "after")
		assert.Equal(c.t, *c.published, true)
	}
	assessment := &base.Execution{Controller: &base.Controller{Store: c.store}}
	assessment.Reads.Init(c.store, object.Named(), nil)
	assessment.Reads.GetDeclaration(object.Named())
	history := base.History{Assessment: assessment}
	if c.calls == 1 {
		history.Records = maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{
			object.Named(): {File: "rejected.yaml", Refs: []manifest.NamedResource{c.rejected}},
		})
		history.Complete = true
		return history
	}
	if c.availability == "unavailable" {
		return history
	}
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	if c.availability == "incomplete" {
		records[object.Named()] = manifest.DiscoveryRecord{
			File: "accepted.yaml",
			Refs: []manifest.NamedResource{c.accepted},
		}
	}
	history.Records, history.Complete = maps.All(records), c.availability == "complete empty"
	return history
}

func TestPrepareAdmission_HistorySurvivesPublication(t *testing.T) {
	t.Parallel()
	for _, availability := range []string{"unavailable", "incomplete", "complete empty"} {
		t.Run(availability, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := new(store.SourceArtifact)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "before"}
			st.AddObject(owner)
			published := false
			probe := &publicationHistoryController{
				t: t, store: st, availability: availability, published: &published,
				rejected: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "rejected"},
				accepted: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "accepted"},
			}
			borrowed := map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{owner.Named(): {}}
			o := &Orchestrator{
				cfg: treeConfig{Input: input, missing: depwait.NewMissingPolicy(nil)}, store: st, history: borrowed,
				index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner.Named(): {Declaration: owner},
				}},
				controllers: base.Controllers{Kustomization: probe},
			}
			defer o.sweepProbes(true)
			o.loader = loader.New(st)
			o.loader.Index = o.index
			var err error
			o.selection, err = selection.New(
				st,
				o.loader,
				selection.Request{Roots: []manifest.NamedResource{owner.Named()}},
			)
			assert.NoError(t, err)
			defer func() { o.admissionPreparation.Retire() }()
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			defer reads.Retire()
			reads.RequireData(owner.Named())
			initial, committed, err := o.prepareAdmission(t.Context(), selection.AdmissionOptions{
				Request: selection.PrepareRequest{
					Invocation:  true,
					Reads:       &reads,
					Preparation: &o.admissionPreparation,
				},
				Stage: selection.Construction,
			})
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, probe.calls, 0)
			assert.Equal(t, initial.Request.History[owner.Named()].Records == nil, true)
			st.SetIncluded(owner.Named(), false, false)
			stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if published || id != owner.Named() {
					return
				}
				published = true
				replacement := owner.Clone()
				replacement.Path = "after"
				st.AddObject(replacement)
			}, false)
			defer stop()
			accepted, committed, err := o.prepareAdmission(t.Context(), selection.AdmissionOptions{
				Request: selection.PrepareRequest{Invocation: true}, Previous: &initial, Stage: selection.Execution,
			})
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, published, true)
			assert.Equal(t, probe.calls, 2)
			assert.Equal(t, accepted.Request.Preparation, nil)
			assert.Equal(t, len(accepted.Demands), 0)
			history, present := o.history[owner.Named()]
			assert.Equal(t, present, true)
			assert.Equal(t, history.Records != nil, availability != "unavailable")
			assert.Equal(t, history.Complete, availability == "complete empty")
			if history.Records != nil {
				records := maps.Collect(history.Records)
				assert.Equal(t, len(records) == 1, availability == "incomplete")
				if availability == "incomplete" {
					assert.Diff(t, records[owner.Named()].Refs, []manifest.NamedResource{probe.accepted})
				}
			}
			assert.Equal(t, len(borrowed), 1)
			assert.Equal(t, borrowed[owner.Named()].Records == nil, true)
			assert.Equal(t, borrowed[owner.Named()].Complete, false)
			o.sweepProbes(true)
			var nextReads store.ReadSet
			nextReads.Init(st, manifest.NamedResource{}, nil)
			defer nextReads.Retire()
			nextReads.RequireData(owner.Named())
			next, committed, err := o.prepareAdmission(t.Context(), selection.AdmissionOptions{
				Request: selection.PrepareRequest{Invocation: true, Reads: &nextReads}, Stage: selection.Construction,
			})
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, probe.calls, 2)
			assert.Equal(t, slices.Contains(next.Needed, probe.rejected), false)
			assert.Equal(t, slices.Contains(next.Needed, probe.accepted), availability == "incomplete")
			assert.Equal(t, next.Request.History[owner.Named()].Complete, history.Complete)
			assert.Equal(t, accepted.Request.History[owner.Named()].Complete, history.Complete)
			assert.Equal(t, borrowed[owner.Named()].Records == nil, true)
			assert.Equal(t, borrowed[owner.Named()].Complete, false)
		})
	}
}
