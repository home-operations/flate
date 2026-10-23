package helmrelease

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func publishDocuments(c *Controller, id manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	if c.Store.GetObject(id) == nil {
		c.Store.AddObject(&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace})
	}
	children, retained := base.ParseChildren(id, docs, opts)
	_, next := c.Store.GetEmission(id)
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = children, retained, true
	c.Emit(id, &next)
	return children, retained
}

func parentIndexForTest(parents map[manifest.NamedResource]manifest.NamedResource) *manifest.DiscoveryIndex {
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	for id, parent := range parents {
		index.Records[id] = manifest.DiscoveryRecord{Parent: parent, Productions: []manifest.Production{{Owner: parent}}}
	}
	return index
}

func publishRawFixture(t *testing.T, c *Controller, obj manifest.BaseManifest) {
	t.Helper()
	root := &manifest.Kustomization{Name: "producer-root", Namespace: obj.Named().Namespace}
	if c.Store.GetObject(root.Named()) == nil {
		c.Store.AddObject(root)
	}
	c.Emit(root.Named(), &store.Emission{Children: []store.Child{{Obj: obj}}})
}

func runValuesFixture(t *testing.T, c *Controller, hr *manifest.HelmRelease) store.StatusInfo {
	t.Helper()
	dispatch := valuesFixtureDispatcher{c: c, reader: hr.Named()}
	sched := schedule.New(c.Tasks, dispatch)
	arrivals := c.Store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		sched.OnArrival(id, id == hr.Named())
	}, false)
	defer arrivals()
	statuses := c.Store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		info := payload.(store.StatusInfo)
		sched.OnStatusWake(id, info.Status == store.StatusReady, info.Status == store.StatusFailed)
	}, false)
	defer statuses()
	ids := []manifest.NamedResource{hr.Named()}
	for ref := range manifest.References(hr) {
		if ref.Use != "valuesFrom" {
			continue
		}
		for _, p := range c.Store.Productions(ref.NamedResource, nil) {
			if p.Owner != (manifest.NamedResource{}) && p.Owner != hr.Named() && !slices.Contains(ids, p.Owner) {
				ids = append(ids, p.Owner)
			}
		}
	}
	sched.Seed(ids)
	if err := sched.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	info, _ := c.Store.GetStatus(hr.Named())
	return info
}

type valuesFixtureDispatcher struct {
	c      *Controller
	reader manifest.NamedResource
}

func (d valuesFixtureDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait) {
	if id != d.reader {
		d.c.Store.UpdateStatus(id, store.StatusReady, "", "")
		return schedule.Outcome{}, manifest.ReconcileWait{}
	}
	return d.c.ReconcileNode(ctx, id, attempt)
}
