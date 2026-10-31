package orchestrator

import (
	"context"
	"errors"
	"slices"

	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

type dagDispatcher struct{ o *Orchestrator }

// Go admits scheduler bodies through the invocation's shared task service.
func (d dagDispatcher) Go(ctx context.Context, name string, fn func(context.Context)) {
	d.o.tasks.Go(ctx, name, fn)
}

// BlockTillDone cancels remote owners before joining the shared task service.
func (d dagDispatcher) BlockTillDone() {
	d.o.ksc.Trees.Close()
	d.o.inputs.Close()
	d.o.tasks.BlockTillDone()
}

func (d dagDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	execution := d.o.controllers.ReconcileNode(ctx, id, level)
	if execution == nil {
		return schedule.Result{}
	}
	return schedule.Result{Assessment: execution}
}

func (d dagDispatcher) Sweep(ctx context.Context, quiescent bool) []schedule.NodeID {
	changed := slices.Collect(d.o.store.Sweep())
	if !quiescent || len(changed) != 0 || ctx.Err() != nil {
		return changed
	}
	// Raw root admission MUST wait for its possible owners' settled publications.
	if d.o.loader != nil {
		var reads *store.ReadSet
		for id, record := range d.o.index.Files() {
			_, selected, _, _ := d.o.filter.Membership(id)
			if selected && record.Path != "" && record.Parent == (manifest.NamedResource{}) && record.BuilderOwner == (manifest.NamedResource{}) && d.o.loader.AdmitsRoot(d.o.loader.CatalogObject(id), record.Path) && d.o.store.GetObject(id) == nil {
				if reads == nil {
					reads = &store.ReadSet{}
					reads.Init(d.o.store, manifest.NamedResource{}, nil)
				}
				reads.RequireData(id)
			}
		}
		if reads != nil {
			_, err := d.o.ksc.KeepEmitted(ctx, manifest.NamedResource{}, store.Child{}, reads)
			reads.Retire()
			if err != nil {
				d.o.entryFailure = err
			}
			changed = slices.Collect(d.o.store.Sweep())
			if len(changed) != 0 {
				return changed
			}
		}
	}
	if d.o.entryBinding == (discovery.Binding{}) && d.o.entryFailure == nil {
		root, binding, err := d.o.admission.BindEntry(ctx, d.o.store)
		d.o.entryFailure = err
		if root != nil {
			d.o.entryBinding = binding
			if _, err := d.o.ksc.KeepEmitted(ctx, manifest.NamedResource{}, store.Child{Obj: root}, nil); err != nil {
				d.o.entryFailure = err
				return changed
			}
			d.o.store.AddObject(root)
			return slices.Collect(d.o.store.Sweep())
		}
	}
	d.o.classifyCompleted()
	return slices.Collect(d.o.store.Sweep())
}

func (o *Orchestrator) seedNodes() []manifest.NamedResource {
	var ids []manifest.NamedResource
	for info := range manifest.Kinds() {
		if !info.Runnable {
			continue
		}
		for _, obj := range o.store.ListObjects(info.Group, info.Kind) {
			if o.schedulable(obj) {
				if o.filter.Enabled() {
					if !o.includeNode(obj.Named()) {
						continue
					}
				}
				ids = append(ids, obj.Named())
			}
		}
	}
	return ids
}

func (o *Orchestrator) runDAG(ctx context.Context) error {
	dispatcher := dagDispatcher{o}
	sched := schedule.New(dispatcher, dispatcher)

	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.StartLifecycle()
	o.rsc.StartLifecycle()
	o.agc.StartLifecycle()

	unsubscribe := o.watchDAG(sched)
	defer unsubscribe()

	sched.Seed(o.seedNodes())
	err := sched.Run(ctx)

	return errors.Join(err, o.entryFailure, o.finalize())
}

func (o *Orchestrator) schedulable(obj manifest.BaseManifest) bool {
	if obj == nil {
		return false
	}
	info := manifest.KindOf(obj)
	return info.Runnable && (info.Group != manifest.SourceDomain || o.src.Owns(obj.Named()))
}

func (o *Orchestrator) watchDAG(sched *schedule.Scheduler) func() {
	observeEntry := o.entryBinding == (discovery.Binding{})
	unsubAdd := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		obj, _ := payload.(manifest.BaseManifest)
		if obj == nil {
			sched.OnArrival(id, false)
			return
		}
		if observeEntry {
			o.admission.AddApplication(o.store, obj)
		}
		if o.filter.Enabled() && o.schedulable(obj) && !o.includeNode(id) {
			return
		}
		sched.OnArrival(id, o.schedulable(obj))
	}, false)

	publication := func(manifest.NamedResource, any) { sched.OnPublication() }
	unsubStatus := o.store.AddListener(store.EventStatusUpdated, publication, false)
	unsubArtifact := o.store.AddListener(store.EventArtifactUpdated, publication, false)
	return func() {
		unsubAdd()
		unsubStatus()
		unsubArtifact()
	}
}

func (o *Orchestrator) includeNode(id manifest.NamedResource) bool {
	included := o.filter.ShouldReconcile(id)
	o.store.SetIncluded(id, included)
	return included
}
