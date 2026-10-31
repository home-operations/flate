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
	if d.o.entryBinding == (discovery.Binding{}) && d.o.entryFailure == nil {
		root, binding, err := d.o.admission.BindEntry(ctx, d.o.store)
		d.o.entryFailure = err
		if root != nil {
			d.o.entryBinding = binding
			if d.o.store.GetObject(root.Named()) == nil {
				d.o.ksc.KeepEmitted(manifest.NamedResource{}, store.Child{Obj: root}, nil)
				d.o.store.AddObject(root)
			}
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
			o.admission.AddApplication(o.store, obj)
			if o.schedulable(obj) {
				o.store.SetIncluded(obj.Named(), o.filter.ShouldReconcile(obj.Named()))
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
	o.fic.StartLifecycle()
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
	unsubAdd := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		obj, _ := payload.(manifest.BaseManifest)
		if obj == nil {
			sched.OnArrival(id, false)
			return
		}
		o.admission.AddApplication(o.store, obj)
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
