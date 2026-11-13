package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/selection"

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

func (d dagDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
	previous schedule.Result,
) schedule.Result {
	if previous.Assessment == nil {
		if probe := *d.o.probeSlot(id); probe != nil && !probe.ProbeConsumed {
			probe.ProbeConsumed = true
			previous.Assessment = probe
			defer probe.Retire()
		}
	}
	execution := d.o.controllers.ReconcileNode(ctx, id, level, previous)
	if execution == nil {
		return schedule.Result{}
	}
	return schedule.Result{Assessment: execution}
}

func (d dagDispatcher) Sweep(ctx context.Context, quiescent bool) []schedule.NodeID {
	changed := slices.Collect(d.o.store.Sweep())
	if quiescent && ctx.Err() == nil && d.o.session != nil && d.o.selection != nil {
		pending := slices.DeleteFunc(d.o.session.PendingSources(), d.o.includeNode)
		if err := reconcileConstruction(ctx, pending, d.o.src, d.o.tasks); err != nil {
			d.o.entryFailure = err
		}
		changed = append(changed, slices.Collect(d.o.store.Sweep())...)
	}
	if quiescent {
		d.o.sweepProbes(false)
		d.o.loader.SweepPreparations()
	}
	if !quiescent || len(changed) != 0 || ctx.Err() != nil {
		return changed
	}
	// Raw root admission MUST wait for its possible owners' settled publications.
	if d.o.loader != nil && d.admitRawRoots(ctx) {
		changed = slices.Collect(d.o.store.Sweep())
		if len(changed) != 0 {
			return changed
		}
	}
	if admitted, err := d.o.admitEntry(ctx); err != nil {
		d.o.entryFailure = err
	} else if admitted {
		return slices.Collect(d.o.store.Sweep())
	}
	d.o.classifyCompleted()
	return slices.Collect(d.o.store.Sweep())
}

func (o *Orchestrator) seedNodes() ([]manifest.NamedResource, error) {
	var ids []manifest.NamedResource
	for info := range manifest.Kinds() {
		if !info.Runnable {
			continue
		}
		for _, obj := range o.store.ListObjects(info.Group, info.Kind) {
			err := o.visitNodes(obj, false, func(id manifest.NamedResource, runnable bool) {
				if runnable {
					ids = append(ids, id)
				}
			})
			if err != nil {
				return nil, err
			}
		}
	}
	return ids, nil
}

func (o *Orchestrator) runDAG(ctx context.Context) error {
	o.sweepProbes(false)
	dispatcher := dagDispatcher{o}
	sched := schedule.New(dispatcher, dispatcher)

	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.StartLifecycle()
	o.rsc.StartLifecycle()
	o.agc.StartLifecycle()

	unsubscribe := o.watchDAG(sched)
	defer unsubscribe()

	seeds, err := o.seedNodes()
	if err != nil {
		return err
	}
	if o.session != nil {
		if phaseErr := reconcileConstruction(ctx, o.session.PendingSources(), o.src, o.tasks); phaseErr != nil {
			return phaseErr
		}
	}
	sched.Seed(seeds)
	err = sched.Run(ctx)
	o.freezeWork()

	return errors.Join(err, o.entryFailure, o.finalize())
}

func (o *Orchestrator) schedulable(obj manifest.BaseManifest) bool {
	if obj == nil {
		return false
	}
	info := manifest.KindOf(obj)
	return o.store.Policy(obj.Named()).Reconcile && (!info.IsSource() || o.src.Owns(obj.Named()))
}

func (o *Orchestrator) watchDAG(sched *schedule.Scheduler) func() {
	unsubAdd := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		obj, _ := payload.(manifest.BaseManifest)
		if obj == nil {
			sched.OnArrival(id, false)
			return
		}
		o.admission.AddApplication(o.store, obj)
		if err := o.visitNodes(obj, false, sched.OnArrival); err != nil {
			o.store.SetPreflight(id, err.Error())
			sched.OnArrival(id, true)
		}
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

func (o *Orchestrator) visitNodes(
	obj manifest.BaseManifest,
	dependency bool,
	yield func(manifest.NamedResource, bool),
) error {
	visited := map[manifest.NamedResource]bool{}
	var visit func(manifest.BaseManifest, bool) error
	visit = func(obj manifest.BaseManifest, dependency bool) error {
		id := obj.Named()
		if active, seen := visited[id]; seen {
			if active {
				return manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("source reference cycle at %s", id))
			}
			return nil
		}
		o.selection.Include(id)
		policy := o.store.Policy(id)
		eligible := o.schedulable(obj)
		if (o.selection != nil) && eligible && !o.includeNode(id) {
			return nil
		}
		runnable := eligible && (policy.Root || dependency)
		visited[id] = true
		if runnable {
			if err := o.visitNodeReferences(obj, visit); err != nil {
				return err
			}
		}
		visited[id] = false
		yield(id, runnable)
		return nil
	}
	return visit(obj, dependency)
}

func (o *Orchestrator) includeNode(id manifest.NamedResource) bool {
	return o.selection.Snapshot().Reconcile(id)
}

func (d dagDispatcher) admitRawRoots(ctx context.Context) bool {
	reads := d.o.selection.PendingRoots()
	if reads == nil {
		return false
	}
	defer d.o.store.Retire(reads)
	_, err := d.o.ksc.KeepEmitted(
		ctx,
		manifest.NamedResource{},
		selection.PrepareRequest{Reads: reads, Preparation: &d.o.admissionPreparation},
	)
	if err != nil {
		d.o.entryFailure = err
	}
	return true
}

func (o *Orchestrator) admitEntry(ctx context.Context) (bool, error) {
	if o.entryFailure != nil {
		return false, o.entryFailure
	}
	for {
		inputs := o.admission.EntryProducers()
		root, binding, err := o.admission.BindEntry(ctx, o.store)
		if err != nil {
			return false, err
		}
		if binding.Reads != nil {
			accepted, acceptErr := o.commitEntry(ctx, root, &binding)
			if acceptErr != nil || accepted {
				return accepted, acceptErr
			}
			continue
		}
		if slices.Equal(inputs, o.admission.EntryProducers()) {
			return false, nil
		}
		if err := o.installAdmission(ctx); err != nil {
			return false, err
		}
	}
}

func (o *Orchestrator) commitEntry(
	ctx context.Context,
	root *manifest.Kustomization,
	binding *discovery.Binding,
) (bool, error) {
	defer o.store.Retire(binding.Reads)
	defer binding.Reads.ReleaseCapture()
	changed := root == nil || root != o.store.GetDeclaration(root.Named())
	previous := o.entryBinding
	if changed {
		committed, err := o.publishEntry(ctx, root, binding)
		if err != nil || !committed {
			return false, err
		}
	}
	accepted, err := o.acceptEntry(ctx, binding)
	if err != nil || !accepted {
		return false, err
	}
	if !changed && previous.Producer == binding.Producer && previous.Source == binding.Source &&
		previous.Path == binding.Path {
		return true, nil
	}
	return true, o.installAdmission(ctx)
}

func (o *Orchestrator) publishEntry(
	ctx context.Context,
	root *manifest.Kustomization,
	binding *discovery.Binding,
) (bool, error) {
	incoming := store.Child{}
	if root != nil {
		application := manifest.EntryApplication{Source: root.EffectiveSourceID(), Path: root.Path}
		incoming = store.Child{Obj: root, Entry: &application}
	}
	if o.selection.Snapshot().Output(manifest.NamedResource{}) {
		if incoming.Obj != nil {
			binding.Reads.Admit(incoming, binding.Source, manifest.NamedResource{})
		}
		return binding.Reads.Valid(), nil
	}
	options := selection.AdmissionOptions{
		Request: selection.PrepareRequest{
			Generation: []store.Child{incoming}, Reads: binding.Reads, Preparation: &o.admissionPreparation,
		},
		Stage: selection.Execution,
	}
	if binding.Entry != nil {
		options.EntryHistory.Owner, options.EntryHistory.Records = binding.Entry.Owner, binding.Entry.Records
	}
	_, committed, err := o.prepareAdmission(ctx, options)
	return committed && binding.Reads.Valid(), err
}

func (o *Orchestrator) visitNodeReferences(
	obj manifest.BaseManifest,
	visit func(manifest.BaseManifest, bool) error,
) error {
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if o.store.Policy(ref.NamedResource).Root {
			continue
		}
		if target := o.store.GetObject(ref.NamedResource); target != nil && o.schedulable(target) {
			if err := visit(target, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Orchestrator) acceptEntry(ctx context.Context, binding *discovery.Binding) (bool, error) {
	accepted, err := o.admission.AcceptEntry(ctx, *binding)
	if err != nil {
		return false, err
	}
	if accepted == (discovery.Binding{}) {
		return false, nil
	}
	previous := o.entryBinding
	o.entryBinding = accepted
	if previous.Producer.ReportHidden() && previous.Producer != accepted.Producer {
		var seen []manifest.NamedResource
		owner := manifest.NamedResource{}
		if accepted.Entry != nil && accepted.Entry.Owner != previous.Producer {
			owner = accepted.Entry.Owner
		}
		for id := owner; id != (manifest.NamedResource{}); {
			parent, found := o.parentOf(id)
			if !found || slices.Contains(seen, parent) {
				break
			}
			if id == previous.Producer || parent == previous.Producer {
				return true, nil
			}
			seen = append(seen, parent)
			id = parent
		}
		o.store.DeleteObject(previous.Producer)
	}
	return true, nil
}
