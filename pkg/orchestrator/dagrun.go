package orchestrator

import (
	"context"
	"errors"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// dagDispatcher routes invocations to their owning controller.
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

func (d dagDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait) {
	o := d.o
	switch {
	case id.Group == manifest.FluxKustomizeDomain && id.Kind == manifest.KindKustomization:
		return o.ksc.ReconcileNode(ctx, id, attempt)
	case id.Group == manifest.HelmReleaseDomain && id.Kind == manifest.KindHelmRelease:
		return o.hrc.ReconcileNode(ctx, id, attempt)
	case id.Group == manifest.FluxOperatorDomain && id.Kind == manifest.KindResourceSet:
		return o.rsc.ReconcileNode(ctx, id, attempt)
	case o.src.Owns(id):
		return o.src.ReconcileNode(ctx, id, attempt)
	default:
		return schedule.Outcome{}, manifest.ReconcileWait{}
	}
}

// seedNodes returns every file-loaded reconcilable + ResourceSet + source
// object currently in the store — the scheduler's initial frontier.
// ResourceSetInputProviders are NOT seeded: they are pure data, not
// runnable nodes according to the manifest kind table.
func (o *Orchestrator) seedNodes() []manifest.NamedResource {
	var ids []manifest.NamedResource
	for info := range manifest.Kinds() {
		if !info.Runnable {
			continue
		}
		for _, obj := range o.store.ListObjects(info.Group, info.Kind) {
			if o.schedulable(obj) {
				ids = append(ids, obj.Named())
			}
		}
	}
	return ids
}

// runDAG drives reconciliation with the re-entrant fixpoint scheduler instead
// of the event-driven dispatch loop. The cycle-detect listener is already
// registered by Run (shared with the event path), so preflight cycle failures
// are recorded before any node runs.
func (o *Orchestrator) runDAG(ctx context.Context) error {
	dispatcher := dagDispatcher{o}
	sched := schedule.New(dispatcher, dispatcher)

	// Wake adapters MUST be registered before any reconcile can publish output.
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.Start(ctx)
	o.rsc.StartLifecycle()

	unsubscribe := o.watchDAG(sched)
	defer unsubscribe()

	sched.Seed(o.seedNodes())
	err := sched.Run(ctx)

	return errors.Join(err, o.finalize())
}

func (o *Orchestrator) schedulable(obj manifest.BaseManifest) bool {
	if obj == nil {
		return false
	}
	info := manifest.KindOf(obj)
	return info.Runnable && (info.Group != manifest.SourceDomain || o.src.Owns(obj.Named()))
}

func (o *Orchestrator) watchDAG(sched *schedule.Scheduler) func() {
	// Store → scheduler adapters (fire-only, flush=false), registered AFTER the
	// shared cycle-detect listener so cycle preflight runs first.
	//
	//   EventObjectAdded: discover render-emitted nodes, wake nodes parked on
	//   an arriving dep (source/CM/KS), and re-dispatch a terminal producer
	//   whose Refire status-reset re-arrival signals a changed-only
	//   resurrection. The listener runs OUTSIDE the store shard lock (AddObject
	//   fires it post-unlock), and sched.OnArrival only updates
	//   scheduler state, so there is no store-lock ↔ sched.mu inversion.
	unsubAdd := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		obj, _ := payload.(manifest.BaseManifest)
		if obj == nil {
			sched.OnArrival(id, false)
			return
		}
		sched.OnArrival(id, o.schedulable(obj))
	}, false)
	//   EventStatusUpdated: wake nodes parked on a dep that reached a terminal
	//   status. Non-terminal (Pending) updates return before the scheduler lock.
	unsubStatus := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		info, ok := payload.(store.StatusInfo)
		if !ok {
			return
		}
		sched.OnStatusWake(id, info.Status == store.StatusReady, info.Status == store.StatusFailed)
	}, false)

	return func() {
		unsubStatus()
		unsubAdd()
	}
}
