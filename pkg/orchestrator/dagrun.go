package orchestrator

import (
	"context"
	"errors"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// dagDispatcher implements schedule.Dispatcher: it routes a node by Kind to the
// owning controller's ReconcileNode and maps the blocked result into a
// scheduler Outcome. This is the single composition point where the scheduler,
// the controllers, and the store meet — pkg/schedule itself imports none of
// them. NodeID is manifest.NamedResource, so the blocked slice flows through
// untranslated.
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

func (d dagDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, drainLevel manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
	out, blocked := d.reconcile(ctx, id, drainLevel)
	return out, blocked, id.Group == manifest.FluxOperatorDomain && id.Kind == manifest.KindResourceSet && d.o.rsc.WantsDrainRerun(id)
}

func (d dagDispatcher) reconcile(ctx context.Context, id schedule.NodeID, drainLevel manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID) {
	o := d.o
	var blocked []manifest.NamedResource
	switch {
	case id.Group == manifest.FluxKustomizeDomain && id.Kind == manifest.KindKustomization:
		blocked = o.ksc.ReconcileNode(ctx, id, drainLevel)
	case id.Group == manifest.HelmReleaseDomain && id.Kind == manifest.KindHelmRelease:
		blocked = o.hrc.ReconcileNode(ctx, id, drainLevel)
	case id.Group == manifest.FluxOperatorDomain && id.Kind == manifest.KindResourceSet:
		before, _ := o.store.GetArtifact(id).(*store.Emission)
		blocked = o.rsc.ReconcileNode(ctx, id, drainLevel)
		if len(blocked) == 0 && before != nil && before.Fingerprint != "" && o.store.GetArtifact(id) == before {
			info, ok := o.store.GetStatus(id)
			// A fresh fingerprint replaces the artifact even for identical docs.
			// Nil dependencies alone do not prove success: gates and skips
			// may preserve an artifact and an informative Ready status.
			// deleteLocked removes conditions with the object; Ready implies prior presence on this path.
			if ok && info.Outcome() == store.OutcomePassed && info.Message == "" {
				return schedule.OutcomeTerminalNoop, nil
			}
		}
	case o.src.Owns(id):
		blocked = o.src.ReconcileNode(ctx, id, drainLevel)
	default:
		// Not a schedulable kind (should never be dispatched) — terminal no-op.
		return schedule.OutcomeTerminal, nil
	}
	if len(blocked) > 0 {
		return schedule.OutcomeBlocked, blocked
	}
	if info, _ := o.store.GetStatus(id); info.IsDependencyFailure() {
		return schedule.OutcomeDependencyFailed, info.BlockedBy()
	}
	return schedule.OutcomeTerminal, nil
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
