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
// untranslated. The controllers' ReconcileNode also reports a readiness bool,
// which the scheduler does not consume, so it is discarded here.
type dagDispatcher struct{ o *Orchestrator }

func (d dagDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, drainLevel int) (schedule.Outcome, []schedule.NodeID) {
	o := d.o
	var blocked []manifest.NamedResource
	switch {
	case id.Kind == manifest.KindKustomization:
		blocked, _ = o.ksc.ReconcileNode(ctx, id, drainLevel)
	case id.Kind == manifest.KindHelmRelease:
		blocked, _ = o.hrc.ReconcileNode(ctx, id, drainLevel)
	case id.Kind == manifest.KindResourceSet:
		blocked, _ = o.rsc.ReconcileNode(ctx, id, drainLevel)
	case o.src.HasFetcher(id.Kind):
		blocked, _ = o.src.ReconcileNode(ctx, id, drainLevel)
	default:
		// Not a schedulable kind (should never be dispatched) — terminal no-op.
		return schedule.OutcomeTerminal, nil
	}
	if len(blocked) > 0 {
		return schedule.OutcomeBlocked, blocked
	}
	return schedule.OutcomeTerminal, nil
}

// dagSchedulable reports whether id is a node the scheduler runs
// (Kustomization/HelmRelease/ResourceSet/source) versus pure data
// (ConfigMap/Secret/RSIP) that only WAKES nodes parked on it. A
// ResourceSetInputProvider is deliberately excluded: a render-emitted
// RSIP fires EventObjectAdded to wake RSes parked on it, but it is not a
// runnable node (flate exports its inputs statically, there is nothing to
// reconcile).
func (o *Orchestrator) dagSchedulable(id manifest.NamedResource) bool {
	return isReconcilableKind(id.Kind) ||
		id.Kind == manifest.KindResourceSet ||
		o.src.HasFetcher(id.Kind)
}

// seedNodes returns every file-loaded reconcilable + ResourceSet + source
// object currently in the store — the scheduler's initial frontier.
// ResourceSetInputProviders are NOT seeded: they are pure data, not
// runnable nodes (see dagSchedulable).
func (o *Orchestrator) seedNodes() []manifest.NamedResource {
	var ids []manifest.NamedResource
	for _, kind := range reconcilableKinds {
		for _, obj := range o.store.ListObjects(kind) {
			ids = append(ids, obj.Named())
		}
	}
	for _, obj := range o.store.ListObjects(manifest.KindResourceSet) {
		ids = append(ids, obj.Named())
	}
	for kind := range o.src.Fetchers {
		for _, obj := range o.store.ListObjects(kind) {
			ids = append(ids, obj.Named())
		}
	}
	return ids
}

// runDAG drives reconciliation with the re-entrant fixpoint scheduler instead
// of the event-driven dispatch loop. The cycle-detect listener is already
// registered by Run (shared with the event path), so preflight cycle failures
// are recorded before any node runs.
func (o *Orchestrator) runDAG(ctx context.Context) error {
	defer o.Stop()
	sched := schedule.New(o.tasks, dagDispatcher{o})
	// A selector-inputsFrom ResourceSet has no nameable input provider to park
	// on, so the scheduler re-expands it at the structural fixpoint (against the
	// now-complete store). Only ResourceSets ever opt in.
	sched.SetRerunAtDrain(func(id manifest.NamedResource) bool {
		return id.Kind == manifest.KindResourceSet && o.rsc.WantsDrainRerun(id)
	})

	// Start the controllers. This registers lifecycle state + the HR producer
	// index (flush=true replay), but no dispatch listeners — the scheduler owns
	// dispatch. No render runs here, so no children are emitted before the wake
	// adapters below are registered.
	o.src.Start(ctx)
	o.ksc.Start(ctx)
	o.hrc.Start(ctx)
	o.rsc.Start(ctx)

	// Store → scheduler adapters (fire-only, flush=false), registered AFTER the
	// shared cycle-detect listener so cycle preflight runs first.
	//
	//   EventObjectAdded: discover render-emitted nodes, wake nodes parked on
	//   an arriving dep (source/CM/KS), and re-dispatch a terminal producer
	//   whose Refire status-reset re-arrival signals a changed-only
	//   resurrection. The listener runs OUTSIDE the store shard lock (AddObject
	//   fires it post-unlock), and dagStatusReady's read completes before
	//   sched.OnArrival takes sched.mu — so there is no store-lock ↔ sched.mu
	//   inversion.
	unsubAdd := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		sched.OnArrival(id, o.dagSchedulable(id))
	}, false)
	defer unsubAdd()
	//   EventStatusUpdated: wake nodes parked on a dep that reached a terminal
	//   status. Terminal-gated inside OnStatusWake (Pending writes are ignored).
	unsubStatus := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		info, ok := payload.(store.StatusInfo)
		if !ok {
			return
		}
		sched.OnStatusWake(id, info.Status == store.StatusReady, info.Status == store.StatusFailed)
	}, false)
	defer unsubStatus()

	sched.Seed(o.seedNodes())
	sched.Run(ctx)

	return errors.Join(o.finalize(), ctx.Err())
}
