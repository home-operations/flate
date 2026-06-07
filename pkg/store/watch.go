package store

import (
	"context"
	"errors"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
)

// FailedGrace is how long WatchReady will wait, after observing a
// Failed status, for the resource to potentially flip back to Ready.
// flate's reconcile model allows a resource to be re-emitted (and
// re-reconciled) by a parent Kustomization's render after an initial
// failure — most commonly when the parent's strategic-merge patches
// inject the postBuild.substituteFrom the child needs. The grace
// window absorbs that brief Failed→Ready transition so dependents
// don't propagate a transient failure.
//
// Tuned to be much shorter than the per-dep DefaultTimeout (30s) so
// genuinely broken resources still fail relatively fast, but long
// enough to cover the parent-render re-emission (usually <1s).
//
// Exposed as a var so tests can shrink it to keep their wall time
// down.
var FailedGrace = 3 * time.Second

// WatchReady blocks until the resource reaches Ready status — or
// stays Failed for FailedGrace without flipping to Ready, in which
// case it returns the Failed StatusInfo and a *ResourceFailedError.
// Cancellation via ctx returns ctx.Err().
//
// A Failed transition does not short-circuit immediately because
// flate's parent-Kustomization render can re-emit a child after the
// child's initial reconcile has already failed; the grace window
// lets that recovery land before dependents propagate the failure.
//
// quiesce, when non-nil, cancels the grace as soon as it closes — a
// signal from the embedder that "no future reconcile could re-emit
// this dep" (e.g., the orchestrator's task pool has drained). For a
// typo'd dependsOn there's nothing to re-emit, so the grace is pure
// wall-clock waste; the quiesce hook lets dep-waiters fast-fail the
// moment the orchestrator gives up on finding more work. Pass nil
// from contexts that can't surface that signal (legacy callers,
// tests) and the wait keeps its full grace budget.
func (s *Store) WatchReady(ctx context.Context, id manifest.NamedResource, quiesce <-chan struct{}) (StatusInfo, error) {
	// Short-circuit on Ready — no transition to wait for.
	if info, ok := s.GetStatus(id); ok && info.Status == StatusReady {
		return info, nil
	}

	// Subscribe with a wake-only signal — the actual status is read
	// back from the store on every wake. Carrying StatusInfo through
	// the channel directly would lose Failed→Ready transitions when
	// the buffer-1 channel drops on a default-send.
	//
	// The subscribe + initial status read run under rLockAll to
	// serialize against writers: any UpdateStatus that completes
	// AFTER this critical section will fire our listener (because
	// we're in its listener-set snapshot); any update that completed
	// BEFORE this section is already reflected in the GetStatus read.
	// The wake-loop's per-event GetStatus continues to absorb later
	// updates.
	wake := make(chan struct{}, 1)
	listener := func(other manifest.NamedResource, _ any) {
		if other != id {
			return
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	initial, hasInitial, unsub := s.subscribeWithStatus(listener, id)
	defer unsub()

	var currentFailed *StatusInfo
	if hasInitial {
		if initial.Status == StatusReady {
			return initial, nil
		}
		if initial.Status == StatusFailed {
			f := initial
			currentFailed = &f
		}
	}

	// graceTimer fires when the post-Failed grace window expires.
	// Arm it only once a Failed has been observed. We use NewTimer
	// (not time.After) so an early return via ctx-cancel or a
	// Ready transition can Stop() the timer — otherwise it stays
	// armed in the runtime until expiry, leaking the underlying
	// timer per call on the depwait hot path.
	var graceTimer *time.Timer
	var graceCh <-chan time.Time
	// quiesced flips true the first time the embedder's quiesce
	// channel fires — see the quiesce case below. Once set, any
	// later Failed observation short-circuits the grace; armGrace
	// also checks it so a Failed already pending when quiesce
	// arrives doesn't re-arm a useless grace.
	quiesced := false
	armGrace := func() {
		if quiesced || graceTimer != nil {
			return
		}
		graceTimer = time.NewTimer(FailedGrace)
		graceCh = graceTimer.C
	}
	defer func() {
		if graceTimer != nil {
			graceTimer.Stop()
		}
	}()
	if currentFailed != nil {
		armGrace()
	}

	failNow := func() (StatusInfo, error) {
		return *currentFailed, &manifest.ResourceFailedError{
			Resource: id.String(), Reason: currentFailed.Message,
		}
	}

	for {
		select {
		case <-wake:
			info, ok := s.GetStatus(id)
			if !ok {
				continue
			}
			switch info.Status {
			case StatusReady:
				return info, nil
			case StatusFailed:
				f := info
				currentFailed = &f
				// If the embedder already signalled quiescence, no
				// re-emit is coming — return the Failed immediately
				// instead of arming a wasted grace window. This is
				// the dominant termination path for typo'd dependsOn:
				// quiesce fires when the orchestrator's task pool
				// drains, then the parent KS's status flips to
				// Failed, and we land here.
				if quiesced {
					return failNow()
				}
				armGrace()
			}
		case <-graceCh:
			return failNow()
		case <-quiesce:
			// Embedder signalled "no future reconcile can re-emit
			// this dep". Two cases:
			//   - Failed already observed: short-circuit the grace
			//     immediately (no point waiting for a re-emit that
			//     can't happen).
			//   - Failed not yet observed: record the quiesced state
			//     so the next Failed transition skips the grace, and
			//     nil out the channel so the closed receive doesn't
			//     busy-spin.
			if currentFailed != nil {
				return failNow()
			}
			quiesced = true
			quiesce = nil
		case <-ctx.Done():
			// Propagate ctx.Err() ONLY when the caller explicitly
			// cancelled (context.Canceled) — otherwise (per-dep
			// Timeout expired with a Failed observed), preserve the
			// established "surface ResourceFailedError so dep
			// summaries carry the real reason" semantic. Without
			// this distinction, clean Ctrl-C on a depwait stuck on a
			// genuinely-failed dep silently masqueraded the
			// shutdown as a dep-failure downstream.
			if errors.Is(ctx.Err(), context.Canceled) {
				info := StatusInfo{}
				if currentFailed != nil {
					info = *currentFailed
				}
				return info, ctx.Err()
			}
			if currentFailed != nil {
				return failNow()
			}
			return StatusInfo{}, ctx.Err()
		}
	}
}

// WatchExists blocks until id is present in the store, then returns it.
// Useful for kinds outside SupportsStatus (ConfigMap, Secret).
//
// Like WatchReady, the listener-register-and-initial-read pair runs
// under rLockAll so a writer that lands after we subscribed always
// reaches our listener, and a writer that landed before we subscribed
// is observed by the initial read. Without this pairing the listener
// can be added after the writer's listener-set snapshot AND the
// initial read can see the pre-write state — wedging until ctx
// timeout.
func (s *Store) WatchExists(ctx context.Context, id manifest.NamedResource) (manifest.BaseManifest, error) {
	if obj := s.GetObject(id); obj != nil {
		return obj, nil
	}

	ch := make(chan manifest.BaseManifest, 1)
	listener := func(other manifest.NamedResource, payload any) {
		if other != id {
			return
		}
		obj, ok := payload.(manifest.BaseManifest)
		if !ok {
			return
		}
		select {
		case ch <- obj:
		default:
		}
	}
	obj, unsub := s.subscribeWithObject(listener, id)
	defer unsub()
	if obj != nil {
		return obj, nil
	}

	select {
	case obj := <-ch:
		return obj, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// subscribeWithObject atomically registers fn as an EventObjectAdded
// listener AND reads the current object for id under one rLockAll
// acquisition. The atomicity closes the subscribe-then-recheck race
// at the source: any writer landing after this call sees our
// listener in its dispatch snapshot; any writer that completed
// before this call has its object visible via the initial read.
//
// rLockAll is required (not just the target shard's RLock) because
// AddListener's no-flush path takes every shard's RLock — using a
// single-shard RLock here would leave a window where a writer on
// shard X can fire AFTER our set.add but BEFORE we read sh.objects,
// missing both the listener delivery and the recheck. Holding every
// shard's RLock matches the AddListener invariant and only costs
// what AddListener already pays.
func (s *Store) subscribeWithObject(fn Listener, id manifest.NamedResource) (manifest.BaseManifest, Unsubscribe) {
	set := s.listeners[EventObjectAdded]
	sh := s.shardFor(id)
	s.rLockAll()
	handle := set.add(fn)
	obj := sh.objects[id]
	s.rUnlockAll()
	return obj, func() { set.remove(handle) }
}

// subscribeWithStatus atomically registers fn as an
// EventStatusUpdated listener AND reads the current status for id
// under one rLockAll. Mirrors subscribeWithObject for the status
// channel; same race-closing argument and same per-shard rationale.
func (s *Store) subscribeWithStatus(fn Listener, id manifest.NamedResource) (StatusInfo, bool, Unsubscribe) {
	set := s.listeners[EventStatusUpdated]
	sh := s.shardFor(id)
	s.rLockAll()
	handle := set.add(fn)
	info, ok := statusInfoFromConditions(sh.conditions[id])
	s.rUnlockAll()
	return info, ok, func() { set.remove(handle) }
}
