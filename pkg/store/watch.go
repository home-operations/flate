package store

import (
	"context"
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
func (s *Store) WatchReady(ctx context.Context, id manifest.NamedResource) (StatusInfo, error) {
	// Short-circuit on Ready — no transition to wait for.
	if info, ok := s.GetStatus(id); ok && info.Status == StatusReady {
		return info, nil
	}

	// Subscribe with a wake-only signal — the actual status is read
	// back from the store on every wake. Carrying StatusInfo through
	// the channel directly would lose Failed→Ready transitions when
	// the buffer-1 channel drops on a default-send.
	//
	// The subscribe + initial status read run under s.mu.RLock to
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
	initial, hasInitial, unsub := s.subscribeWithStatus(EventStatusUpdated, listener, id)
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

	// graceCh fires when the post-Failed grace window expires. We
	// arm it only once a Failed has been observed.
	var graceCh <-chan time.Time
	if currentFailed != nil {
		graceCh = time.After(FailedGrace)
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
				if graceCh == nil {
					graceCh = time.After(FailedGrace)
				}
			}
		case <-graceCh:
			return *currentFailed, &manifest.ResourceFailedError{
				Resource: id.String(), Reason: currentFailed.Message,
			}
		case <-ctx.Done():
			if currentFailed != nil {
				return *currentFailed, &manifest.ResourceFailedError{
					Resource: id.String(), Reason: currentFailed.Message,
				}
			}
			return StatusInfo{}, ctx.Err()
		}
	}
}

// WatchExists blocks until id is present in the store, then returns it.
// Useful for kinds outside SupportsStatus (ConfigMap, Secret).
//
// Like WatchReady, the listener-register-and-initial-read pair runs
// under s.mu.RLock so a writer that lands after we subscribed always
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
// listener AND reads the current object for id under one s.mu.RLock
// acquisition. The atomicity closes the subscribe-then-recheck race
// at the source: any writer landing after this call sees our
// listener in its dispatch snapshot; any writer that completed
// before this call has its object visible via the initial read.
func (s *Store) subscribeWithObject(fn Listener, id manifest.NamedResource) (manifest.BaseManifest, Unsubscribe) {
	set := s.listeners[EventObjectAdded]
	s.mu.RLock()
	handle := set.add(fn)
	obj := s.objects[id]
	s.mu.RUnlock()
	return obj, func() { set.remove(handle) }
}

// subscribeWithStatus atomically registers fn as an
// EventStatusUpdated listener AND reads the current status for id
// under one s.mu.RLock. Mirrors subscribeWithObject for the status
// channel; same race-closing argument.
func (s *Store) subscribeWithStatus(_ EventKind, fn Listener, id manifest.NamedResource) (StatusInfo, bool, Unsubscribe) {
	set := s.listeners[EventStatusUpdated]
	s.mu.RLock()
	handle := set.add(fn)
	info, ok := statusInfoFromConditions(s.conditions[id])
	s.mu.RUnlock()
	return info, ok, func() { set.remove(handle) }
}

