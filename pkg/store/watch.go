package store

import (
	"context"
	"fmt"
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
	wake := make(chan struct{}, 1)
	unsub := s.AddListener(EventStatusUpdated, func(other manifest.NamedResource, _ any) {
		if other != id {
			return
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}, false)
	defer unsub()

	// Re-check after subscribing to close the race window.
	var currentFailed *StatusInfo
	if info, ok := s.GetStatus(id); ok {
		if info.Status == StatusReady {
			return info, nil
		}
		if info.Status == StatusFailed {
			f := info
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
func (s *Store) WatchExists(ctx context.Context, id manifest.NamedResource) (manifest.BaseManifest, error) {
	if obj := s.GetObject(id); obj != nil {
		return obj, nil
	}

	ch := make(chan manifest.BaseManifest, 1)
	unsub := s.AddListener(EventObjectAdded, func(other manifest.NamedResource, payload any) {
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
	}, false)
	defer unsub()

	if obj := s.GetObject(id); obj != nil {
		return obj, nil
	}

	select {
	case obj := <-ch:
		return obj, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// String formatter is occasionally useful for tests.
func (s *Store) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Store{objects:%d, conditions:%d, artifacts:%d}",
		len(s.objects), len(s.conditions), len(s.artifacts))
}
