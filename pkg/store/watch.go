package store

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
)

// WatchReady blocks until the resource reaches Ready or Failed status,
// then returns the StatusInfo. If the status is Failed it also returns
// a *manifest.ResourceFailedError. Cancellation via ctx returns
// ctx.Err().
//
// If the resource is already in a terminal status when called, the
// function returns immediately.
func (s *Store) WatchReady(ctx context.Context, id manifest.NamedResource) (StatusInfo, error) {
	// Short-circuit on already-terminal status.
	if info, ok := s.GetStatus(id); ok {
		switch info.Status {
		case StatusReady:
			return info, nil
		case StatusFailed:
			return info, &manifest.ResourceFailedError{Resource: id.String(), Reason: info.Message}
		}
	}

	// Subscribe and then re-check (avoids the gap between GetStatus and
	// AddListener).
	ch := make(chan StatusInfo, 1)
	unsub := s.AddListener(EventStatusUpdated, func(other manifest.NamedResource, payload any) {
		if other != id {
			return
		}
		info, ok := payload.(StatusInfo)
		if !ok {
			return
		}
		if info.Status == StatusReady || info.Status == StatusFailed {
			select {
			case ch <- info:
			default:
			}
		}
	}, false)
	defer unsub()

	// Re-check after subscribing to close the race window.
	if info, ok := s.GetStatus(id); ok {
		if info.Status == StatusReady {
			return info, nil
		}
		if info.Status == StatusFailed {
			return info, &manifest.ResourceFailedError{Resource: id.String(), Reason: info.Message}
		}
	}

	select {
	case info := <-ch:
		if info.Status == StatusFailed {
			return info, &manifest.ResourceFailedError{Resource: id.String(), Reason: info.Message}
		}
		return info, nil
	case <-ctx.Done():
		return StatusInfo{}, ctx.Err()
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

// AddedEvent is the value yielded by WatchAdded.
type AddedEvent struct {
	ID     manifest.NamedResource
	Object manifest.BaseManifest
}

// WatchAdded returns a channel that yields every object of the given
// kind, starting with everything already in the store. The channel is
// closed when ctx is cancelled. The channel has buffer size bufSize;
// values are dropped if the consumer falls behind to keep dispatch
// non-blocking. Use bufSize generously for slow consumers.
func (s *Store) WatchAdded(ctx context.Context, kind string, bufSize int) <-chan AddedEvent {
	if bufSize < 1 {
		bufSize = 16
	}
	out := make(chan AddedEvent, bufSize)

	go func() {
		defer close(out)

		// Seed with existing objects.
		for _, obj := range s.ListObjects(kind) {
			id := obj.Named()
			select {
			case out <- AddedEvent{ID: id, Object: obj}:
			case <-ctx.Done():
				return
			}
		}

		// Subscribe for new objects.
		seen := make(map[manifest.NamedResource]struct{})
		for _, obj := range s.ListObjects(kind) {
			seen[obj.Named()] = struct{}{}
		}

		unsub := s.AddListener(EventObjectAdded, func(other manifest.NamedResource, payload any) {
			if kind != "" && other.Kind != kind {
				return
			}
			obj, ok := payload.(manifest.BaseManifest)
			if !ok {
				return
			}
			select {
			case out <- AddedEvent{ID: other, Object: obj}:
			default:
				// Consumer too slow — drop. Could optionally log here.
			}
		}, false)
		defer unsub()

		<-ctx.Done()
	}()

	return out
}

// String formatter is occasionally useful for tests.
func (s *Store) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Store{objects:%d, conditions:%d, artifacts:%d}",
		len(s.objects), len(s.conditions), len(s.artifacts))
}
