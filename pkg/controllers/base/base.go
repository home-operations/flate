// Package base provides the small shared harness that the
// kustomization, helmrelease, and source controllers wrap around their
// per-resource reconcile bodies. It centralizes panic attribution
// (so a panicked reconcile marks the affected resource Failed instead
// of silently disappearing) and the post-reconcile status transition.
package base

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Recover catches a panic from the current goroutine and marks id
// StatusFailed with a "panic: <r>" message so the orchestrator
// surfaces it. Intended for use as `defer base.Recover(store, id, "kind")`
// in controllers that don't go through RunWithStatus (e.g. source
// fetchers that own their own status writes).
func Recover(s *store.Store, id manifest.NamedResource, logKind string) {
	if r := recover(); r != nil {
		slog.Error(logKind+": panic during reconcile", "id", id.String(), "panic", r)
		s.UpdateStatus(id, store.StatusFailed, fmt.Sprintf("panic: %v", r))
	}
}

// RunWithStatus is the standard reconcile body for controllers that
// (a) coalesce concurrent submits per-id and (b) want the recover →
// re-read → run → mark-Ready/Failed pattern. The re-read lets a
// coalesced re-run pick up patches a parent KS installed mid-flight
// rather than the stale payload from the original event. A missing
// object (deleted between coalescer enqueue and run) is treated as a
// no-op rather than a failure.
func RunWithStatus[T manifest.BaseManifest](
	ctx context.Context,
	s *store.Store,
	id manifest.NamedResource,
	logKind string,
	fn func(context.Context, T) error,
) {
	defer Recover(s, id, logKind)
	obj, ok := s.GetObject(id).(T)
	if !ok {
		return
	}
	if err := fn(ctx, obj); err != nil {
		s.UpdateStatus(id, store.StatusFailed, err.Error())
		return
	}
	s.UpdateStatus(id, store.StatusReady, "")
}
