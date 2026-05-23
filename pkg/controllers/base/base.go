// Package base provides the shared lifecycle harness every flate
// controller wraps around its per-resource reconcile body.
//
// Each concrete controller (source, kustomization, helmrelease)
// embeds *base.Controller and contributes only the controller-specific
// dependencies (Fetchers, Helm client, Staging cache, ...) plus the
// reconcile function itself. Lifecycle wiring — the started gate, the
// unsubscriber slice, the per-id coalescer, the change filter, the
// Suspend/Filter pre-gate — lives here exactly once.
//
// The package also owns the panic-recovery + status-transition harness
// (Recover, RunWithStatus) that surrounds individual reconcile bodies.
package base

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Controller is the embeddable lifecycle harness. Construct via New,
// install reconcile-shaping config via SetFilter (panics if called
// after Start), then Start to register listeners.
//
// All three concrete controllers carry the same lifecycle shape:
//   - a started gate so Configure-after-Start is a hard error
//   - a coalescer so duplicate AddObject events don't double-reconcile
//   - a filter for changed-only mode
//   - an unsubscriber list cleared by Close
//
// Encoding it once means new pre-reconcile concerns (rate-limit,
// retries, debug-mode toggle) drop into one place and propagate.
type Controller struct {
	Store *store.Store
	Tasks *task.Service

	started atomic.Bool
	unsub   []store.Unsubscribe
	coal    *task.Coalescer[manifest.NamedResource]
	filter  *change.Filter

	// kindLabel prefixes coalescer keys ("source/", "kustomization/",
	// "helmrelease/") and labels panic logs. Set by Start.
	kindLabel string
}

// New constructs a base controller. Concrete controllers call this
// from their own constructor and embed the result.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{Store: s, Tasks: t}
}

// SetFilter installs the change filter that gates reconciliation in
// changed-only mode. Panics if called after Start — the invariant is
// that reconcile-shaping config is frozen once dispatch begins.
func (c *Controller) SetFilter(f *change.Filter) {
	if c.started.Load() {
		panic("controller: SetFilter called after Start")
	}
	c.filter = f
}

// Filter returns the configured filter (may be nil-but-non-active).
func (c *Controller) Filter() *change.Filter { return c.filter }

// StartLifecycle flips the started gate and allocates the coalescer.
// Concrete controllers call this from their Start(ctx) before
// installing listeners via AddListener.
func (c *Controller) StartLifecycle(kindLabel string) {
	c.kindLabel = kindLabel
	c.started.Store(true)
	c.coal = task.NewCoalescer[manifest.NamedResource](c.Tasks)
}

// AddListener registers a store listener and records it so Close can
// unsubscribe. snapshot=true matches every concrete controller's needs
// (deliver the current store state on subscribe).
func (c *Controller) AddListener(event store.EventKind, l store.Listener) {
	c.unsub = append(c.unsub, c.Store.AddListener(event, l, true))
}

// Close removes every registered listener. Idempotent.
func (c *Controller) Close() {
	for _, u := range c.unsub {
		u()
	}
	c.unsub = nil
}

// Submit enqueues a reconcile body keyed by id. Duplicate submits with
// the same id coalesce so a re-emit by a parent KS doesn't double the
// work. Caller-supplied fn runs with panic-recover already installed.
func (c *Controller) Submit(ctx context.Context, id manifest.NamedResource, fn func(context.Context)) {
	c.coal.Submit(ctx, c.kindLabel+"/"+id.String(), id, fn)
}

// PreGate is the canonical Suspend/Filter pre-reconcile check.
// Returns true when the resource is gated out — caller MUST bail.
//
//   - suspended → marks Ready "suspended", returns true
//   - filter excludes the id → marks Ready "unchanged", returns true
//   - otherwise → returns false, caller proceeds to Submit/reconcile
func (c *Controller) PreGate(id manifest.NamedResource, suspended bool) bool {
	if suspended {
		c.Store.UpdateStatus(id, store.StatusReady, "suspended")
		return true
	}
	if c.filter.Enabled() && !c.filter.ShouldReconcile(id) {
		c.Store.UpdateStatus(id, store.StatusReady, "unchanged")
		return true
	}
	return false
}

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
		// ErrSourceSkipped propagates a soft-skip from a referenced
		// source (e.g. --allow-missing-secrets on its auth secret) up
		// to this consumer. Mark Ready with a "skipped:" message
		// rather than Failed so depwait treats us as ready and the
		// test runner reports SKIPPED, matching the source's outcome.
		if errors.Is(err, manifest.ErrSourceSkipped) {
			s.UpdateStatus(id, store.StatusReady,
				store.SkippedPrefix+" "+manifest.TrimSentinelPrefix(err.Error()))
			return
		}
		s.UpdateStatus(id, store.StatusFailed, err.Error())
		return
	}
	s.UpdateStatus(id, store.StatusReady, "")
}
