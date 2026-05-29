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
	"sync"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
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
//
// The KS and HR controllers additionally share depwait configuration
// (Existence, Renders) and a pre-reconcile preflight check (Preflight,
// ParentOf). Configure these via SetDepwait / SetPreflight / SetParentOf
// before Start so reconcile bodies can call NewWaiter, PreflightError,
// and LookupParent without each controller duplicating the nil-check
// boilerplate.
type Controller struct {
	Store *store.Store
	Tasks *task.Service

	started atomic.Bool
	coal    *task.Coalescer[manifest.NamedResource]
	filter  *change.Filter

	// closed is set by Close so any later AddListener short-circuits
	// rather than appending into a slice nobody will drain. The flag
	// is checked once on the fast path (no lock) and re-checked under
	// unsubMu before the append so a Close racing AddListener cannot
	// snapshot c.unsub and miss a registration landing just after.
	// Matches the started lifecycle-gate pattern above.
	closed atomic.Bool

	// unsubMu guards unsub so AddListener and Close can be called
	// concurrently (e.g. shutdown racing a listener-triggered Submit).
	// The slice is short-lived (registered at Start, drained at Close)
	// and per-controller, so the lock has near-zero contention.
	unsubMu sync.Mutex
	unsub   []store.Unsubscribe

	// kindLabel prefixes coalescer keys ("source/", "kustomization/",
	// "helmrelease/") and labels panic logs. Set by Start.
	kindLabel string

	// Shared KS/HR depwait and preflight state. Set via SetDepwait,
	// SetPreflight, SetParentOf. The source controller leaves these nil;
	// KS and HR configure them before Start via their Configure methods.
	existence depwait.ExistenceLookup
	renders   depwait.RenderInflight
	preflight func(manifest.NamedResource) (string, bool)
	parentOf  func(manifest.NamedResource) (manifest.NamedResource, bool)
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

// SetDepwait installs the depwait resolution wires. Panics after Start.
func (c *Controller) SetDepwait(existence depwait.ExistenceLookup, renders depwait.RenderInflight) {
	if c.started.Load() {
		panic("controller: SetDepwait called after Start")
	}
	c.existence = existence
	c.renders = renders
}

// SetPreflight installs the pre-reconcile failure reporter. Panics after Start.
func (c *Controller) SetPreflight(f func(manifest.NamedResource) (string, bool)) {
	if c.started.Load() {
		panic("controller: SetPreflight called after Start")
	}
	c.preflight = f
}

// SetParentOf installs the structural parent resolver. Panics after Start.
func (c *Controller) SetParentOf(f func(manifest.NamedResource) (manifest.NamedResource, bool)) {
	if c.started.Load() {
		panic("controller: SetParentOf called after Start")
	}
	c.parentOf = f
}

// LookupParent reports the structural parent KS of id via the
// configured resolver, or (zero, false) when no parent exists or no
// resolver was configured.
func (c *Controller) LookupParent(id manifest.NamedResource) (manifest.NamedResource, bool) {
	if c.parentOf == nil {
		return manifest.NamedResource{}, false
	}
	return c.parentOf(id)
}

// PreflightFailure reports the pre-reconcile failure for id if the
// orchestrator detected a dependency-graph error. Returns ("", false)
// when no preflight check is configured or no failure was recorded.
func (c *Controller) PreflightFailure(id manifest.NamedResource) (string, bool) {
	if c.preflight == nil {
		return "", false
	}
	return c.preflight(id)
}

// PreflightError returns an error wrapping the preflight failure
// message for id, or nil when no failure is recorded. Used at each
// yield point inside reconcile so a cycle detection or topology error
// published mid-flight aborts the current pass without waiting.
func (c *Controller) PreflightError(id manifest.NamedResource) error {
	if msg, failed := c.PreflightFailure(id); failed {
		return errors.New(msg)
	}
	return nil
}

// NewWaiter constructs a depwait.Waiter pre-wired with the
// controller's Store, Existence lookup, and Renders quiescence signal,
// parented to id and budgeted from timeout. HR and KS controllers call
// this rather than constructing their own Waiter literals so the
// Existence/Renders wiring is set once in Configure and flows through
// automatically.
func (c *Controller) NewWaiter(id manifest.NamedResource, timeout *metav1.Duration) *depwait.Waiter {
	return &depwait.Waiter{
		Store:     c.Store,
		Parent:    id,
		Timeout:   depwait.TimeoutFromSpec(timeout),
		Existence: c.existence,
		Renders:   c.renders,
	}
}

// IsFileIndexed reports whether id is tracked by the file-existence
// index wired at Configure time. Returns false when no index is
// configured (offline / unit-test paths), which degrades safely by
// treating the resource as not-file-indexed.
func (c *Controller) IsFileIndexed(id manifest.NamedResource) bool {
	return c.existence != nil && c.existence.IsFileIndexed(id)
}

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
// (deliver the current store state on subscribe). Safe to call
// concurrently with Close — once Close has flipped the closed gate,
// any in-flight or later AddListener is refused and the registration
// is rolled back so the underlying store does not retain a listener
// the controller will never unsubscribe.
func (c *Controller) AddListener(event store.EventKind, l store.Listener) {
	// Fast path: avoid the store registration entirely when Close has
	// already started. The post-lock re-check below catches the TOCTOU
	// window where Close flips closed between this load and the lock.
	if c.closed.Load() {
		return
	}
	u := c.Store.AddListener(event, l, true)
	c.unsubMu.Lock()
	if c.closed.Load() {
		// Close set the flag and drained c.unsub between our fast-path
		// check and the lock — releasing the store registration here
		// matches what Close would have done with our entry.
		c.unsubMu.Unlock()
		u()
		return
	}
	c.unsub = append(c.unsub, u)
	c.unsubMu.Unlock()
}

// Close removes every registered listener and refuses any later
// AddListener so a late call from a shutdown-racing goroutine cannot
// leak a registration past us. Idempotent: a second Close is a no-op
// because the closed flag is set via Swap.
func (c *Controller) Close() {
	c.unsubMu.Lock()
	if c.closed.Swap(true) {
		// Another Close already drained and marked us closed.
		c.unsubMu.Unlock()
		return
	}
	unsubs := c.unsub
	c.unsub = nil
	c.unsubMu.Unlock()
	for _, u := range unsubs {
		u()
	}
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
		c.Store.UpdateStatus(id, store.StatusReady, store.MsgSuspended)
		return true
	}
	if c.filterActive() && !c.filter.ShouldReconcile(id) {
		c.Store.UpdateStatus(id, store.StatusReady, store.MsgUnchanged)
		return true
	}
	return false
}

// filterActive reports whether a non-nil, enabled change filter is
// configured. Replaces the previous c.filter.Enabled() call that
// relied on Filter.Enabled being nil-safe — making every future
// non-pointer-deref method on *Filter latently NPE-prone. Routing
// every read through this helper means a future method addition on
// *Filter doesn't crash PreGate.
func (c *Controller) filterActive() bool {
	return c.filter != nil && c.filter.Enabled()
}

// Await blocks until each dep in deps reaches Ready, yielding the
// caller's worker-pool slot during the wait so children depended on
// can themselves acquire a slot and make progress. Centralizes the
// "set pending → yield → WaitAll → check failed" dance the three
// concrete controllers each implemented inline; the per-call-site
// difference (which error sentinel wraps a failed summary) is
// expressed via onFail.
//
// pendingMsg is the StatusPending message written before the wait —
// surfaces in `flate test` reporting and the orchestrator's failure
// rollup. Pass an empty string to skip the status write (e.g. when
// the caller already set its own).
//
// onFail receives the depwait Summary on any AnyFailed and returns
// the error the caller propagates. Use it to pick between
// manifest.DependencyFailedError, ErrObjectNotFound, etc. — the
// concrete controllers each have their own conventions.
func (c *Controller) Await(
	ctx context.Context,
	id manifest.NamedResource,
	w *depwait.Waiter,
	deps []manifest.DependencyRef,
	pendingMsg string,
	onFail func(depwait.Summary) error,
) error {
	if pendingMsg != "" {
		c.Store.UpdateStatus(id, store.StatusPending, pendingMsg)
	}
	var sum depwait.Summary
	runWait := func() {
		sum = depwait.WaitAll(w.Watch(ctx, deps))
	}
	if w.ReadyNow(deps) {
		runWait()
	} else {
		// YieldQuiescent (not YieldSlot): the wait is on OTHER tasks'
		// work, so this task isn't producing anything while parked.
		// Decrementing active lets QuiescenceCh fire on a render-only
		// dep the moment no productive task remains. The ReadyNow fast
		// path above keeps an immediately-unblocked producer counted as
		// active so consumers do not observe a false drained pool.
		c.Tasks.YieldQuiescent(runWait)
	}
	if sum.AnyFailed() {
		return onFail(sum)
	}
	return nil
}

// Recover catches a panic from the current goroutine and marks id
// StatusFailed with a "panic: <r>" message so the orchestrator
// surfaces it. Intended for use as `defer base.Recover(store, id, "kind")`
// in controllers that don't go through RunWithStatus (e.g. source
// fetchers that own their own status writes).
//
// After recording status, re-raises the panic so the enclosing
// task.Service.Go increments its failures counter — a panicked
// reconcile MUST count against the orchestrator's failure gate, not
// silently masquerade as success when Service.Failures() is consulted
// for the final exit-code decision.
func Recover(s *store.Store, id manifest.NamedResource, logKind string) {
	r := recover()
	if r == nil {
		return
	}
	slog.Error(logKind+": panic during reconcile", "id", id.String(), "panic", r)
	s.UpdateStatus(id, store.StatusFailed, fmt.Sprintf("panic: %v", r))
	panic(r)
}

// RunWithStatus is the standard reconcile body for controllers that
// (a) coalesce concurrent submits per-id and (b) want the recover →
// re-read → run → mark-Ready/Failed pattern. The re-read lets a
// coalesced re-run pick up patches a parent KS installed mid-flight
// rather than the stale payload from the original event. A missing
// object (deleted between coalescer enqueue and run) is treated as a
// no-op rather than a failure.
//
// On success the terminal status write is conditional: if the
// current status already carries an informative Ready message (a
// soft-skip from --allow-missing-secrets, an MsgUnchanged from the
// change filter, an MsgSuspended from PreGate), the empty-message
// overwrite is suppressed so the informative message survives a
// short-circuited coalesced re-run that returns nil. Plain Ready
// (no message) and any non-Ready status get the standard "" Ready
// write so the controller's terminal contract is preserved.
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
		// Object deleted (or wrong type) between coalescer enqueue and
		// run. A Refire-driven re-run that previously wrote
		// StatusPending/MsgRefetching would otherwise stick at Pending
		// forever — every depwait blocking on this id rides its full
		// per-dep timeout. Write a terminal Ready with a brief reason
		// so dep checks unblock and the testrunner reports cleanly.
		// Use Ready (not Failed) because a vanished resource is the
		// same outcome real Flux would see when the CR is deleted.
		if info, has := s.GetStatus(id); has && info.Status != store.StatusReady {
			s.UpdateStatus(id, store.StatusReady, "skipped: object not in store at reconcile time")
		}
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
	if existing, ok := s.GetStatus(id); ok {
		if existing.Status == store.StatusFailed {
			return
		}
		if existing.Status == store.StatusReady && existing.Message != "" {
			// Existing Ready message is informative (skipped:, unchanged,
			// suspended, or any future Ready sub-state) — don't clobber.
			return
		}
	}
	s.UpdateStatus(id, store.StatusReady, "")
}
