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
	"slices"
	"sync"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// RenderTracker is the seam a controller uses to report
// "this child id was emitted by THIS parent's render" to the
// orchestrator. Nil is OK — the controller no-ops.
//
// The parent linkage feeds detectOrphans, the structural-parent
// resolver, and ResourceSet extension attribution for render-emitted
// resources. Both the KS and HR controllers report through it
// identically; MarkRenderedBatch records multiple children under a
// single lock acquisition so a render emitting N children pays one
// tracker round-trip rather than N.
type RenderTracker interface {
	MarkRenderedBatch(parent manifest.NamedResource, children []manifest.NamedResource)
}

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
	// "helmrelease/"). Set by StartLifecycle.
	kindLabel string

	// Shared KS/HR depwait and preflight state. Set via SetDepwait,
	// SetPreflight, SetParentOf. The source controller leaves these nil;
	// KS and HR configure them before Start via their Configure methods.
	existence depwait.ExistenceLookup
	renders   depwait.RenderInflight
	preflight func(manifest.NamedResource) (string, bool)
	parentOf  func(manifest.NamedResource) (manifest.NamedResource, bool)

	// engine selects the dependency-gating strategy Require uses. The zero
	// value (EngineEvent) is the blocking event engine — today's default; the
	// orchestrator flips it to EngineDAG via SetEngine when --engine=dag.
	engine EngineMode

	// renderTracker receives every reconcilable/source child a render
	// emits. Set via SetRenderTracker before Start; read-only after.
	// nil is fine — ReportRendered no-ops.
	renderTracker RenderTracker
}

// New constructs a base controller. Concrete controllers call this
// from their own constructor and embed the result.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{Store: s, Tasks: t}
}

// requireNotStarted panics if the started gate is set, enforcing the
// invariant that reconcile-shaping config is frozen once dispatch
// begins. method is the calling setter's name for the panic message.
func (c *Controller) requireNotStarted(method string) {
	if c.started.Load() {
		panic("controller: " + method + " called after Start")
	}
}

// SetFilter installs the change filter that gates reconciliation in
// changed-only mode. Panics if called after Start — the invariant is
// that reconcile-shaping config is frozen once dispatch begins.
func (c *Controller) SetFilter(f *change.Filter) {
	c.requireNotStarted("SetFilter")
	c.filter = f
}

// Filter returns the configured filter (may be nil-but-non-active).
func (c *Controller) Filter() *change.Filter { return c.filter }

// SetDepwait installs the depwait resolution wires. Panics after Start.
func (c *Controller) SetDepwait(existence depwait.ExistenceLookup, renders depwait.RenderInflight) {
	c.requireNotStarted("SetDepwait")
	c.existence = existence
	c.renders = renders
}

// SetPreflight installs the pre-reconcile failure reporter. Panics after Start.
func (c *Controller) SetPreflight(f func(manifest.NamedResource) (string, bool)) {
	c.requireNotStarted("SetPreflight")
	c.preflight = f
}

// SetParentOf installs the structural parent resolver. Panics after Start.
func (c *Controller) SetParentOf(f func(manifest.NamedResource) (manifest.NamedResource, bool)) {
	c.requireNotStarted("SetParentOf")
	c.parentOf = f
}

// SetRenderTracker installs the render-emission tracker. Panics after
// Start — reconcile-shaping config is frozen once dispatch begins.
func (c *Controller) SetRenderTracker(rt RenderTracker) {
	c.requireNotStarted("SetRenderTracker")
	c.renderTracker = rt
}

// KeepEmitted extends the change filter's keep set so render-emitted
// children pass the changed-only-mode PreGate check. Without this, a
// parent whose render emits a child that wasn't on disk at filter-build
// time (kustomize component+replacement KSes, charts that render source
// CRs) would silently drop that child from the diff comparison. Routed
// through Filter.AddEmitted so an ancestor-only parent doesn't cascade
// unrelated file-loaded children into keep (#204/#260/#308).
//
// MUST be called BEFORE Store.AddObject so the listener that fires
// synchronously during AddObject sees the extended keep set.
func (c *Controller) KeepEmitted(parent manifest.NamedResource, child manifest.BaseManifest) {
	if f := c.Filter(); f != nil {
		f.AddEmitted(parent, child)
	}
}

// ReportRendered reports parent→child render emissions to the
// configured RenderTracker; no-op when none is wired or there are no
// children. The emit loop accumulates every child it emits and flushes
// through this helper exactly once, holding the tracker's lock for one
// acquisition instead of N.
func (c *Controller) ReportRendered(parent manifest.NamedResource, children []manifest.NamedResource) {
	if c.renderTracker == nil || len(children) == 0 {
		return
	}
	c.renderTracker.MarkRenderedBatch(parent, children)
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

// SetPendingUnlessReady writes a StatusPending progress message for id,
// UNLESS id is already StatusReady. A no-op re-reconcile (a parent render
// re-emitting the object with stamped ownership labels, a coalesced re-run)
// must not transiently downgrade Ready→Pending: a dependent's quiescence-bound
// depwait can re-read that transient Pending at a transient task-pool drain and
// give up ("not ready"), dropping the dependent nondeterministically.
//
// Use for the progress writes that PRECEDE a reconcile's no-op (fingerprint /
// artifact) short-circuit. The genuine-work downgrade AFTER that check should
// stay an unconditional UpdateStatus so a real re-render re-gates dependents.
// Re-reading status per call is equivalent to capturing it once at reconcile
// entry: the coalescer serializes per-id, so nothing mutates an id's own status
// mid-reconcile. See #657 (kustomization) / #658 (source).
func (c *Controller) SetPendingUnlessReady(id manifest.NamedResource, msg string) {
	if info, ok := c.Store.GetStatus(id); ok && info.Status == store.StatusReady {
		return
	}
	c.Store.UpdateStatus(id, store.StatusPending, msg)
}

// FingerprintDedup short-circuits a reconcile when id's cached rendered
// artifact carries a non-empty fingerprint equal to fp — the effective inputs
// are byte-identical, so the expensive render (kustomize/helm) is skipped. It
// still replays the cached docs through emit so the idempotent per-emission
// side-effects (keep-set extension + parent provenance) fire on every reconcile
// pass; emit is the controller's emitRenderedChildren(id, docs, publish=false)
// closure. The replay deliberately does NOT re-publish the children: they were
// already published byte-identically by the render that set this artifact, so
// re-AddObject-ing them would only re-fire listeners and re-submit already-
// settled resources — churn that can transiently un-Ready a parent and race
// quiescence (the "not ready" non-determinism, see #657–#660).
//
// Returns (handled=true, err): the caller returns err. err is non-nil only when
// a preflight error was discovered mid-flight. Returns (false, nil) to render
// normally. Centralizes the byte-identical KS/HR dedup short-circuit.
func (c *Controller) FingerprintDedup(id manifest.NamedResource, fp, logKind string, emit func(docs []map[string]any)) (bool, error) {
	existing, ok := c.Store.GetArtifact(id).(store.RenderedArtifact)
	if !ok || existing.RenderedFingerprint() == "" || existing.RenderedFingerprint() != fp {
		return false, nil
	}
	if err := c.PreflightError(id); err != nil {
		return true, err
	}
	slog.Debug(logKind+": skipped re-render (fingerprint unchanged)", "id", id.String())
	emit(existing.RenderedManifests())
	return true, nil
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
	switch {
	case w.ReadyNow(deps):
		// Every dep already Ready — resolve immediately, no park.
		runWait()
	case w.AllPresentReconcilable(deps):
		// Every dep is a present, CEL-free Kustomization/HelmRelease — a
		// reconcilable resource whose producer is MULTI-HOP (it parks on its
		// own deps/chart source before writing its terminal status). A wait on
		// such a dep is the transient-drain victim: a far-off source fetch
		// exiting can drop the pool to 0 in the window before the dep's HR
		// re-activates and goes Ready, firing a FALSE quiescence that drops
		// this waiter. YieldSlot (release the worker slot but STAY active)
		// keeps the pool non-zero across that handoff. Safe because a present
		// reconcilable dep always terminalizes (so we resolve on its terminal
		// wake); dependsOn cycles can't hang here — the orchestrator's preflight
		// cycle detector fails their members before they reach this park; and
		// `active` feeds only QuiescenceCh, so holding it has no other effect.
		// Source-kind / ReadyExpr deps stay on the give-up path below.
		c.Tasks.YieldSlot(runWait)
	default:
		// At least one dep is absent / render-only (may never be produced) or
		// carries a ReadyExpr (may never satisfy): keep the quiescence-bound
		// give-up. YieldQuiescent decrements active so QuiescenceCh fires the
		// moment no productive task remains and the wait fast-fails instead of
		// hanging. The ReadyNow fast path above keeps an immediately-unblocked
		// producer counted as active so consumers do not observe a false
		// drained pool.
		c.Tasks.YieldQuiescent(runWait)
	}
	if sum.AnyFailed() {
		return onFail(sum)
	}
	return nil
}

// AwaitRefresh fuses Await with the load-bearing store re-read every
// dependency wait performs on success. A structural parent may have
// re-emitted this object with a mutated spec while it was parked, so the
// caller MUST reconcile against the refreshed object, not the stale
// snapshot it was dispatched with (see #102 and the dependsOn re-read
// comments at the call sites). Wait and re-read were two statements joined
// only by convention; fusing them means a future await site can't call
// Await and silently forget the re-read.
//
// On a wait failure it returns (zero, false, err) — the caller returns the
// error. On success it returns (fresh, true, nil) when the object is still
// in the store, or (zero, false, nil) if it vanished, mirroring the prior
// `if obj, ok := store.Get[T](...); ok { x = obj }` guard (the caller keeps
// the object it already held).
func AwaitRefresh[T manifest.BaseManifest](
	ctx context.Context,
	c *Controller,
	id manifest.NamedResource,
	w *depwait.Waiter,
	deps []manifest.DependencyRef,
	pendingMsg string,
	onFail func(depwait.Summary) error,
) (T, bool, error) {
	if err := c.Await(ctx, id, w, deps, pendingMsg, onFail); err != nil {
		var zero T
		return zero, false, err
	}
	obj, ok := store.Get[T](c.Store, id)
	return obj, ok, nil
}

// EngineMode selects how Require gates on dependencies.
type EngineMode uint8

const (
	// EngineEvent is the blocking event engine (depwait + task quiescence) —
	// the zero value and current default.
	EngineEvent EngineMode = iota
	// EngineDAG is the re-entrant fixpoint scheduler: Require returns a
	// *depwait.ErrBlocked sentinel instead of blocking, so the scheduler parks
	// the node and re-runs it when a dependency advances.
	EngineDAG
)

// SetEngine selects the dependency-gating engine. Panics after Start —
// reconcile-shaping config is frozen once dispatch begins.
func (c *Controller) SetEngine(m EngineMode) {
	c.requireNotStarted("SetEngine")
	c.engine = m
}

// DAGEngine reports whether the dag scheduler owns dispatch. Concrete
// controllers use it to skip registering their event-driven OnReconcile
// dispatch listener in Start — under dag the scheduler invokes ReconcileNode
// directly. Other listeners (HR's producer index) stay registered.
func (c *Controller) DAGEngine() bool { return c.engine == EngineDAG }

type drainLevelKey struct{}

// WithDrainLevel stamps the dag scheduler's current drain level onto ctx so
// the body's Require calls reach Classify with it. The orchestrator's
// Dispatcher sets it per-dispatch; absent ⇒ DrainNone (0).
func WithDrainLevel(ctx context.Context, level int) context.Context {
	return context.WithValue(ctx, drainLevelKey{}, level)
}

func drainLevel(ctx context.Context) int {
	v, _ := ctx.Value(drainLevelKey{}).(int)
	return v
}

// Require gates the caller on deps. Under the event engine it blocks (Await);
// under the dag engine it classifies each dep WITHOUT blocking and returns
// one of:
//   - nil — every dep satisfied; the body proceeds.
//   - onFail(sum) (a *manifest.DependencyFailedError) — a dep is terminally
//     Failed and none is still blockable; instant cascade, no FailedGrace.
//   - *depwait.ErrBlocked — at least one dep is absent/Pending/ReadyExpr-pending;
//     the body returns and the scheduler parks the node on those deps.
//
// Blocked WINS over failed: if any dep is still producible (ClassBlocked) we
// park even when another dep already failed, discarding sum — the re-run
// re-derives the failure on a later pass once nothing is blocked. This matches
// the event engine, where a producible dep keeps the wait alive while a
// failed-omittable ref is handled by the caller's onFail only once it is the
// sole remaining signal (see resolvePreRenderValuesFrom).
//
// The pendingMsg status write mirrors Await exactly (unconditional Pending when
// non-empty, nothing when empty) so a dependent observing this node mid-gate
// sees the identical status under both engines.
func (c *Controller) Require(
	ctx context.Context,
	id manifest.NamedResource,
	timeout *metav1.Duration,
	deps []manifest.DependencyRef,
	pendingMsg string,
	onFail func(depwait.Summary) error,
) error {
	if c.engine != EngineDAG {
		return c.Await(ctx, id, c.NewWaiter(id, timeout), deps, pendingMsg, onFail)
	}
	if pendingMsg != "" {
		c.Store.UpdateStatus(id, store.StatusPending, pendingMsg)
	}
	level := drainLevel(ctx)
	w := c.NewWaiter(id, timeout)
	var blocked []manifest.NamedResource
	sum := depwait.Summary{Messages: map[manifest.NamedResource]string{}}
	for _, dep := range deps {
		switch cl := w.Classify(dep, level); cl.Kind {
		case depwait.ClassReady:
		case depwait.ClassFailed:
			sum.Failed = append(sum.Failed, dep.NamedResource)
			sum.Messages[dep.NamedResource] = cl.Message
		case depwait.ClassBlocked:
			blocked = append(blocked, dep.NamedResource)
		}
	}
	if len(blocked) > 0 {
		return &depwait.ErrBlocked{Deps: blocked}
	}
	if sum.AnyFailed() {
		return onFail(sum)
	}
	return nil
}

// RequireRefresh fuses Require with the load-bearing store re-read every
// dependency gate performs on success — the Require counterpart of
// AwaitRefresh (see its doc for the #102 re-read rationale). Call sites use
// this instead of AwaitRefresh so the engine seam (Require) is the only
// thing they depend on; the dag engine re-reads identically after a
// satisfied gate.
func RequireRefresh[T manifest.BaseManifest](
	ctx context.Context,
	c *Controller,
	id manifest.NamedResource,
	timeout *metav1.Duration,
	deps []manifest.DependencyRef,
	pendingMsg string,
	onFail func(depwait.Summary) error,
) (T, bool, error) {
	if err := c.Require(ctx, id, timeout, deps, pendingMsg, onFail); err != nil {
		var zero T
		return zero, false, err
	}
	obj, ok := store.Get[T](c.Store, id)
	return obj, ok, nil
}

// DepFailed returns the canonical onFail closure that reports a
// dependency-wait failure as a *manifest.DependencyFailedError for id.
// The identical literal was rebuilt at every dependsOn/pre-render wait;
// single-sourcing it keeps the failure shape consistent across controllers.
func DepFailed(id manifest.NamedResource) func(depwait.Summary) error {
	return func(sum depwait.Summary) error {
		// Sort the failed-dep list so the rendered DependencyFailedError
		// message is deterministic across runs AND identical between the two
		// engines: the event engine's WaitAll collects failures in
		// nondeterministic channel-receive order, while the dag engine collects
		// them in dep-slice order. Sorting here normalizes both (and fixes a
		// latent event-engine nondeterminism for multi-failed-dependency
		// resources).
		slices.SortFunc(sum.Failed, func(a, b manifest.NamedResource) int { return a.Compare(b) })
		return &manifest.DependencyFailedError{
			Parent:  id,
			Failed:  sum.Failed,
			Reasons: sum.Messages,
		}
	}
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
	_ = RunWithStatusOutcome[T](ctx, s, id, logKind, fn)
}

// RunWithStatusOutcome is RunWithStatus that additionally reports the dag
// scheduler's outcome: the blocked dependency set, or nil when the body
// terminalized (Ready / Skipped / Failed). It intercepts a *depwait.ErrBlocked
// returned by the body BEFORE the generic Failed-status write, leaving the
// Pending status the body's Require wrote — so a blocked node stays non-Ready
// and re-runnable. Under the event engine the body never returns ErrBlocked, so
// the returned slice is always nil and behavior is byte-identical to the prior
// RunWithStatus.
func RunWithStatusOutcome[T manifest.BaseManifest](
	ctx context.Context,
	s *store.Store,
	id manifest.NamedResource,
	logKind string,
	fn func(context.Context, T) error,
) []manifest.NamedResource {
	defer Recover(s, id, logKind)
	obj, ok := store.Get[T](s, id)
	if !ok {
		// Object deleted (or wrong type) between discovery/enqueue and run.
		// Write a terminal Ready with a brief reason so dependents unblock and
		// the testrunner reports cleanly — a vanished resource is the same
		// outcome real Flux sees when the CR is deleted.
		if info, has := s.GetStatus(id); has && info.Status != store.StatusReady {
			s.UpdateStatus(id, store.StatusReady, "skipped: object not in store at reconcile time")
		}
		return nil
	}
	if err := fn(ctx, obj); err != nil {
		// dag-only: a dependency gate is unsatisfied. Surface the blocked deps
		// WITHOUT writing Failed — the node keeps the Pending status Require
		// wrote and stays parkable + re-runnable. Provably unreachable under the
		// event engine (Await blocks instead of returning ErrBlocked), so the
		// event path is byte-identical.
		var blocked *depwait.ErrBlocked
		if errors.As(err, &blocked) {
			return blocked.Deps
		}
		// ErrSourceSkipped propagates a soft-skip from a referenced source
		// (--allow-missing-secrets on its auth secret). Mark Ready+"skipped:"
		// rather than Failed so dependents treat us as ready and the runner
		// reports SKIPPED, matching the source's outcome.
		if errors.Is(err, manifest.ErrSourceSkipped) {
			s.UpdateStatus(id, store.StatusReady,
				store.SkippedPrefix+" "+manifest.TrimSentinelPrefix(err.Error()))
			return nil
		}
		s.UpdateStatus(id, store.StatusFailed, err.Error())
		return nil
	}
	if existing, ok := s.GetStatus(id); ok {
		if existing.Status == store.StatusFailed {
			return nil
		}
		if existing.Status == store.StatusReady && existing.Message != "" {
			// Informative Ready message (skipped:, unchanged, suspended) —
			// don't clobber.
			return nil
		}
	}
	s.UpdateStatus(id, store.StatusReady, "")
	return nil
}

// statusReady reports whether id's current store status is Ready — the dag
// scheduler's "this terminal node satisfies a dependency" signal.
func (c *Controller) statusReady(id manifest.NamedResource) bool {
	info, ok := c.Store.GetStatus(id)
	return ok && info.Status == store.StatusReady
}

// DispatchNode runs id's reconcile body under the dag engine and reports what
// the scheduler needs: the blocked dependency set (nil = terminalized) and
// whether id's final store status is Ready. It performs the same PreGate /
// preflight pre-checks the event listener (OnReconcile) does, then
// RunWithStatusOutcome. drainLevel threads the scheduler's fixpoint level into
// Require via ctx. The orchestrator's Dispatcher calls this, routing by Kind, so
// no per-kind match check is needed here.
func DispatchNode[T manifest.BaseManifest](
	ctx context.Context,
	c *Controller,
	id manifest.NamedResource,
	drainLevel int,
	suspended func(T) bool,
	logKind string,
	reconcile func(context.Context, T) error,
) (blocked []manifest.NamedResource, ready bool) {
	ctx = WithDrainLevel(ctx, drainLevel)
	obj, ok := store.Get[T](c.Store, id)
	if !ok {
		// Vanished — RunWithStatusOutcome's vanished branch records a terminal
		// status; report it.
		blocked = RunWithStatusOutcome[T](ctx, c.Store, id, logKind, reconcile)
		return blocked, c.statusReady(id)
	}
	if c.PreGate(id, suspended(obj)) {
		return nil, c.statusReady(id) // gated out: Ready (suspended/unchanged)
	}
	if msg, failed := c.PreflightFailure(id); failed {
		c.Store.UpdateStatus(id, store.StatusFailed, msg)
		return nil, false
	}
	blocked = RunWithStatusOutcome[T](ctx, c.Store, id, logKind, reconcile)
	return blocked, c.statusReady(id)
}

// OnReconcile builds the EventObjectAdded listener every controller registers
// in Start. It collapses the previously-duplicated onObjectAdded shape across
// the source/kustomization/helmrelease controllers into one generic: match the
// id, type-assert the payload to T, PreGate on suspension, fail-fast on a
// preflight error, then Submit the reconcile wrapped in RunWithStatus.
//
// The per-controller bits are the parameters: match (a single-kind check for
// KS/HR, a fetcher-registered check for source), suspended (the Suspend field
// for KS/HR, the Suspendable interface for source), the logKind label, and the
// typed reconcile fn. PreflightFailure is safe to call for every controller —
// it returns ("", false) when no preflight reporter is wired (source), so the
// shared check is a no-op there.
func OnReconcile[T manifest.BaseManifest](
	ctx context.Context,
	c *Controller,
	match func(manifest.NamedResource) bool,
	suspended func(T) bool,
	logKind string,
	reconcile func(context.Context, T) error,
) store.Listener {
	return func(id manifest.NamedResource, payload any) {
		if !match(id) {
			return
		}
		obj, ok := payload.(T)
		if !ok {
			return
		}
		if c.PreGate(id, suspended(obj)) {
			return
		}
		if msg, failed := c.PreflightFailure(id); failed {
			c.Store.UpdateStatus(id, store.StatusFailed, msg)
			return
		}
		c.Submit(ctx, id, func(ctx context.Context) {
			RunWithStatus(ctx, c.Store, id, logKind, reconcile)
		})
	}
}
