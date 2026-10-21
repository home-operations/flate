// Package base provides the shared lifecycle harness every flate
// controller wraps around its per-resource reconcile body.
//
// Each concrete controller (source, kustomization, helmrelease, resourceset)
// embeds *base.Controller and contributes only the controller-specific
// dependencies (Fetchers, Helm client, Staging cache, ...) plus the
// reconcile function itself. Lifecycle wiring — the started gate, the
// unsubscriber slice, the per-id coalescer, the change filter, the
// Suspend/Filter pre-gate — lives here exactly once.
//
// The package also owns the panic-recovery + status-transition harness
// (RunWithStatusOutcome) that surrounds individual reconcile bodies.
package base

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller owns shared reconciliation lifecycle, dependency gates and publication.
// Configure MUST complete before StartLifecycle or dispatch.
type Controller struct {
	Store *store.Store
	Tasks *task.Service

	// log is name's logger, pre-bound at New with a controller=<name>
	// attribute. Reconcile bodies and the shared base helpers log through it
	// (via Logger()) so the controller identity is a structured field rather
	// than a per-call message prefix. Bound once from slog.Default(), which the
	// CLI installs in PersistentPreRunE before any controller is constructed.
	log *slog.Logger

	started atomic.Bool
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
	// The slice is short-lived (registered via AddListener, drained at Close)
	// and per-controller, so the lock has near-zero contention.
	unsubMu sync.Mutex
	unsub   []store.Unsubscribe

	dependencySubstitutions values.DependencySubstitutions
	existence               depwait.ExistenceLookup
	allowMissingCRDs        bool
	preflight               func(manifest.NamedResource) (string, bool)
}

// New constructs a controller with its stable identity for reconciliation logs.
func New(s *store.Store, t *task.Service, name string) *Controller {
	return &Controller{
		Store: s,
		Tasks: t,
		log:   slog.Default().With(slog.String("controller", name)),
	}
}

// Logger returns the controller's slog logger, pre-bound with a
// controller=<name> attribute. Reconcile bodies (and the emit helper, via the
// controller they receive) log through it so the controller identity is a
// structured field rather than a per-call message prefix.
func (c *Controller) Logger() *slog.Logger { return c.log }

// requireNotStarted panics if the started gate is set, enforcing the
// invariant that reconcile-shaping config is frozen once dispatch
// begins. method is the calling setter's name for the panic message.
func (c *Controller) requireNotStarted(method string) {
	if c.started.Load() {
		panic("controller: " + method + " called after dispatch begins")
	}
}

// Options is the post-bootstrap, reconcile-shaping config common to every
// render controller (Kustomization, HelmRelease, ResourceSet). The
// orchestrator wires it once before dispatch; the source controller is fetch-only
// and configures just its filter directly, so it does NOT embed this. A
// concrete controller embeds Options in its own option struct and forwards it
// to Configure, then assigns its controller-specific fields.
type Options struct {
	// DependencySubstitutions is the frozen discovered lookup used before child publication.
	DependencySubstitutions values.DependencySubstitutions
	// Filter narrows reconciliation to changed resources in changed-only mode.
	Filter *change.Filter
	// Index contains immutable source-qualified discovery evidence.
	Index *manifest.DiscoveryIndex
	// Existence is the file-existence lookup depwait uses to lazy-promote
	// file-indexed deps and to distinguish a render-only dep still in flight
	// from a typo'd one. See depwait.ExistenceLookup.
	Existence depwait.ExistenceLookup
	// AllowMissingCRDs accepts CRD dependencies absent from the offline inputs
	// because the cluster installs them at runtime.
	AllowMissingCRDs bool
	// PreflightFailure reports dependency-graph errors discovered before
	// reconcile; when set for an id the controller marks it Failed and renders
	// nothing.
	PreflightFailure func(manifest.NamedResource) (string, bool)
}

// Configure installs immutable setup before dispatch.
func (c *Controller) Configure(opts Options) {
	c.requireNotStarted("Configure")
	c.dependencySubstitutions = opts.DependencySubstitutions
	c.filter = opts.Filter
	c.existence = opts.Existence
	c.preflight = opts.PreflightFailure
	c.Store.SetDiscoveryIndex(opts.Index)
	c.allowMissingCRDs = opts.AllowMissingCRDs
}

// KeepEmitted extends the change filter's keep set so render-emitted
// children pass the changed-only filter. Without this, a
// parent whose render emits a child that wasn't on disk at filter-build
// time (kustomize component+replacement KSes, charts that render source
// CRs) would silently drop that child from the diff comparison. Routed
// through Filter.AddEmitted so an ancestor-only parent doesn't cascade
// unrelated file-loaded children into keep (#204/#260/#308).
//
// Newly-kept source and Kustomization ids are refired because their listeners
// unchanged-filtered before a consumer joined keep (#260, #418). Other kinds are never
// refired: nothing moves a refired HelmChart's Pending status back to Ready.
//
// MUST be called BEFORE Store.AddObject so the listener that fires
// synchronously during AddObject sees the extended keep set.
func (c *Controller) KeepEmitted(parent manifest.NamedResource, child manifest.BaseManifest) {
	if f := c.filter; f != nil {
		for _, id := range f.AddEmitted(parent, child) {
			if id.KindInfo().Refire {
				c.Store.Refire(id)
			}
		}
	}
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
	c.Store.UpdateStatus(id, store.StatusPending, "", msg)
}

// FingerprintDedup derives the replay key from content and reuses accepted children.
// Nil or unmarshalable content MUST bypass replay; preflight failures still win.
func (c *Controller) FingerprintDedup(id manifest.NamedResource, content any) (string, bool, error) {
	if content == nil {
		return "", false, nil
	}
	fp := manifest.Fingerprint(content)
	if fp == "" {
		return "", false, nil
	}
	current, _ := c.Store.GetArtifact(id).(*store.Emission)
	if current != nil && current.Fingerprint == fp {
		if err := c.PreflightError(id); err != nil {
			return fp, true, err
		}
		for _, child := range current.Children {
			c.KeepEmitted(id, child.Obj)
		}
		return fp, true, nil
	}
	_, attempt := c.Store.GetEmission(id)
	defer attempt.Finish()
	if attempt.Fingerprint != fp {
		return fp, false, nil
	}
	if err := c.PreflightError(id); err != nil {
		return fp, true, err
	}
	for _, child := range attempt.Children {
		c.KeepEmitted(id, child.Obj)
	}
	attempt.Complete = true
	c.Store.RecordEmitted(id, &attempt)
	return fp, true, nil
}

// KeepOrderingDep drops only known unchanged targets; unknown refs MUST remain
// available for missing-dependency diagnosis.
func (c *Controller) KeepOrderingDep(id manifest.NamedResource) bool {
	return !c.filterActive() || c.filter.ShouldReconcile(id) ||
		(c.existence == nil || !c.existence.IsFileIndexed(id)) && c.Store.GetObject(id) == nil
}

// SourceSkipped converts a referenced source's soft skip into the caller's skip.
func (c *Controller) SourceSkipped(id manifest.NamedResource, label string) error {
	if info, ok := c.Store.GetStatus(id); ok && info.Outcome() == store.OutcomeSkipped {
		return fmt.Errorf("%w: %s %s skipped: %s", manifest.ErrSourceSkipped, label, id.String(), info.Message)
	}
	return nil
}

// StartLifecycle flips the started gate, freezing reconcile-shaping
// config. Call before dispatch or installing listeners via AddListener.
func (c *Controller) StartLifecycle() {
	c.started.Store(true)
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

// preGate marks suspended objects Ready; refresh MUST never apply changed-only filtering.
func (c *Controller) preGate(obj manifest.BaseManifest) bool {
	if obj.Suspended() {
		c.Store.UpdateStatus(obj.Named(), store.StatusReady, store.ReasonSuspended, "suspended")
		return true
	}
	return false
}

// filterActive reports whether a non-nil, enabled change filter is configured.
func (c *Controller) filterActive() bool {
	return c.filter != nil && c.filter.Enabled()
}

// Require requests readiness for each compact prerequisite; statusless targets
// need only existence. Blocked gates take precedence over failures so a producible
// reference cannot be discarded. Callers own their progress status writes.
func (c *Controller) Require(
	_ context.Context,
	id manifest.NamedResource,
	deps []manifest.DependencyRef,
	level manifest.DrainLevel,
) error {
	w := depwait.Waiter{
		Store: c.Store, Parent: id,
		Existence: c.existence, AllowMissingCRDs: c.allowMissingCRDs,
	}
	var blocked []manifest.NamedResource
	var failed *manifest.DependencyFailedError
	for _, dep := range deps {
		switch cl := w.Classify(dep, level); cl.Kind {
		case depwait.ClassReady:
		case depwait.ClassFailed:
			if failed == nil {
				failed = &manifest.DependencyFailedError{Parent: id, Reasons: make(map[manifest.NamedResource]string)}
			}
			failed.Failed = append(failed.Failed, dep.NamedResource)
			failed.Reasons[dep.NamedResource] = cl.Message
		case depwait.ClassBlocked:
			blocked = append(blocked, dep.NamedResource)
		}
	}
	if len(blocked) > 0 {
		return &depwait.ErrBlocked{Deps: blocked}
	}
	if failed != nil {
		slices.SortFunc(failed.Failed, manifest.NamedResource.Compare)
		return failed
	}
	return nil
}

var errSuspended = errors.New("reconcile suspended")

// RequireRefresh checks dependencies and re-reads the canonical object after a
// successful gate. Callers MUST reconcile against that object when found and
// retain their snapshot when absent. A structural parent may have re-emitted a
// changed spec while this object was parked. A suspended canonical object is
// marked Ready/suspended and returns an internal stop error consumed by
// RunWithStatusOutcome; callers MUST propagate that error before rendering or
// emitting children.
func (c *Controller) RequireRefresh[T manifest.BaseManifest](
	ctx context.Context,
	id manifest.NamedResource,
	deps []manifest.DependencyRef,
	level manifest.DrainLevel,
) (T, bool, error) {
	if err := c.Require(ctx, id, deps, level); err != nil {
		var zero T
		return zero, false, err
	}
	obj, ok := c.Store.Get[T](id)
	if ok && c.preGate(obj) {
		return obj, true, errSuspended
	}
	return obj, ok, nil
}

// recoverPanic catches a panic from the current goroutine and marks id
// StatusFailed with a "panic: <r>" message so the orchestrator
// surfaces it. log is the controller's pre-bound logger (Logger()); the
// panic is recorded under its controller=<name> attribute.
//
// After recording status, re-raises the panic so the enclosing
// task.Service.Go increments its failures counter — a panicked
// reconcile MUST count against the orchestrator's failure gate, not
// silently masquerade as success when Service.Failures() is consulted
// for the final exit-code decision.
func recoverPanic(s *store.Store, id manifest.NamedResource, log *slog.Logger) {
	r := recover()
	if r == nil {
		return
	}
	log.Error("panic during reconcile", "id", id.String(), "panic", r)
	s.UpdateStatus(id, store.StatusFailed, "", fmt.Sprintf("panic: %v", r))
	panic(r)
}

// RunWithStatusOutcome re-reads the stored object, runs its reconcile body,
// and records a terminal status under panic recovery. Informative Ready
// messages and externally recorded failures MUST survive a no-op reconcile.
// A missing object is skipped. A blocked dependency set is returned without
// marking Failed so the scheduler can park and re-run the node.
func RunWithStatusOutcome[T manifest.BaseManifest](
	ctx context.Context,
	c *Controller,
	id manifest.NamedResource,
	fn func(context.Context, T) error,
) []manifest.NamedResource {
	s := c.Store
	defer recoverPanic(s, id, c.log)
	obj, ok := s.Get[T](id)
	if !ok {
		// Object deleted (or wrong type) between discovery/enqueue and run.
		// Write a terminal Ready with a brief reason so dependents unblock and
		// the testrunner reports cleanly — a vanished resource is the same
		// outcome real Flux sees when the CR is deleted.
		if info, has := s.GetStatus(id); has && info.Status != store.StatusReady {
			s.UpdateStatus(id, store.StatusReady, store.ReasonSkipped, "object not in store at reconcile time")
		}
		return nil
	}
	if err := fn(ctx, obj); err != nil {
		if errors.Is(err, errSuspended) {
			return nil
		}
		// A dependency gate is unsatisfied. Surface the blocked deps WITHOUT
		// writing Failed — the node keeps the Pending status the body wrote and
		// stays parkable + re-runnable.
		if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
			return blocked.Deps
		}
		if errors.Is(err, manifest.ErrSourceSkipped) {
			s.UpdateStatus(id, store.StatusReady, store.ReasonSkipped,
				strings.TrimSpace(manifest.TrimSentinelPrefix(err.Error())))
			return nil
		}
		s.UpdateFailure(id, err)
		return nil
	}
	if existing, ok := s.GetStatus(id); ok {
		if existing.Status == store.StatusFailed {
			return nil
		}
		if existing.Status == store.StatusReady &&
			(existing.Message != "" || existing.Outcome() != store.OutcomePassed) {
			return nil
		}
	}
	s.UpdateStatus(id, store.StatusReady, "", "")
	return nil
}

// DispatchNode runs id's reconcile body under the dag engine and reports the
// blocked dependency set (nil = terminalized). It checks suspension, the
// unchanged filter, and preflight before RunWithStatusOutcome. level threads the
// scheduler's fixpoint level into Require. The orchestrator's Dispatcher
// calls this, routing by Kind, so no per-kind match check is needed here.
func (c *Controller) DispatchNode[T manifest.BaseManifest](
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	fn func(context.Context, T, manifest.DrainLevel) error,
) []manifest.NamedResource {
	prior, _ := c.Store.GetStatus(id)
	reconcile := func(ctx context.Context, obj T) error {
		return fn(ctx, obj, level)
	}
	obj, ok := c.Store.Get[T](id)
	if !ok {
		// Vanished — RunWithStatusOutcome's vanished branch records a terminal status.
		return RunWithStatusOutcome[T](ctx, c, id, reconcile)
	}
	if c.preGate(obj) {
		return nil
	}
	if c.filterActive() && !c.filter.ShouldReconcile(id) {
		c.Store.UpdateStatus(id, store.StatusReady, store.ReasonUnchanged, "unchanged")
		return nil
	}
	if msg, failed := c.PreflightFailure(id); failed {
		c.Store.UpdateStatus(id, store.StatusFailed, "", msg)
		return nil
	}
	if prior.IsDependencyFailure() {
		c.Store.UpdateStatus(id, store.StatusPending, "", "")
	}
	return RunWithStatusOutcome[T](ctx, c, id, reconcile)
}
