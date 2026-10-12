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
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Controller is the embeddable lifecycle harness. Construct via New,
// install reconcile-shaping config via SetFilter, then call StartLifecycle
// before dispatch or listener registration.
//
// All four concrete controllers carry the same lifecycle shape:
//   - a started gate so configuration during dispatch is a hard error
//   - a filter for changed-only mode
//   - an unsubscriber list cleared by Close
//
// Encoding it once means new pre-reconcile concerns (rate-limit,
// retries, debug-mode toggle) drop into one place and propagate.
//
// The KS and HR controllers additionally share depwait configuration
// (Existence) and a pre-reconcile preflight check (Preflight,
// ParentOf). Configure these via SetDepwait / SetPreflight / SetParentOf
// before dispatch so reconcile bodies can call Require, PreflightError,
// and LookupParent without each controller duplicating the nil-check
// boilerplate.
type Controller struct {
	Store *store.Store
	Tasks *task.Service

	// name is the controller's stable identity (e.g. "helmrelease",
	// "kustomization", "resourceset", "source"), set once at New and never
	// mutated. Read via Name().
	name string

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

	// Shared KS/HR depwait and preflight state. Set via SetDepwait,
	// SetPreflight, SetParentOf. The source controller leaves these nil;
	// KS and HR configure them before dispatch via their Configure methods.
	existence        depwait.ExistenceLookup
	allowMissingCRDs bool
	preflight        func(manifest.NamedResource) (string, bool)
	parentOf         func(manifest.NamedResource) (manifest.NamedResource, bool)
}

// New constructs a base controller with the given stable name (its
// controller-kind identity, surfaced in reconcile logs via Name()). Concrete
// controllers call this from their own constructor and embed the result.
func New(s *store.Store, t *task.Service, name string) *Controller {
	return &Controller{
		Store: s,
		Tasks: t,
		name:  name,
		log:   slog.Default().With(slog.String("controller", name)),
	}
}

// Name returns the controller's stable identity (e.g. "helmrelease"). Set once
// at New.
func (c *Controller) Name() string { return c.name }

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

// SetFilter installs the change filter that gates reconciliation in
// changed-only mode. Panics once StartLifecycle is called; the invariant is
// that reconcile-shaping config is frozen once dispatch begins.
func (c *Controller) SetFilter(f *change.Filter) {
	c.requireNotStarted("SetFilter")
	c.filter = f
}

// SetDepwait installs the depwait resolution wires.
// Panics once StartLifecycle is called.
func (c *Controller) SetDepwait(existence depwait.ExistenceLookup) {
	c.requireNotStarted("SetDepwait")
	c.existence = existence
}

// SetPreflight installs the pre-reconcile failure reporter.
// Panics once StartLifecycle is called.
func (c *Controller) SetPreflight(f func(manifest.NamedResource) (string, bool)) {
	c.requireNotStarted("SetPreflight")
	c.preflight = f
}

// SetParentOf installs the structural parent resolver.
// Panics once StartLifecycle is called.
func (c *Controller) SetParentOf(f func(manifest.NamedResource) (manifest.NamedResource, bool)) {
	c.requireNotStarted("SetParentOf")
	c.parentOf = f
}

// Options is the post-bootstrap, reconcile-shaping config common to every
// render controller (Kustomization, HelmRelease, ResourceSet). The
// orchestrator wires it once before dispatch; the source controller is fetch-only
// and configures just its filter directly, so it does NOT embed this. A
// concrete controller embeds Options in its own option struct and forwards it
// to Configure, then assigns its controller-specific fields.
type Options struct {
	// Filter narrows reconciliation to changed resources in changed-only mode.
	Filter *change.Filter
	// ParentOf maps a resource to its structural-parent Kustomization so
	// reconcile waits for the parent's Ready before rendering — any
	// parent-render-time spec mutation (replacements injecting
	// targetNamespace, postBuild substitutions, a re-emitted patched spec) is
	// then observable. Nil disables parent enforcement.
	ParentOf func(manifest.NamedResource) (manifest.NamedResource, bool)
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

// Configure applies the common reconcile-shaping config. A concrete controller
// calls c.Controller.Configure(opts.Options) before assigning its own fields.
// The setters are independent single-field writes, so their order is
// immaterial; each panics once StartLifecycle is called.
func (c *Controller) Configure(opts Options) {
	c.SetFilter(opts.Filter)
	c.SetDepwait(opts.Existence)
	c.SetPreflight(opts.PreflightFailure)
	c.SetParentOf(opts.ParentOf)
	c.allowMissingCRDs = opts.AllowMissingCRDs
}

// KeepEmitted extends the change filter's keep set so render-emitted
// children pass the changed-only-mode preGate check. Without this, a
// parent whose render emits a child that wasn't on disk at filter-build
// time (kustomize component+replacement KSes, charts that render source
// CRs) would silently drop that child from the diff comparison. Routed
// through Filter.AddEmitted so an ancestor-only parent doesn't cascade
// unrelated file-loaded children into keep (#204/#260/#308).
//
// Newly-kept source and Kustomization ids are refired because their listeners
// preGate-skipped before a consumer joined keep (#260, #418). Other kinds are never
// refired: nothing moves a refired HelmChart's Pending status back to Ready.
//
// MUST be called BEFORE Store.AddObject so the listener that fires
// synchronously during AddObject sees the extended keep set.
func (c *Controller) KeepEmitted(parent manifest.NamedResource, child manifest.BaseManifest) {
	if f := c.filter; f != nil {
		for _, id := range f.AddEmitted(parent, child) {
			switch id.Kind {
			case manifest.KindGitRepository, manifest.KindOCIRepository,
				manifest.KindHelmRepository, manifest.KindBucket,
				manifest.KindExternalArtifact, manifest.KindKustomization:
				c.Store.Refire(id)
			}
		}
	}
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
// pass; emit is the controller's Emit(id, docs, wipeSecrets, publish=false)
// replay closure. The replay deliberately does NOT re-publish the children: they were
// already published byte-identically by the render that set this artifact, so
// re-AddObject-ing them would only re-fire listeners and re-submit already-
// settled resources — churn that can transiently un-Ready a parent and race
// quiescence (the "not ready" non-determinism, see #657–#660).
//
// Returns (handled=true, err): the caller returns err. err is non-nil only when
// a preflight error was discovered mid-flight. Returns (false, nil) to render
// normally. Centralizes the byte-identical KS/HR dedup short-circuit.
func (c *Controller) FingerprintDedup(id manifest.NamedResource, fp string, emit func(docs []map[string]any)) (bool, error) {
	existing, ok := c.Store.GetArtifact(id).(store.RenderedArtifact)
	if !ok || existing.RenderedFingerprint() == "" || existing.RenderedFingerprint() != fp {
		return false, nil
	}
	if err := c.PreflightError(id); err != nil {
		return true, err
	}
	c.log.Debug("skipped re-render (fingerprint unchanged)", "id", id.String())
	emit(existing.RenderedManifests())
	return true, nil
}

// IsFileIndexed reports whether id is tracked by the file-existence
// index wired at Configure time. Returns false when no index is
// configured (offline / unit-test paths), which degrades safely by
// treating the resource as not-file-indexed.
func (c *Controller) IsFileIndexed(id manifest.NamedResource) bool {
	return c.existence != nil && c.existence.IsFileIndexed(id)
}

// OrderingDeps clones explicit dependsOn refs and prunes only known unchanged
// targets. Unknown refs MUST remain available for missing-dependency diagnosis.
func (c *Controller) OrderingDeps(deps []manifest.DependencyRef) []manifest.DependencyRef {
	deps = slices.Clone(deps)
	if c.filterActive() {
		deps = slices.DeleteFunc(deps, func(d manifest.DependencyRef) bool {
			return !c.filter.ShouldReconcile(d.NamedResource) &&
				(c.IsFileIndexed(d.NamedResource) || c.Store.GetObject(d.NamedResource) != nil)
		})
	}
	return deps
}

// SourceSkipped converts a referenced source's soft skip into the caller's skip.
func (c *Controller) SourceSkipped(id manifest.NamedResource, label string) error {
	if info, ok := c.Store.GetStatus(id); ok && store.IsSkipped(info) {
		return fmt.Errorf("%w: %s %s %s", manifest.ErrSourceSkipped, label, id.String(), info.Message)
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

// preGate is the canonical Suspend/Filter pre-reconcile check.
// Returns true when the resource is gated out — caller MUST bail.
//
//   - suspended → marks Ready "suspended", returns true
//   - filter excludes the id → marks Ready "unchanged", returns true
//   - otherwise → returns false, caller proceeds to Submit/reconcile
func (c *Controller) preGate(id manifest.NamedResource, suspended bool) bool {
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

// filterActive reports whether a non-nil, enabled change filter is configured.
func (c *Controller) filterActive() bool {
	return c.filter != nil && c.filter.Enabled()
}

// Require gates the caller on deps WITHOUT blocking: it classifies each dep
// and returns one of:
//   - nil — every dep satisfied; the body proceeds.
//   - *manifest.DependencyFailedError — a dep is terminally
//     Failed and none is still blockable; instant cascade, no grace.
//   - *depwait.ErrBlocked — at least one dep is absent/Pending/ReadyExpr-pending;
//     the body returns and the scheduler parks the node on those deps.
//
// Blocked WINS over failed: if any dep is still producible (ClassBlocked) we
// park even when another dep already failed. The re-run re-derives failure
// once nothing is blocked, so values omission cannot discard producible refs.
// Callers own their progress status writes.
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

// RequireRefresh fuses Require with the load-bearing store re-read every
// dependency gate performs on success. A structural parent may have re-emitted
// this object with a mutated spec while it was parked, so the caller MUST
// reconcile against the refreshed object, not the stale snapshot it was
// dispatched with (see #102 and the dependsOn re-read comments at the call
// sites). Wait and re-read were two statements joined only by convention;
// fusing them means a future gate site can't call Require and silently forget
// the re-read.
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
	s.UpdateStatus(id, store.StatusFailed, fmt.Sprintf("panic: %v", r))
	panic(r)
}

// RunWithStatusOutcome re-reads the stored object, runs its reconcile body,
// and records a terminal status under panic recovery. Informative Ready
// messages and externally recorded failures MUST survive a no-op reconcile.
// A missing object is skipped. A blocked dependency set is returned without
// marking Failed so the scheduler can park and re-run the node.
func RunWithStatusOutcome[T manifest.BaseManifest](
	ctx context.Context,
	s *store.Store,
	id manifest.NamedResource,
	log *slog.Logger,
	fn func(context.Context, T) error,
) []manifest.NamedResource {
	defer recoverPanic(s, id, log)
	obj, ok := s.Get[T](id)
	if !ok {
		// Object deleted (or wrong type) between discovery/enqueue and run.
		// Write a terminal Ready with a brief reason so dependents unblock and
		// the testrunner reports cleanly — a vanished resource is the same
		// outcome real Flux sees when the CR is deleted.
		if info, has := s.GetStatus(id); has && info.Status != store.StatusReady {
			s.UpdateStatus(id, store.StatusReady, store.SkippedPrefix+" object not in store at reconcile time")
		}
		return nil
	}
	if err := fn(ctx, obj); err != nil {
		// A dependency gate is unsatisfied. Surface the blocked deps WITHOUT
		// writing Failed — the node keeps the Pending status the body wrote and
		// stays parkable + re-runnable.
		if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
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
		s.UpdateFailure(id, err)
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

// Dispatch bundles a controller's suspension check and typed reconcile body.
type Dispatch[T manifest.BaseManifest] struct {
	Suspended func(T) bool
	Reconcile func(context.Context, T, manifest.DrainLevel) error
}

// DispatchNode runs id's reconcile body under the dag engine and reports the
// blocked dependency set (nil = terminalized). It performs the preGate /
// preflight pre-checks, then RunWithStatusOutcome. level threads the
// scheduler's fixpoint level into Require. The orchestrator's Dispatcher
// calls this, routing by Kind, so no per-kind match check is needed here.
func (c *Controller) DispatchNode[T manifest.BaseManifest](
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	dispatch Dispatch[T],
) []manifest.NamedResource {
	prior, _ := c.Store.GetStatus(id)
	reconcile := func(ctx context.Context, obj T) error {
		return dispatch.Reconcile(ctx, obj, level)
	}
	obj, ok := c.Store.Get[T](id)
	if !ok {
		// Vanished — RunWithStatusOutcome's vanished branch records a terminal status.
		return RunWithStatusOutcome[T](ctx, c.Store, id, c.log, reconcile)
	}
	if c.preGate(id, dispatch.Suspended(obj)) {
		return nil // gated out (suspended/unchanged)
	}
	if msg, failed := c.PreflightFailure(id); failed {
		c.Store.UpdateStatus(id, store.StatusFailed, msg)
		return nil
	}
	if prior.IsDependencyFailure() {
		c.Store.UpdateStatus(id, store.StatusPending, "")
	}
	return RunWithStatusOutcome[T](ctx, c.Store, id, c.log, reconcile)
}
