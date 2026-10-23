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
// (DispatchNode) that surrounds individual reconcile bodies.
package base

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
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

// FingerprintDedup returns the key, handled flag and proven no-publication flag.
// Nil or unmarshalable content MUST bypass replay; preflight failures still win.
func (c *Controller) FingerprintDedup(id manifest.NamedResource, content any) (string, bool, bool, error) {
	if content == nil {
		return "", false, false, nil
	}
	fp := manifest.Fingerprint(content)
	if fp == "" {
		return "", false, false, nil
	}
	current, _ := c.Store.GetArtifact(id).(*store.Emission)
	if current != nil && current.Fingerprint == fp {
		if err := c.PreflightError(id); err != nil {
			return fp, true, false, err
		}
		return fp, true, true, nil
	}
	_, attempt := c.Store.GetEmission(id)
	defer attempt.Finish()
	if attempt.Fingerprint != fp {
		return fp, false, false, nil
	}
	if err := c.PreflightError(id); err != nil {
		return fp, true, false, err
	}
	for _, child := range attempt.Children {
		c.KeepEmitted(id, child.Obj)
	}
	attempt.Complete = true
	c.Store.RecordEmitted(id, &attempt)
	return fp, true, false, nil
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

// Require classifies one invocation into a zero-valued caller-owned resolution.
func (c *Controller) Require(w *depwait.Waiter, attempt manifest.ReconcileAttempt, result *depwait.Resolution) error {
	w.Store, w.Existence, w.AllowMissingCRDs = c.Store, c.existence, c.allowMissingCRDs
	w.Classify(attempt, result)
	return result.Err()
}

var errSuspended = errors.New("reconcile suspended")

// RequireRefresh checks dependencies and re-reads the canonical object after a
// successful gate. Callers MUST reconcile against that object when found and
// retain their snapshot when absent. A structural parent may have re-emitted a
// changed spec while this object was parked. A suspended canonical object is
// marked Ready/suspended and returns an internal stop error consumed by
// DispatchNode; callers MUST propagate that error before rendering or
// emitting children.
func (c *Controller) RequireRefresh[T manifest.BaseManifest](
	_ context.Context,
	id manifest.NamedResource,
	deps []manifest.DependencyRef,
	level manifest.DrainLevel,
) (T, bool, error) {
	var result depwait.Resolution
	if err := c.Require(&depwait.Waiter{Parent: id, Dependencies: deps}, manifest.ReconcileAttempt{Drain: level}, &result); err != nil {
		var zero T
		return zero, false, err
	}
	obj, ok := c.Store.Get[T](id)
	if ok && c.preGate(obj) {
		return obj, true, errSuspended
	}
	return obj, ok, nil
}

// DispatchNode gates an invocation and preserves its final dependency classification.
func (c *Controller) DispatchNode[T manifest.BaseManifest](
	ctx context.Context,
	id manifest.NamedResource,
	attempt manifest.ReconcileAttempt,
	fn func(context.Context, T, manifest.ReconcileAttempt, bool) (schedule.Outcome, error),
) (schedule.Outcome, manifest.ReconcileWait) {
	s := c.Store
	prior, _ := s.GetStatus(id)
	obj, ok := s.Get[T](id)
	gated := false
	if ok {
		switch {
		case c.preGate(obj):
			gated = true
		case c.filterActive() && !c.filter.ShouldReconcile(id):
			s.UpdateStatus(id, store.StatusReady, store.ReasonUnchanged, "unchanged")
			gated = true
		default:
			if msg, failed := c.PreflightFailure(id); failed {
				s.UpdateStatus(id, store.StatusFailed, "", msg)
				gated = true
			} else if prior.IsDependencyFailure() {
				s.UpdateStatus(id, store.StatusPending, "", "")
			}
		}
	}
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		c.log.Error("panic during reconcile", "id", id.String(), "panic", r)
		s.UpdateStatus(id, store.StatusFailed, "", fmt.Sprintf("panic: %v", r))
		// Re-panic so task.Service records the failed callback.
		panic(r)
	}()
	// Gate status listeners may replace the object; an admitted body must re-read it.
	if !gated {
		obj, ok = s.Get[T](id)
	}
	if !ok {
		if info, has := s.GetStatus(id); has && info.Status != store.StatusReady {
			s.UpdateStatus(id, store.StatusReady, store.ReasonSkipped, "object not in store at reconcile time")
		}
		return schedule.Outcome{}, manifest.ReconcileWait{}
	}
	out, err := fn(ctx, obj, attempt, gated)
	if gated {
		return out, manifest.ReconcileWait{}
	}
	switch {
	case errors.Is(err, errSuspended):
		out.Noop, out.Rerun = false, false
	case err != nil:
		out.Noop = false
		if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
			if !blocked.Empty() {
				out.Kind = schedule.Blocked
			}
			return out, blocked.ReconcileWait
		}
		if errors.Is(err, manifest.ErrSourceSkipped) {
			s.UpdateStatus(id, store.StatusReady, store.ReasonSkipped,
				strings.TrimSpace(manifest.TrimSentinelPrefix(err.Error())))
		} else {
			s.UpdateFailure(id, err)
			if failed, ok := errors.AsType[*manifest.DependencyFailedError](err); ok && len(failed.Failed) != 0 {
				out.Kind = schedule.DependencyFailed
				return out, manifest.ReconcileWait{Deps: failed.Failed}
			}
		}
	default:
		if existing, ok := s.GetStatus(id); ok && (existing.Status == store.StatusFailed ||
			existing.Status == store.StatusReady && (existing.Message != "" || existing.Outcome() != store.OutcomePassed)) {
			out.Noop = false
			if existing.IsDependencyFailure() {
				out.Kind = schedule.DependencyFailed
				return out, manifest.ReconcileWait{Deps: existing.BlockedBy()}
			}
			return out, manifest.ReconcileWait{}
		}
		s.UpdateStatus(id, store.StatusReady, "", "")
	}
	return out, manifest.ReconcileWait{}
}
