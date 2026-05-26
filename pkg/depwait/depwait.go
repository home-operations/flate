// Package depwait blocks a controller until a set of NamedResource
// dependencies become Ready (or, for kinds without a status pipeline,
// merely exist) — with per-dep timeouts and fail-fast for missing refs.
package depwait

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// DefaultTimeout is the per-dep timeout when not specified. The
// upstream Flux controllers default to several minutes since they
// wait for in-cluster reconciliation; flate is purely offline, so
// waits past a few seconds almost always indicate a misconfigured
// reference. Keep this short so typos in dependsOn / sourceRef
// surface immediately instead of stalling a render.
const DefaultTimeout = 30 * time.Second

// MissingGrace is the brief window we tolerate a missing dep before
// failing — it covers the legitimate case where a KS render produces
// the dep slightly later in the same run.
const MissingGrace = 2 * time.Second

// RenderProducingTimeout caps how long step-2 will wait for a
// render-only dep (no file-existence record) to appear via a parent
// render's emitRenderedChildren chain. Bounded so a typo'd
// dependsOn — which also returns IsFileIndexed(id)=false and falls into
// step-2 — fails in a reasonable time instead of burning the full
// per-dep Timeout. Chosen to comfortably cover a multi-level
// kustomize component+replacement chain (~few seconds in practice)
// while keeping a typo's wall time well under DefaultTimeout.
//
// Exposed as a var so tests can shrink it.
var RenderProducingTimeout = 10 * time.Second

// TimeoutFromSpec resolves a Flux spec.timeout (`*metav1.Duration` —
// the shape used by both Kustomization and HelmRelease) into the
// effective per-dep wait. Honors a user-supplied value when set; falls
// back to flate's offline-tuned DefaultTimeout otherwise. Matches the
// principle that a real Flux reconcile would respect spec.timeout.
func TimeoutFromSpec(d *metav1.Duration) time.Duration {
	if d == nil || d.Duration <= 0 {
		return DefaultTimeout
	}
	return d.Duration
}

// DepStatus enumerates the per-dependency resolution result. Every
// Watch event carries one of the terminal statuses below — there is
// no "pending" wire value because Watch only fires when the dep
// resolves (Ready) or hits a wait-ending condition (Failed / Timeout
// / Cancelled).
type DepStatus int

// Possible DepStatus values.
const (
	DepReady DepStatus = iota + 1
	DepFailed
	DepTimeout
	DepCancelled
)

// Event is yielded for each dependency as it resolves.
type Event struct {
	Dep    manifest.NamedResource
	Status DepStatus
	Reason string
}

// Summary tallies the final state of a Waiter run. Every dep ends
// up in Ready or Failed (Watch always emits a terminal status); a
// Pending slot would be dead code.
type Summary struct {
	Ready    []manifest.NamedResource
	Failed   []manifest.NamedResource
	Messages map[manifest.NamedResource]string
}

// AllReady reports whether every dependency reached DepReady.
func (s Summary) AllReady() bool { return len(s.Failed) == 0 }

// AnyFailed reports whether at least one dependency ended in failure.
func (s Summary) AnyFailed() bool { return len(s.Failed) > 0 }

// Waiter holds the parameters for one dependency-wait operation.
type Waiter struct {
	Store   *store.Store
	Parent  manifest.NamedResource
	Timeout time.Duration

	// AdditiveReadyExpr toggles Flux's AdditiveCELDependencyCheck
	// feature gate. When false (the default, matching Flux), a
	// dep's ReadyExpr REPLACES the built-in Ready check — flate
	// re-evaluates the expression on every status update for the
	// dep and treats the dep as Ready when the expression returns
	// true. When true, the dep must satisfy both the built-in
	// Ready check AND the ReadyExpr (additive mode).
	AdditiveReadyExpr bool

	// Existence, when non-nil, drives the post-grace branching for
	// missing deps. The orchestrator wires this against the loader's
	// ExistenceIndex (file-loaded YAML records); tests supply stubs.
	// When Existence is nil, depwait preserves the historical fast-
	// fail-after-grace behavior — callers that don't wire an
	// existence index can't distinguish render-only from typo'd.
	//
	// See ExistenceLookup for the decision matrix at the grace
	// boundary.
	Existence ExistenceLookup

	// Renders, when non-nil, drives the step-2 fast-fail signal:
	// during the render-only long wait, depwait polls
	// Renders.OtherActive(); a false return means no other
	// reconcile in the orchestrator is doing work, so no future
	// render can produce the missing dep. The wait short-circuits
	// to "dependency not found" instead of burning the full
	// RenderProducingTimeout cap. When Renders is nil, step-2 falls
	// back to the fixed cap alone (preserves the legacy timing).
	Renders RenderInflight
}

// ExistenceLookup is the seam depwait uses to resolve missing
// dependencies. Bundled into one interface so the orchestrator
// (and any future embedder) wires a single object through the
// controller Options pipe instead of two parallel closures.
//
// The orchestrator's implementation reads from the loader's
// ExistenceIndex — file-indexed objects (CMs/Secrets/HRs the
// DiscoveryOnly loader kept out of the Store) get lazy-promoted
// the moment a depwait edge needs them, while render-only ids
// (no file record) signal depwait to keep waiting for an
// emitRenderedChildren chain instead of failing fast.
//
// When the grace window expires with id still missing:
//
//   - Promote(id) == true:
//     dep is now in the Store (lazy-promoted from a file YAML).
//     Wait continues against built-in status / exists semantics.
//   - Promote(id) == false, IsFileIndexed(id) == true:
//     file existed but promote failed (parse error, file mutated
//     since record). Fail fast as "dependency not found".
//   - Promote(id) == false, IsFileIndexed(id) == false:
//     no file record. Dep can only arrive via a parent render's
//     emitRenderedChildren chain. Keep watching for up to
//     RenderProducingTimeout — a deeply-chained kustomize
//     component+replacement pattern can take longer than the 2s
//     grace, and failing fast surfaces a spurious "dependency not
//     found" for a dep that would have arrived a few seconds later.
type ExistenceLookup interface {
	// IsFileIndexed reports whether id has a file-existence record.
	IsFileIndexed(id manifest.NamedResource) bool
	// Promote attempts to materialize id into the Store from the
	// file-existence index. Returns true when promotion succeeded
	// and id is now reachable via store.GetObject.
	Promote(id manifest.NamedResource) bool
}

// RenderInflight is the positive quiescence signal depwait's step-2
// uses to fail fast on truly-missing render-only deps. The
// orchestrator's implementation reads from task.Service.ActiveCount
// — true when more than the caller's own goroutine is active in
// the task pool (and could therefore still emit the missing dep);
// false when the orchestrator has drained.
//
// Semantically the inverse of the Existence-based step-2 wait: that
// path keeps waiting on the absence of a finite signal
// (RenderProducingTimeout); RenderInflight short-circuits the wait
// once a positive signal ("no more work could produce this") fires.
// Both together pin the wait between a fast-fail floor (no other
// work running) and a hard ceiling (the timeout cap).
//
// When Renders is nil, step-2 still works via the timeout cap alone
// — embedders that don't drive a task pool get the legacy
// behavior.
type RenderInflight interface {
	// OtherActive reports whether at least one reconcile beyond the
	// caller's own task is still running in the orchestrator's task
	// pool. depwait calls this from inside a Submit'd reconcile
	// body, so the caller's goroutine is counted; OtherActive
	// returns true iff the total active count exceeds 1.
	OtherActive() bool

	// QuiescenceCh returns a channel closed when the pool drains to
	// "no other work in flight" — the event-driven counterpart of
	// OtherActive that lets waitRenderEmission select on the
	// quiescence signal instead of polling. Each call returns a
	// fresh channel; callers Receive once. Implementations that
	// can't deliver an event-driven signal may return a nil channel,
	// in which case waitRenderEmission falls back to the legacy poll.
	QuiescenceCh() <-chan struct{}
}

// Watch concurrently watches each dep and returns a channel of Events.
// The channel closes when every dep has reached a terminal state or ctx
// expires. Callers should drain the channel.
//
// Watch picks WatchReady vs WatchExists per-dep based on
// store.SupportsStatus. When dep.ReadyExpr is non-empty the built-in
// Ready check is *replaced* by the CEL evaluation (Flux's default).
// Set Waiter.AdditiveReadyExpr=true to require BOTH (Flux's
// AdditiveCELDependencyCheck=true mode).
func (w *Waiter) Watch(ctx context.Context, deps []manifest.DependencyRef) <-chan Event {
	out := make(chan Event, len(deps))
	if len(deps) == 0 {
		close(out)
		return out
	}
	timeout := w.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	var wg sync.WaitGroup
	for _, dep := range deps {
		wg.Add(1)
		go func(dep manifest.DependencyRef) {
			defer wg.Done()
			// Recover from panics in watchOne (e.g. malformed CEL expression
			// evaluating against an unexpected payload type) so the whole
			// run isn't killed. The dep is reported failed instead.
			//
			// Send unconditionally — `out` is buffered to len(deps) so
			// the send never blocks. The previous select-on-ctx silently
			// dropped events on cancellation, leaving the consumer with
			// a Pending dep that would time out at the full budget.
			out <- safeWatchOne(ctx, w, dep, timeout)
		}(dep)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// ReadyNow reports whether every dependency is already satisfied
// without waiting. Controllers use this to avoid marking a reconcile
// quiescent while it is only confirming dependencies that are ready
// and can continue producing render output immediately.
func (w *Waiter) ReadyNow(deps []manifest.DependencyRef) bool {
	for _, dep := range deps {
		if !w.readyNow(dep) {
			return false
		}
	}
	return true
}

func (w *Waiter) readyNow(dep manifest.DependencyRef) bool {
	id := dep.NamedResource
	if !w.depExists(id) {
		return false
	}
	if !store.SupportsStatus(id.Kind) {
		return true
	}
	if dep.ReadyExpr != "" && !w.AdditiveReadyExpr {
		ev, done := w.tryReadyExpr(dep.ReadyExpr, id)
		return done && ev.Status == DepReady
	}
	info, ok := w.Store.GetStatus(id)
	if !ok || info.Status != store.StatusReady {
		return false
	}
	if dep.ReadyExpr != "" {
		ev, done := w.tryReadyExpr(dep.ReadyExpr, id)
		return done && ev.Status == DepReady
	}
	return true
}

// safeWatchOne wraps watchOne with panic recovery — a malformed CEL
// ReadyExpr (or any other internal bug) reports the dep as failed
// instead of taking the orchestrator down with it.
//
// The recovered panic is logged with its goroutine stack so the
// reporter can attribute "depwait panic: ..." failures to a concrete
// site (CEL projection panicking against a malformed Snapshot, a
// nil-typed assertion in labelsAndAnnotations, etc.). Without the
// stack the panic message alone is often opaque.
func safeWatchOne(ctx context.Context, w *Waiter, dep manifest.DependencyRef, timeout time.Duration) (ev Event) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("depwait: panic in watchOne",
				"dep", dep.String(),
				"panic", r,
				"stack", string(debug.Stack()))
			ev = Event{
				Dep:    dep.NamedResource,
				Status: DepFailed,
				Reason: fmt.Sprintf("depwait panic: %v", r),
			}
		}
	}()
	return w.watchOne(ctx, dep, timeout)
}

func (w *Waiter) watchOne(ctx context.Context, dep manifest.DependencyRef, timeout time.Duration) Event {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	id := dep.NamedResource

	// Fail fast on deps that never made it into the store: branch on
	// the Existence index (when wired) before burning MissingGrace.
	// We treat "object exists" OR "status known" as proof of presence
	// — the latter covers controllers that update status before
	// AddObject (and unit tests that set status only).
	if !w.depExists(id) {
		if err := w.resolveMissing(ctx, id); err != nil {
			return *err
		}
	}

	if !store.SupportsStatus(id.Kind) {
		_, err := w.Store.WatchExists(ctx, id)
		return classify(id, err, "")
	}

	// Non-additive ReadyExpr: ignore the built-in Ready check entirely.
	// Subscribe to status updates and re-evaluate the CEL on each fire
	// until it returns true (or the per-dep timeout / parent ctx
	// expires). Matches Flux's default semantics — the CEL is the
	// authoritative readiness signal.
	if dep.ReadyExpr != "" && !w.AdditiveReadyExpr {
		return w.watchReadyExpr(ctx, id, dep.ReadyExpr)
	}

	// Pass the orchestrator's quiescence channel (when wired) so the
	// store's post-Failed grace can short-circuit the moment no other
	// reconcile is in flight — a typo'd dependsOn won't be saved by a
	// future re-emit, and burning the full grace adds visible wall time
	// to errored runs (worst case: 3s × depth of chained parent gates).
	var quiesce <-chan struct{}
	if w.Renders != nil {
		quiesce = w.Renders.QuiescenceCh()
	}
	info, err := w.Store.WatchReady(ctx, id, quiesce)
	if err != nil {
		return classify(id, err, info.Message)
	}

	// Additive mode: built-in Ready check passed AND the CEL must also
	// agree. Flux's AdditiveCELDependencyCheck=true behavior.
	if dep.ReadyExpr != "" {
		ok, evalErr := evaluateReadyExpr(dep.ReadyExpr, w.Store, w.Parent, id)
		if evalErr != nil {
			return Event{Dep: id, Status: DepFailed, Reason: "readyExpr: " + evalErr.Error()}
		}
		if !ok {
			return Event{Dep: id, Status: DepFailed, Reason: "readyExpr returned false"}
		}
	}
	return Event{Dep: id, Status: DepReady, Reason: info.Message}
}

// resolveMissing handles the "dep isn't in the store yet" branch.
// Returns a non-nil Event pointer when the wait should terminate
// (success cases fall through to the normal Ready/Exists wait by
// returning nil).
//
// Branch order depends on whether the embedder wired an Existence
// index. Orchestrator path (Existence != nil): try synchronous
// Promote first, then route file-indexed-but-unpromotable failures
// to fast-fail and render-only deps to waitRenderEmission. The 2s
// MissingGrace is pure wall-clock waste here — Promote is the
// authoritative materializer, and waitRenderEmission has its own
// EventObjectAdded subscription for the chained-render race.
//
// Legacy path (Existence == nil): preserves the historic
// "WatchExists with grace, fast-fail on timeout" shape since
// embedders without an existence index can't distinguish typo from
// render-only.
func (w *Waiter) resolveMissing(ctx context.Context, id manifest.NamedResource) *Event {
	if w.Existence == nil {
		graceCtx, graceCancel := context.WithTimeout(ctx, MissingGrace)
		_, err := w.Store.WatchExists(graceCtx, id)
		graceCancel()
		if err != nil && !w.depExists(id) {
			return &Event{Dep: id, Status: DepFailed, Reason: "dependency not found"}
		}
		return nil
	}

	if w.Existence.Promote(id) {
		// Lazy-promoted into the Store; fall through to the regular
		// wait.
		return nil
	}
	if w.Existence.IsFileIndexed(id) {
		// File was indexed at scan time but Promote failed — parse
		// error, file mutated since record, etc. No amount of waiting
		// will materialize this dep.
		return &Event{Dep: id, Status: DepFailed, Reason: "dependency not found"}
	}

	// No file record: dep can only arrive via a parent render's
	// emitRenderedChildren chain. waitRenderEmission has its own
	// EventObjectAdded subscription (no race) and is bounded by
	// RenderProducingTimeout / QuiescenceCh / parent ctx.
	if err := w.waitRenderEmission(ctx, id); err != nil {
		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			ev := classify(id, ctx.Err(), "")
			return &ev
		case errors.Is(err, errRenderCapExceeded),
			errors.Is(err, context.DeadlineExceeded),
			errors.Is(err, errRenderDrained):
			// The render-emission cap fired (or the pool drained
			// without producing, or the wait was bounded by the
			// per-dep Timeout) — the dep isn't going to appear.
			// Fast-fail with "dependency not found" rather than the
			// ambiguous "timeout" label.
			return &Event{Dep: id, Status: DepFailed, Reason: "dependency not found"}
		default:
			return &Event{Dep: id, Status: DepFailed, Reason: err.Error()}
		}
	}
	return nil
}

// watchReadyExpr evaluates expr against id's projected state and
// returns DepReady when it produces true. On false / eval error it
// blocks on the next wake — fired by EITHER an EventStatusUpdated or
// an EventObjectAdded for id — and re-evaluates.
//
// Subscribing to both events closes a wedge class: CEL exprs commonly
// read `dep.metadata.labels[...]` (a property of the *object*, not
// status), and an AddObject that lands new labels without a paired
// SetCondition fires only EventObjectAdded. Without that subscription
// the waiter would miss the wake and ride the per-dep timeout. For
// kinds that emit both events on every reconcile this is harmless
// (the chan is wake-only with a buffer-1 coalesce).
//
// The eval pattern runs three times — pre-subscribe (initial),
// post-subscribe (race window between subscribe and live event), and
// on each fire — so the evaluate-and-translate-to-Event step is
// extracted into tryReadyExpr to keep the control flow readable.
func (w *Waiter) watchReadyExpr(ctx context.Context, id manifest.NamedResource, expr string) Event {
	if ev, done := w.tryReadyExpr(expr, id); done {
		return ev
	}

	// One channel, two listeners — every wake re-evaluates against
	// the latest store state. Coalesced via buffer-1 + default-send
	// so a burst of events doesn't queue redundant evaluations.
	ch := make(chan struct{}, 1)
	wake := func(other manifest.NamedResource, _ any) {
		if other != id {
			return
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	unsubStatus := w.Store.AddListener(store.EventStatusUpdated, wake, false)
	defer unsubStatus()
	unsubObject := w.Store.AddListener(store.EventObjectAdded, wake, false)
	defer unsubObject()

	// Close the subscribe-vs-fire race window: an event that landed
	// between the initial tryReadyExpr and AddListener would otherwise
	// be missed.
	if ev, done := w.tryReadyExpr(expr, id); done {
		return ev
	}

	for {
		select {
		case <-ch:
			if ev, done := w.tryReadyExpr(expr, id); done {
				return ev
			}
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return Event{Dep: id, Status: DepCancelled, Reason: "cancelled"}
			}
			return Event{Dep: id, Status: DepTimeout, Reason: "readyExpr timeout: " + ctx.Err().Error()}
		}
	}
}

// tryReadyExpr evaluates expr once and translates the outcome into
// the per-dep Event. Returns (event, true) on a definitive result —
// Ready, or a compile error that no amount of polling will fix.
// Returns (zero, false) when the expression produced a clean false
// OR a transient runtime eval error (typically a missing attribute
// because the dep's status hasn't been populated yet) — the caller
// should keep waiting on store events.
func (w *Waiter) tryReadyExpr(expr string, id manifest.NamedResource) (Event, bool) {
	ok, err := evaluateReadyExpr(expr, w.Store, w.Parent, id)
	if err != nil {
		var compileErr *celCompileErr
		if errors.As(err, &compileErr) {
			return Event{Dep: id, Status: DepFailed, Reason: "readyExpr: " + err.Error()}, true
		}
		// Eval error: transient, re-poll on next event.
		return Event{}, false
	}
	if ok {
		return Event{Dep: id, Status: DepReady, Reason: "readyExpr satisfied"}, true
	}
	return Event{}, false
}

// depExists reports whether a dep is known to the store via either an
// added object or a recorded status entry.
func (w *Waiter) depExists(dep manifest.NamedResource) bool {
	if w.Store.GetObject(dep) != nil {
		return true
	}
	_, ok := w.Store.GetStatus(dep)
	return ok
}

// errRenderCapExceeded is returned by waitRenderEmission when the
// inner RenderProducingTimeout fires (with or without parent ctx
// also dying). Distinguishes "cap-fired no-show" from a genuine
// caller-cancellation, both of which would otherwise look like
// `context.DeadlineExceeded` on the wire.
var errRenderCapExceeded = errors.New("render-producing-timeout cap reached")

// errRenderDrained is the sentinel waitRenderEmission returns when
// the orchestrator's task pool drains while step-2 is still waiting
// — no future emission can produce the missing dep, so the wait
// fails fast as "dependency not found" without burning the full
// RenderProducingTimeout cap. Internal to depwait; not exported.
var errRenderDrained = errors.New("render pool drained without emission")

// waitRenderEmission is step-2's bounded wait for a render-only dep
// to arrive. It composes three termination signals:
//
//   - Store.EventObjectAdded fires for id → returns nil (caller
//     falls through to the regular Ready wait).
//   - Renders.QuiescenceCh closes (the event-driven counterpart of
//     OtherActive) → returns errRenderDrained (no future emission
//     could produce id; caller fails fast).
//   - ctx hits its deadline / cancellation → returns ctx.Err().
//
// An overall cap of RenderProducingTimeout is layered onto ctx so
// the wait can't run past it even when Renders is nil (legacy
// embedders without a task pool).
//
// The previous implementation polled OtherActive every 100ms. With
// N concurrent waiters during a render that's N×10 wakes/sec doing
// nothing useful most of the time; the quiescence channel replaces
// the poll with one event per drain.
func (w *Waiter) waitRenderEmission(ctx context.Context, id manifest.NamedResource) error {
	started := time.Now()
	renderCtx, renderCancel := context.WithTimeout(ctx, RenderProducingTimeout)
	defer renderCancel()

	// Subscribe FIRST, then re-check the store, to close the race
	// between subscribe and a concurrent AddObject.
	arrived := make(chan struct{}, 1)
	unsub := w.Store.AddListener(store.EventObjectAdded, func(other manifest.NamedResource, _ any) {
		if other != id {
			return
		}
		select {
		case arrived <- struct{}{}:
		default:
		}
	}, false)
	defer unsub()
	if w.depExists(id) {
		slog.Debug("depwait: render-only dependency already exists", "parent", w.Parent.String(), "dep", id.String(), "duration", time.Since(started))
		return nil
	}

	// quiesce is a one-shot signal channel. A nil channel selects
	// never, so embedders without a Renders wired (legacy callers)
	// fall back to "wait for arrived or ctx" exactly as before.
	var quiesce <-chan struct{}
	if w.Renders != nil {
		quiesce = w.Renders.QuiescenceCh()
	}

	for {
		select {
		case <-arrived:
			slog.Debug("depwait: render-only dependency arrived", "parent", w.Parent.String(), "dep", id.String(), "duration", time.Since(started))
			return nil
		case <-quiesce:
			// Quiescence: no other reconcile in the pool is doing
			// work. Re-check arrived once in case the AddObject
			// landed in the same scheduler tick as the drain; if
			// the dep isn't here, no future emission can produce it.
			if w.depExists(id) {
				slog.Debug("depwait: render-only dependency arrived at quiescence", "parent", w.Parent.String(), "dep", id.String(), "duration", time.Since(started))
				return nil
			}
			slog.Debug("depwait: render-only dependency drained", "parent", w.Parent.String(), "dep", id.String(), "duration", time.Since(started))
			return errRenderDrained
		case <-renderCtx.Done():
			// Distinguish parent-canceled (caller wants to abort)
			// from "deadline elapsed without the dep arriving"
			// (whether the deadline came from the per-dep Timeout
			// wrapper or from the inner RenderProducingTimeout cap).
			// Caller-driven Cancel must propagate so the wait
			// classifies as DepCancelled; any deadline becomes a
			// "render couldn't produce" fast-fail.
			if errors.Is(ctx.Err(), context.Canceled) {
				slog.Debug("depwait: render-only dependency cancelled", "parent", w.Parent.String(), "dep", id.String(), "duration", time.Since(started))
				return ctx.Err()
			}
			slog.Debug("depwait: render-only dependency cap reached", "parent", w.Parent.String(), "dep", id.String(), "duration", time.Since(started))
			return errRenderCapExceeded
		}
	}
}

func classify(dep manifest.NamedResource, err error, fallback string) Event {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return Event{Dep: dep, Status: DepTimeout, Reason: "timeout"}
	case errors.Is(err, context.Canceled):
		return Event{Dep: dep, Status: DepCancelled, Reason: "cancelled"}
	}
	if rfe, ok := errors.AsType[*manifest.ResourceFailedError](err); ok {
		return Event{Dep: dep, Status: DepFailed, Reason: cmp.Or(rfe.Reason, fallback)}
	}
	if err != nil {
		return Event{Dep: dep, Status: DepFailed, Reason: err.Error()}
	}
	return Event{Dep: dep, Status: DepReady, Reason: fallback}
}

// WaitAll consumes the channel returned by Watch and produces a Summary.
// First-failure cancellation is not performed automatically — callers
// that want it should drive Watch directly.
func WaitAll(ch <-chan Event) Summary {
	s := Summary{Messages: make(map[manifest.NamedResource]string)}
	for ev := range ch {
		s.Messages[ev.Dep] = ev.Reason
		switch ev.Status {
		case DepReady:
			s.Ready = append(s.Ready, ev.Dep)
		case DepFailed, DepTimeout, DepCancelled:
			s.Failed = append(s.Failed, ev.Dep)
		}
	}
	return s
}
