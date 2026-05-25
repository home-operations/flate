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

// DepStatus enumerates the per-dependency resolution result.
type DepStatus int

// Possible DepStatus values.
const (
	DepPending DepStatus = iota
	DepReady
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

// Summary tallies the final state of a Waiter run.
type Summary struct {
	Ready    []manifest.NamedResource
	Failed   []manifest.NamedResource
	Pending  []manifest.NamedResource
	Messages map[manifest.NamedResource]string
}

// AllReady reports whether every dependency reached DepReady.
func (s Summary) AllReady() bool { return len(s.Failed) == 0 && len(s.Pending) == 0 }

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

// safeWatchOne wraps watchOne with panic recovery — a malformed CEL
// ReadyExpr (or any other internal bug) reports the dep as failed
// instead of taking the orchestrator down with it.
func safeWatchOne(ctx context.Context, w *Waiter, dep manifest.DependencyRef, timeout time.Duration) (ev Event) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("depwait: panic in watchOne", "dep", dep.String(), "panic", r)
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

	// Fail fast on deps that never made it into the store: wait a
	// short grace window for a late-arriving render output, then
	// surface a clear error instead of timing out at the full
	// per-dep budget. We treat "object exists" OR "status known" as
	// proof of presence — the latter covers controllers that update
	// status before AddObject (and unit tests that set status only).
	if !w.depExists(id) {
		graceCtx, graceCancel := context.WithTimeout(ctx, MissingGrace)
		_, err := w.Store.WatchExists(graceCtx, id)
		graceCancel()
		if err != nil && !w.depExists(id) {
			// Step 1: ask the existence lookup to materialize the
			// dep from the file-indexed Existence map. A true return
			// means the dep is now in the Store and the wait can
			// continue against built-in status / exists semantics
			// below.
			switch {
			case w.Existence != nil && w.Existence.Promote(id):
				// promoted; fall through to the regular wait.
			case w.Existence != nil && !w.Existence.IsFileIndexed(id):
				// Step 2: dep has no file record — it can only arrive
				// via a parent render's emitRenderedChildren chain.
				// Two complementary signals bound the wait:
				//
				//   - RenderProducingTimeout caps the absolute wall
				//     time so a missing Renders signal doesn't burn
				//     the full per-dep Timeout. Legacy embedders
				//     without a task pool get this cap as the only
				//     bound.
				//   - Renders.OtherActive() short-circuits the wait
				//     once no other reconcile in the pool is doing
				//     work — at that point, no future render can
				//     produce the missing dep. Typo'd dependsOn
				//     fails as soon as the orchestrator drains
				//     instead of waiting the full cap.
				//
				// On a real chain (the omada-controller-style
				// component+replacement render), other reconciles
				// stay active while the producing chain runs, so
				// OtherActive returns true and the wait holds open
				// until the dep arrives via subscription.
				if err := w.waitRenderEmission(ctx, id); err != nil {
					switch {
					case errors.Is(ctx.Err(), context.Canceled):
						return classify(id, ctx.Err(), "")
					case errors.Is(err, context.DeadlineExceeded), errors.Is(err, errRenderDrained):
						return Event{Dep: id, Status: DepFailed, Reason: "dependency not found"}
					default:
						return Event{Dep: id, Status: DepFailed, Reason: err.Error()}
					}
				}
			default:
				// Step 3: either no Existence wired (legacy callers
				// can't distinguish render-only from typo) or the
				// dep is file-indexed but Promote failed (file
				// disappeared / parse error). Either way the dep is
				// not coming — fail fast at the grace boundary.
				return Event{Dep: id, Status: DepFailed, Reason: "dependency not found"}
			}
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

	info, err := w.Store.WatchReady(ctx, id)
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

// watchReadyExpr evaluates expr against id's projected state and
// returns DepReady when it produces true. On false / eval error it
// blocks on the next EventStatusUpdated for id and re-evaluates.
// Surfaces the parent ctx's timeout/cancel.
//
// The eval pattern runs three times — pre-subscribe (initial),
// post-subscribe (race window between subscribe and live event),
// and on each fire — so the evaluate-and-translate-to-Event step is
// extracted into tryReadyExpr to keep the control flow readable.
func (w *Waiter) watchReadyExpr(ctx context.Context, id manifest.NamedResource, expr string) Event {
	if ev, done := w.tryReadyExpr(expr, id); done {
		return ev
	}

	// Subscribe AFTER the initial eval, then re-check — closes the
	// race where id flipped Ready between the first eval and the
	// subscribe (we'd otherwise miss the EventStatusUpdated and
	// block on the channel forever).
	ch := make(chan struct{}, 1)
	unsub := w.Store.AddListener(store.EventStatusUpdated, func(other manifest.NamedResource, _ any) {
		if other != id {
			return
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	}, false)
	defer unsub()
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
			return Event{Dep: id, Status: DepTimeout, Reason: "readyExpr timeout: " + ctx.Err().Error()}
		}
	}
}

// tryReadyExpr evaluates expr once and translates the outcome into
// the per-dep Event. Returns (event, true) on a definitive result
// (Ready or eval error); returns (zero, false) when the expression
// produced a clean false and the caller should keep waiting.
func (w *Waiter) tryReadyExpr(expr string, id manifest.NamedResource) (Event, bool) {
	ok, err := evaluateReadyExpr(expr, w.Store, w.Parent, id)
	if err != nil {
		return Event{Dep: id, Status: DepFailed, Reason: "readyExpr: " + err.Error()}, true
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
//   - Renders.OtherActive() flips to false → returns errRenderDrained
//     (no future emission could produce id; caller fails fast).
//   - ctx hits its deadline / cancellation → returns ctx.Err().
//
// An overall cap of RenderProducingTimeout is layered onto ctx so
// the wait can't run past it even when Renders is nil (legacy
// embedders without a task pool).
func (w *Waiter) waitRenderEmission(ctx context.Context, id manifest.NamedResource) error {
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
		return nil
	}

	// Polling cadence for the RenderInflight quiescence check. 100ms
	// keeps drain detection responsive without burning CPU on tight
	// loops; the typo case fails within ~100ms of the orchestrator's
	// last reconcile finishing.
	const pollInterval = 100 * time.Millisecond
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()

	for {
		select {
		case <-arrived:
			return nil
		case <-renderCtx.Done():
			return renderCtx.Err()
		case <-poll.C:
			if w.depExists(id) {
				return nil
			}
			if w.Renders != nil && !w.Renders.OtherActive() {
				return errRenderDrained
			}
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
		default:
			s.Pending = append(s.Pending, ev.Dep)
		}
	}
	return s
}
