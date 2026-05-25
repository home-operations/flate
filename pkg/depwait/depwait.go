// Package depwait blocks a controller until a set of NamedResource
// dependencies become Ready (or, for kinds without a status pipeline,
// merely exist) — with per-dep timeouts and fail-fast for missing refs.
package depwait

import (
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

	// ResolveMissing, when non-nil, is consulted after the
	// missing-dep grace window elapses and the dep is still absent
	// from the Store. A true return means the resolver has (or can)
	// promote the dep into the Store — the wait then continues
	// instead of failing. Used by the orchestrator to lazy-load
	// file-indexed objects (CMs/Secrets/HRs the DiscoveryOnly
	// loader kept in Existence) the moment a depwait edge needs
	// them. Without this hook, the render-driven loader breaks the
	// bjw-s parent-patches pattern where a KS's substituteFrom CM
	// lives inside that KS's own spec.path: the dep would never
	// clear because the producing render hasn't run yet.
	ResolveMissing func(manifest.NamedResource) bool
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
			// Final fallback: ask the resolver to materialize the
			// dep from the file-indexed Existence map. A true
			// return means the dep is now in the Store and the
			// wait can continue against built-in status / exists
			// semantics below; false confirms it really is missing.
			if w.ResolveMissing == nil || !w.ResolveMissing(id) {
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

func classify(dep manifest.NamedResource, err error, fallback string) Event {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return Event{Dep: dep, Status: DepTimeout, Reason: "timeout"}
	case errors.Is(err, context.Canceled):
		return Event{Dep: dep, Status: DepCancelled, Reason: "cancelled"}
	}
	var rfe *manifest.ResourceFailedError
	if errors.As(err, &rfe) {
		reason := rfe.Reason
		if reason == "" {
			reason = fallback
		}
		return Event{Dep: dep, Status: DepFailed, Reason: reason}
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
