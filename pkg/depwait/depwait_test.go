package depwait

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// lookupStub satisfies ExistenceLookup from inline closures so
// individual tests can express both halves of the contract without
// declaring a custom type per case.
type lookupStub struct {
	promote     func(manifest.NamedResource) bool
	fileIndexed func(manifest.NamedResource) bool
}

func (l lookupStub) Promote(id manifest.NamedResource) bool {
	if l.promote == nil {
		return false
	}
	return l.promote(id)
}

func (l lookupStub) IsFileIndexed(id manifest.NamedResource) bool {
	if l.fileIndexed == nil {
		return false
	}
	return l.fileIndexed(id)
}

// rendersStub satisfies RenderInflight from inline closures so tests
// can pin the QuiescenceCh behavior precisely.
type rendersStub struct {
	otherActive func() bool
	quiesce     func() <-chan struct{}
}

// QuiescenceCh returns a closed channel when otherActive returns
// false (drained), nil when otherActive returns true (active), and
// delegates to quiesce when explicitly set. Returning nil exercises
// the "fall back to ctx deadline" path in waitRenderEmission.
func (r rendersStub) QuiescenceCh() <-chan struct{} {
	if r.quiesce != nil {
		return r.quiesce()
	}
	if r.otherActive != nil && !r.otherActive() {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return nil
}

func TestWaiter_AllReady(t *testing.T) {
	dep1 := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "a"}
	dep2 := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "b"}
	s := testutil.NewStoreWithStatuses(map[manifest.NamedResource]store.Status{
		dep1: store.StatusReady,
		dep2: store.StatusReady,
	})

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep1, dep2)))
	if sum.AnyFailed() {
		t.Errorf("expected all ready: %+v", sum)
	}
}

func TestWaiter_OneFails(t *testing.T) {
	s := store.New()
	dep1 := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "a"}
	dep2 := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "b"}
	s.UpdateStatus(dep1, store.StatusReady, "")
	s.UpdateStatus(dep2, store.StatusFailed, "denied")

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep1, dep2)))
	if !sum.AnyFailed() {
		t.Errorf("expected failure: %+v", sum)
	}
	if sum.Messages[dep2] != "denied" {
		t.Errorf("missing reason: %q", sum.Messages[dep2])
	}
}

func TestWaiter_Exists_NonStatusKind(t *testing.T) {
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "cm"}
	go s.AddObject(&manifest.ConfigMap{Name: "cm", Namespace: "ns"})

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(id)))
	if sum.AnyFailed() {
		t.Errorf("expected ConfigMap to become ready: %+v", sum)
	}
}

func TestWaiter_Timeout(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "absent"}
	w := &Waiter{Store: s, Timeout: 20 * time.Millisecond}
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if !sum.AnyFailed() {
		t.Errorf("expected timeout failure: %+v", sum)
	}
}

func TestWaiter_MissingDepFailsFast(t *testing.T) {
	// A dependency that never appears in the store should fail well
	// before the per-dep Timeout — the missing-grace window covers
	// late-arriving render output but won't wait for typos.
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "ghost"}
	w := &Waiter{Store: s, Timeout: 30 * time.Second}

	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	elapsed := time.Since(start)

	if !sum.AnyFailed() {
		t.Fatalf("expected fail for missing dep: %+v", sum)
	}
	if elapsed > MissingGrace+500*time.Millisecond {
		t.Errorf("missing-grace exceeded: %s (cap %s)", elapsed, MissingGrace)
	}
	if got := sum.Messages[dep]; got != "dependency not found" {
		t.Errorf("expected 'dependency not found', got %q", got)
	}
}

// TestWaiter_ResolveMissingLazyPromotes covers the step-4 fallback:
// when the missing-grace expires and the dep is still absent, the
// Waiter consults ResolveMissing. A true return (closure has
// promoted the object into the Store) means the wait continues
// against built-in existence semantics instead of failing.
// Mirrors the orchestrator's wiring where ResolveMissing closes
// over the loader.ExistenceIndex's Promote.
func TestWaiter_ResolveMissingLazyPromotes(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "settings"}

	resolveCalled := false
	resolve := func(id manifest.NamedResource) bool {
		resolveCalled = true
		if id != dep {
			t.Errorf("resolver called with %+v, want %+v", id, dep)
			return false
		}
		// Simulate lazy promotion: object lands in the Store as a
		// side effect, which is how loader.Promote works.
		s.AddObject(&manifest.ConfigMap{Name: dep.Name, Namespace: dep.Namespace})
		return true
	}

	w := &Waiter{Store: s, Timeout: 5 * time.Second, Existence: lookupStub{promote: resolve}}
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))

	if !resolveCalled {
		t.Errorf("Existence.Promote was never invoked")
	}
	if sum.AnyFailed() {
		t.Errorf("expected dep to clear after lazy promotion: %+v", sum)
	}
}

// TestWaiter_ResolveMissingFalseStillFails locks the symmetric
// contract: a false return from ResolveMissing means the dep
// genuinely doesn't exist (no Existence index entry) and the wait
// surfaces the same "dependency not found" failure as if no
// resolver were configured.
func TestWaiter_ResolveMissingFalseStillFails(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "ghost"}
	resolve := func(manifest.NamedResource) bool { return false }
	w := &Waiter{Store: s, Timeout: 5 * time.Second, Existence: lookupStub{promote: resolve}}

	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if !sum.AnyFailed() {
		t.Errorf("Existence.Promote=false should still fail: %+v", sum)
	}
}

// TestWaiter_RenderOnlyDepWaitsBeyondGrace covers the
// chained-render race documented in the docstring on Waiter.IsFileIndexed:
// a HelmRelease emitted by a render-only leaf KS (components/ks/app
// + APP-app-vars replacement pattern) can land in the Store seconds
// AFTER the consuming HR's depwait started — well past the 2s
// MissingGrace. Before IsFileIndexed was wired, depwait would fast-fail
// at the grace boundary even though the dep was actively being
// produced. With IsFileIndexed(id)=false telling depwait "no file record,
// only render emission can produce this", the Waiter keeps watching
// on the per-dep ctx and clears the moment the dep lands.
func TestWaiter_RenderOnlyDepWaitsBeyondGrace(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "network", Name: "omada-controller"}

	resolve := func(manifest.NamedResource) bool { return false }
	isFileIndexed := func(manifest.NamedResource) bool { return false }

	// Add the dep AFTER MissingGrace expires but well before the
	// per-dep Timeout. Mark it Ready so the regular Ready wait
	// (which runs after we fall through the missing-dep path)
	// completes.
	go func() {
		time.Sleep(MissingGrace + 500*time.Millisecond)
		s.AddObject(&manifest.HelmRelease{Name: dep.Name, Namespace: dep.Namespace})
		s.UpdateStatus(dep, store.StatusReady, "")
	}()

	w := &Waiter{
		Store:     s,
		Timeout:   MissingGrace + 5*time.Second,
		Existence: lookupStub{promote: resolve, fileIndexed: isFileIndexed},
	}
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if sum.AnyFailed() {
		t.Errorf("render-only dep that arrived after grace should clear, not fail: %+v", sum)
	}
}

// TestWaiter_RenderOnlyDepStillFailsAfterFullTimeout pins the
// terminal case: a render-only dep that NEVER appears (typo'd
// dependsOn, deleted resource, or a producing chain that itself
// failed) must still surface as "dependency not found" — just at
// the per-dep Timeout instead of MissingGrace. The error message
// stays the same so users grep for it the same way.
func TestWaiter_RenderOnlyDepStillFailsAfterFullTimeout(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "network", Name: "never-arrives"}

	resolve := func(manifest.NamedResource) bool { return false }
	isFileIndexed := func(manifest.NamedResource) bool { return false }

	// Use a tight Timeout so the test doesn't burn 30s. Add a
	// meaningful gap (300ms) so the assertion that elapsed >=
	// MissingGrace + 100ms actually pins the long-wait branch.
	w := &Waiter{
		Store:     s,
		Timeout:   MissingGrace + 300*time.Millisecond,
		Existence: lookupStub{promote: resolve, fileIndexed: isFileIndexed},
	}
	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	elapsed := time.Since(start)

	if !sum.AnyFailed() {
		t.Fatalf("expected fail for never-appearing dep: %+v", sum)
	}
	// Strictly past MissingGrace, with a 100ms cushion so a
	// future regression that reverted step-2 to grace-boundary
	// fast-fail wouldn't accidentally still satisfy elapsed >=
	// MissingGrace.
	if elapsed < MissingGrace+100*time.Millisecond {
		t.Errorf("wait returned at or near grace boundary, not exercising step-2: elapsed=%s, threshold=%s",
			elapsed, MissingGrace+100*time.Millisecond)
	}
	if got := sum.Messages[dep]; got != "dependency not found" {
		t.Errorf("expected 'dependency not found', got %q", got)
	}
}

// TestWaiter_FileIndexedPromoteFailsFastAtGrace pins the
// docstring's "IsFileIndexed(id) == true and ResolveMissing(id) == false"
// branch: the dep is file-indexed but promote failed (parse error,
// file mutated since record). depwait must fail fast at the grace
// boundary — keeping the legacy "typo / broken file" UX where a
// truly-missing dep doesn't burn the full per-dep budget.
func TestWaiter_FileIndexedPromoteFailsFastAtGrace(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "broken"}

	// Both wirings: ResolveMissing fails (promote unhappy), IsFileIndexed
	// returns true (file-indexed). depwait should NOT enter step-2.
	resolve := func(manifest.NamedResource) bool { return false }
	isFileIndexed := func(manifest.NamedResource) bool { return true }

	w := &Waiter{
		Store:     s,
		Timeout:   30 * time.Second, // generous; if step-2 ran we'd see it
		Existence: lookupStub{promote: resolve, fileIndexed: isFileIndexed},
	}
	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	elapsed := time.Since(start)

	if !sum.AnyFailed() {
		t.Fatalf("expected fail for broken file-indexed dep: %+v", sum)
	}
	if elapsed > MissingGrace+500*time.Millisecond {
		t.Errorf("file-indexed-but-promote-failed should fast-fail at grace; elapsed=%s, cap=%s",
			elapsed, MissingGrace+500*time.Millisecond)
	}
	if got := sum.Messages[dep]; got != "dependency not found" {
		t.Errorf("expected 'dependency not found', got %q", got)
	}
}

// TestWaiter_RenderOnlyCappedAtRenderProducingTimeout pins the
// step-2 budget cap: when a render-only dep never appears AND the
// per-dep Timeout is much larger than RenderProducingTimeout, the
// wait must end at the cap, not at the full per-dep budget. Before
// the cap landed, a typo'd dependsOn (IsFileIndexed returns false the
// same as a render-only dep) burned the full per-dep Timeout —
// ~30s instead of ~RenderProducingTimeout.
//
// With Existence wired, MissingGrace is skipped entirely — Promote
// is synchronous so there's nothing to wait for at the grace
// boundary. The cap thus equals RenderProducingTimeout (not
// MissingGrace + RenderProducingTimeout).
func TestWaiter_RenderOnlyCappedAtRenderProducingTimeout(t *testing.T) {
	// Shrink the cap so the test doesn't burn 10s.
	old := RenderProducingTimeout
	RenderProducingTimeout = 500 * time.Millisecond
	defer func() { RenderProducingTimeout = old }()

	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "network", Name: "typo-or-broken"}

	resolve := func(manifest.NamedResource) bool { return false }
	isFileIndexed := func(manifest.NamedResource) bool { return false }

	w := &Waiter{
		Store:     s,
		Timeout:   30 * time.Second, // huge — render cap should win
		Existence: lookupStub{promote: resolve, fileIndexed: isFileIndexed},
	}
	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	elapsed := time.Since(start)

	if !sum.AnyFailed() {
		t.Fatalf("expected fail for capped render-only dep: %+v", sum)
	}
	// elapsed should be approximately RenderProducingTimeout (grace
	// is skipped when Existence is wired). Allow generous slack for
	// slow CI but reject the uncapped 30s case.
	upperBound := RenderProducingTimeout + 1*time.Second
	if elapsed > upperBound {
		t.Errorf("step-2 cap not enforced: elapsed=%s, upper bound=%s", elapsed, upperBound)
	}
	if elapsed < RenderProducingTimeout-100*time.Millisecond {
		t.Errorf("step-2 returned before cap: elapsed=%s, lower bound=%s",
			elapsed, RenderProducingTimeout-100*time.Millisecond)
	}
	if got := sum.Messages[dep]; got != "dependency not found" {
		t.Errorf("expected 'dependency not found', got %q", got)
	}
}

// TestWaiter_RenderInflightDrainShortCircuits pins the
// RenderInflight quiescence path: when no other reconcile is in
// flight, depwait's step-2 long wait must NOT burn the full
// RenderProducingTimeout cap. It detects "no more emissions can
// produce this dep" via Renders.OtherActive() and fails fast.
//
// The realistic scenario this defends: a typo'd dependsOn at the
// end of a reconcile pass. The orchestrator drains all real work
// (active count goes to 1 = just the depwait's own task), depwait
// observes quiescence on its next poll, and returns
// "dependency not found" within ~100ms — instead of waiting the
// full cap.
func TestWaiter_RenderInflightDrainShortCircuits(t *testing.T) {
	// Tighten the cap and grace so the test runs fast; the win we
	// pin here is "ended well before the cap fired", which only
	// works if the cap is large enough that the RenderInflight
	// path is the actual termination signal.
	oldCap := RenderProducingTimeout
	RenderProducingTimeout = 10 * time.Second
	defer func() { RenderProducingTimeout = oldCap }()

	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "network", Name: "typo-or-gone"}

	w := &Waiter{
		Store:   s,
		Timeout: 30 * time.Second,
		Existence: lookupStub{
			promote:     func(manifest.NamedResource) bool { return false },
			fileIndexed: func(manifest.NamedResource) bool { return false },
		},
		Renders: rendersStub{
			otherActive: func() bool { return false }, // quiescent
		},
	}

	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	elapsed := time.Since(start)

	if !sum.AnyFailed() {
		t.Fatalf("expected fail when no other renders active: %+v", sum)
	}
	// Must end well before the RenderProducingTimeout cap. We
	// allow grace + a few poll intervals; anything past 1s past
	// grace means the drain check didn't fire.
	upper := MissingGrace + 1*time.Second
	if elapsed > upper {
		t.Errorf("drain short-circuit didn't fire: elapsed=%s, upper=%s", elapsed, upper)
	}
	if got := sum.Messages[dep]; got != "dependency not found" {
		t.Errorf("expected 'dependency not found', got %q", got)
	}
}

// TestWaiter_RenderInflightActiveHoldsTheWait pins the inverse:
// while Renders.OtherActive() returns true, depwait keeps waiting
// past the grace boundary for the dep to land. We flip the signal
// to false after the dep has already arrived; the wait should
// return via the subscription, not via the drain.
func TestWaiter_RenderInflightActiveHoldsTheWait(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "network", Name: "produced-late"}

	// Renders is "active" throughout; the only thing that ends the
	// wait is the dep arriving via AddObject.
	w := &Waiter{
		Store:   s,
		Timeout: MissingGrace + 5*time.Second,
		Existence: lookupStub{
			promote:     func(manifest.NamedResource) bool { return false },
			fileIndexed: func(manifest.NamedResource) bool { return false },
		},
		Renders: rendersStub{
			otherActive: func() bool { return true },
		},
	}

	go func() {
		time.Sleep(MissingGrace + 500*time.Millisecond)
		s.AddObject(&manifest.HelmRelease{Name: dep.Name, Namespace: dep.Namespace})
		s.UpdateStatus(dep, store.StatusReady, "")
	}()

	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if sum.AnyFailed() {
		t.Errorf("dep arrived after grace with Renders active; expected Ready: %+v", sum)
	}
}

// TestWaiter_RenderOnlyCancelDuringLongWaitSurfacesCancelled
// pins the classify() routing in step-2's terminal error path: a
// parent ctx cancellation mid-long-wait must NOT be flattened into
// "dependency not found". orchestrator shutdown should propagate
// the cancel as DepCancelled / "cancelled" so logs and Summary
// counters stay accurate. Without the routing, the cancel was
// previously silently mapped to DepFailed / "dependency not found".
func TestWaiter_RenderOnlyCancelDuringLongWaitSurfacesCancelled(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "network", Name: "cancelled-mid-wait"}

	resolve := func(manifest.NamedResource) bool { return false }
	isFileIndexed := func(manifest.NamedResource) bool { return false }

	w := &Waiter{
		Store:     s,
		Timeout:   30 * time.Second,
		Existence: lookupStub{promote: resolve, fileIndexed: isFileIndexed},
	}

	// Cancel after grace expires + 500ms — well into step-2's wait.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(MissingGrace + 500*time.Millisecond)
		cancel()
	}()

	sum := WaitAll(w.Watch(ctx, testutil.DepRefs(dep)))
	// classify() maps context.Canceled to DepCancelled / "cancelled".
	if got := sum.Messages[dep]; got != "cancelled" {
		t.Errorf("expected 'cancelled' after step-2 ctx cancel, got %q", got)
	}
}

func TestWaiter_ReadyExprCancelSurfacesCancelled(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "network", Name: "expr-cancelled"}
	s.AddObject(&manifest.Kustomization{Name: dep.Name, Namespace: dep.Namespace})
	s.UpdateStatus(dep, store.StatusPending, "waiting")

	w := &Waiter{Store: s, Timeout: 30 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sum := WaitAll(w.Watch(ctx, []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     "false",
	}}))
	if got := sum.Messages[dep]; got != "cancelled" {
		t.Errorf("expected readyExpr cancellation to report 'cancelled', got %q", got)
	}
}

func TestWaiter_NoDeps(t *testing.T) {
	w := &Waiter{Store: store.New()}
	sum := WaitAll(w.Watch(context.Background(), nil))
	if sum.AnyFailed() {
		t.Errorf("expected vacuous ready: %+v", sum)
	}
}

// TestWaiter_PanicReportedAsFailed asserts that a panic in watchOne
// (here triggered by a nil Store) is caught and reported as a failed
// Event instead of killing the orchestrator.
func TestWaiter_PanicReportedAsFailed(t *testing.T) {
	w := &Waiter{} // Store nil — depExists will nil-deref
	dep := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "x"}
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if !sum.AnyFailed() {
		t.Fatalf("expected fail on panic: %+v", sum)
	}
	if msg := sum.Messages[dep]; !strings.Contains(msg, "depwait panic:") {
		t.Errorf("expected 'depwait panic:' prefix, got %q", msg)
	}
}

// TestWaiter_ReadyExprWakesOnObjectAdded pins the EventObjectAdded
// subscription added alongside EventStatusUpdated in watchReadyExpr.
// A CEL expression like `dep.metadata.labels['ready'] == 'true'`
// depends on labels on the OBJECT, not on its status. If the labels
// land via AddObject without a paired SetCondition, the watcher
// previously missed the event and wedged until the per-dep timeout.
//
// We reproduce the wedge condition by adding the dep with the
// required label fields AFTER the watcher subscribes, with no
// status update at any point; the wait must return DepReady well
// before the timeout.
func TestWaiter_ReadyExprWakesOnObjectAdded(t *testing.T) {
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "r"}

	w := &Waiter{Store: s, Timeout: 3 * time.Second}

	// Pre-seed status so the existence grace short-circuits and the
	// watcher enters watchReadyExpr immediately. Status is Pending,
	// so the built-in WatchReady would block — but the ReadyExpr
	// path replaces that check.
	s.UpdateStatus(id, store.StatusPending, "")

	// Use a has()-guarded expression so the initial eval against a
	// status-only projection (no object yet) doesn't error on the
	// missing labels map — it returns false and the watcher blocks
	// waiting for an event. The bug being pinned is that the wake
	// MUST come via EventObjectAdded (the AddObject below), since
	// no SetCondition fires.
	dep := manifest.DependencyRef{
		NamedResource: id,
		ReadyExpr:     `has(dep.metadata.labels) && dep.metadata.labels['ready'] == 'true'`,
	}

	// Start the wait, then asynchronously AddObject with the
	// expected label after a short delay. No SetCondition fires —
	// the only wake source is EventObjectAdded.
	done := make(chan Summary, 1)
	go func() {
		done <- WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{dep}))
	}()
	// Sleep long enough that the watcher has subscribed; longer than
	// scheduler jitter, much shorter than the per-dep timeout.
	time.Sleep(50 * time.Millisecond)
	hr := &manifest.HelmRelease{Name: id.Name, Namespace: id.Namespace}
	hr.Labels = map[string]string{"ready": "true"}
	s.AddObject(hr)

	select {
	case sum := <-done:
		if sum.AnyFailed() {
			t.Fatalf("expected ready; got %+v", sum)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("watchReadyExpr wedged — missed EventObjectAdded wake")
	}
}

// TestCompileReadyExpr_MemoizesErrors pins the compile-error cache:
// a known-bad expression should not recompile (and re-error) on
// every fire. We compile twice and assert both calls return the
// same error instance — sync.Map.LoadOrStore guarantees pointer
// identity for the cached entry.
func TestCompileReadyExpr_MemoizesErrors(t *testing.T) {
	const bad = `this is not valid CEL ((` // unclosed paren = compile error
	_, err1 := compileReadyExpr(bad)
	if err1 == nil {
		t.Fatal("expected compile error")
	}
	_, err2 := compileReadyExpr(bad)
	if err2 == nil {
		t.Fatal("expected error on second call")
	}
	if err1 != err2 {
		t.Errorf("compile error not memoized: %p vs %p", err1, err2)
	}
}

// TimeoutFromSpec mirrors Flux KS/HR's `*metav1.Duration` shape: nil
// and zero fall back to DefaultTimeout; user-supplied values win.
func TestTimeoutFromSpec(t *testing.T) {
	if got := TimeoutFromSpec(nil); got != DefaultTimeout {
		t.Errorf("nil → %v, want DefaultTimeout (%v)", got, DefaultTimeout)
	}
	zero := &metav1.Duration{Duration: 0}
	if got := TimeoutFromSpec(zero); got != DefaultTimeout {
		t.Errorf("zero → %v, want DefaultTimeout", got)
	}
	custom := &metav1.Duration{Duration: 5 * time.Minute}
	if got := TimeoutFromSpec(custom); got != 5*time.Minute {
		t.Errorf("5m → %v", got)
	}
}
