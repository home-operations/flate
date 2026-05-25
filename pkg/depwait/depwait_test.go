package depwait

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// refs wraps NamedResources as bare DependencyRefs (no ReadyExpr)
// so test cases keep their original shape.
func refs(ids ...manifest.NamedResource) []manifest.DependencyRef {
	out := make([]manifest.DependencyRef, len(ids))
	for i, id := range ids {
		out[i] = manifest.DependencyRef{NamedResource: id}
	}
	return out
}

func TestWaiter_AllReady(t *testing.T) {
	s := store.New()
	dep1 := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "a"}
	dep2 := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "b"}
	s.UpdateStatus(dep1, store.StatusReady, "")
	s.UpdateStatus(dep2, store.StatusReady, "")

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), refs(dep1, dep2)))
	if !sum.AllReady() {
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
	sum := WaitAll(w.Watch(context.Background(), refs(dep1, dep2)))
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
	sum := WaitAll(w.Watch(context.Background(), refs(id)))
	if !sum.AllReady() {
		t.Errorf("expected ConfigMap to become ready: %+v", sum)
	}
}

func TestWaiter_Timeout(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "absent"}
	w := &Waiter{Store: s, Timeout: 20 * time.Millisecond}
	sum := WaitAll(w.Watch(context.Background(), refs(dep)))
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
	sum := WaitAll(w.Watch(context.Background(), refs(dep)))
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

	w := &Waiter{Store: s, Timeout: 5 * time.Second, ResolveMissing: resolve}
	sum := WaitAll(w.Watch(context.Background(), refs(dep)))

	if !resolveCalled {
		t.Errorf("ResolveMissing was never invoked")
	}
	if !sum.AllReady() {
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
	w := &Waiter{Store: s, Timeout: 5 * time.Second, ResolveMissing: resolve}

	sum := WaitAll(w.Watch(context.Background(), refs(dep)))
	if !sum.AnyFailed() {
		t.Errorf("ResolveMissing=false should still fail: %+v", sum)
	}
}

// TestWaiter_RenderOnlyDepWaitsBeyondGrace covers the
// chained-render race documented in the docstring on Waiter.IsKnown:
// a HelmRelease emitted by a render-only leaf KS (components/ks/app
// + APP-app-vars replacement pattern) can land in the Store seconds
// AFTER the consuming HR's depwait started — well past the 2s
// MissingGrace. Before IsKnown was wired, depwait would fast-fail
// at the grace boundary even though the dep was actively being
// produced. With IsKnown(id)=false telling depwait "no file record,
// only render emission can produce this", the Waiter keeps watching
// on the per-dep ctx and clears the moment the dep lands.
func TestWaiter_RenderOnlyDepWaitsBeyondGrace(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "network", Name: "omada-controller"}

	resolve := func(manifest.NamedResource) bool { return false }
	isKnown := func(manifest.NamedResource) bool { return false }

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
		Store:          s,
		Timeout:        MissingGrace + 5*time.Second,
		ResolveMissing: resolve,
		IsKnown:        isKnown,
	}
	sum := WaitAll(w.Watch(context.Background(), refs(dep)))
	if !sum.AllReady() {
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
	isKnown := func(manifest.NamedResource) bool { return false }

	// Use a tight Timeout so the test doesn't burn 30s. Add a
	// meaningful gap (300ms) so the assertion that elapsed >=
	// MissingGrace + 100ms actually pins the long-wait branch.
	w := &Waiter{
		Store:          s,
		Timeout:        MissingGrace + 300*time.Millisecond,
		ResolveMissing: resolve,
		IsKnown:        isKnown,
	}
	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), refs(dep)))
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
// docstring's "IsKnown(id) == true and ResolveMissing(id) == false"
// branch: the dep is file-indexed but promote failed (parse error,
// file mutated since record). depwait must fail fast at the grace
// boundary — keeping the legacy "typo / broken file" UX where a
// truly-missing dep doesn't burn the full per-dep budget.
func TestWaiter_FileIndexedPromoteFailsFastAtGrace(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "broken"}

	// Both wirings: ResolveMissing fails (promote unhappy), IsKnown
	// returns true (file-indexed). depwait should NOT enter step-2.
	resolve := func(manifest.NamedResource) bool { return false }
	isKnown := func(manifest.NamedResource) bool { return true }

	w := &Waiter{
		Store:          s,
		Timeout:        30 * time.Second, // generous; if step-2 ran we'd see it
		ResolveMissing: resolve,
		IsKnown:        isKnown,
	}
	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), refs(dep)))
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
	isKnown := func(manifest.NamedResource) bool { return false }

	w := &Waiter{
		Store:          s,
		Timeout:        30 * time.Second,
		ResolveMissing: resolve,
		IsKnown:        isKnown,
	}

	// Cancel after grace expires + 500ms — well into step-2's wait.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(MissingGrace + 500*time.Millisecond)
		cancel()
	}()

	sum := WaitAll(w.Watch(ctx, refs(dep)))
	// classify() maps context.Canceled to DepCancelled / "cancelled".
	if got := sum.Messages[dep]; got != "cancelled" {
		t.Errorf("expected 'cancelled' after step-2 ctx cancel, got %q", got)
	}
}

func TestWaiter_NoDeps(t *testing.T) {
	w := &Waiter{Store: store.New()}
	sum := WaitAll(w.Watch(context.Background(), nil))
	if !sum.AllReady() {
		t.Errorf("expected vacuous ready: %+v", sum)
	}
}

// TestWaiter_PanicReportedAsFailed asserts that a panic in watchOne
// (here triggered by a nil Store) is caught and reported as a failed
// Event instead of killing the orchestrator.
func TestWaiter_PanicReportedAsFailed(t *testing.T) {
	w := &Waiter{} // Store nil — depExists will nil-deref
	dep := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "x"}
	sum := WaitAll(w.Watch(context.Background(), refs(dep)))
	if !sum.AnyFailed() {
		t.Fatalf("expected fail on panic: %+v", sum)
	}
	if msg := sum.Messages[dep]; !strings.Contains(msg, "depwait panic:") {
		t.Errorf("expected 'depwait panic:' prefix, got %q", msg)
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
