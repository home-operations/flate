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
