package base_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRunWithStatus_Success(t *testing.T) {
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	base.RunWithStatus(t.Context(), s, id, "helmrelease",
		func(_ context.Context, obj *manifest.HelmRelease) error {
			if obj.Name != "app" {
				t.Errorf("re-read got %q, want app", obj.Name)
			}
			return nil
		},
	)
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusReady {
		t.Errorf("status = %v, want Ready", got.Status)
	}
}

func TestRunWithStatus_Failure(t *testing.T) {
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	base.RunWithStatus(t.Context(), s, id, "helmrelease",
		func(_ context.Context, _ *manifest.HelmRelease) error {
			return errors.New("render failed")
		},
	)
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusFailed {
		t.Errorf("status = %v, want Failed", got.Status)
	}
	if got.Message != "render failed" {
		t.Errorf("message = %q, want %q", got.Message, "render failed")
	}
}

func TestRunWithStatus_Panic(t *testing.T) {
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	// Contract: panic is converted into StatusFailed AND re-raised
	// so the enclosing task.Service.Go's recover increments failures
	// (Service.Failures() must count panicked reconciles). Recover
	// the re-raise at the test boundary and assert both pieces.
	var rec any
	func() {
		defer func() { rec = recover() }()
		base.RunWithStatus(t.Context(), s, id, "helmrelease",
			func(_ context.Context, _ *manifest.HelmRelease) error {
				panic("kaboom")
			},
		)
	}()
	if rec == nil {
		t.Fatal("expected panic to be re-raised so task.Service.Go's recover counts it; got nil")
	}
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusFailed {
		t.Errorf("status = %v, want Failed", got.Status)
	}
	if !strings.Contains(got.Message, "panic:") || !strings.Contains(got.Message, "kaboom") {
		t.Errorf("message = %q, want a 'panic: kaboom' summary", got.Message)
	}
}

func TestRunWithStatus_MissingObject(t *testing.T) {
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "ghost"}
	called := false
	base.RunWithStatus(t.Context(), s, id, "helmrelease",
		func(_ context.Context, _ *manifest.HelmRelease) error {
			called = true
			return nil
		},
	)
	if called {
		t.Error("fn ran for a missing object; expected silent no-op")
	}
	if _, ok := s.GetStatus(id); ok {
		t.Error("missing object should not get a status entry")
	}
}

// TestRunWithStatus_PreservesInformativeReadyMessage pins the
// 3.5 fix: when a coalesced re-run returns nil but the current
// status carries an informative Ready message (skipped:, unchanged,
// suspended), the terminal "" Ready write must NOT clobber it. The
// previous unconditional s.UpdateStatus(id, Ready, "") at the end
// of RunWithStatus erased these sub-states whenever a short-circuit
// returned nil after the message had been set.
func TestRunWithStatus_PreservesInformativeReadyMessage(t *testing.T) {
	for _, message := range []string{store.MsgUnchanged, store.MsgSuspended, store.SkippedPrefix + " missing secret"} {
		t.Run(message, func(t *testing.T) {
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
			s.AddObject(hr)
			id := hr.Named()
			s.UpdateStatus(id, store.StatusReady, message)

			base.RunWithStatus(t.Context(), s, id, "helmrelease",
				func(_ context.Context, _ *manifest.HelmRelease) error { return nil },
			)
			got, _ := s.GetStatus(id)
			if got.Status != store.StatusReady {
				t.Errorf("status = %v, want Ready", got.Status)
			}
			if got.Message != message {
				t.Errorf("informative Ready message %q was clobbered: now %q", message, got.Message)
			}
		})
	}
}

func TestRunWithStatus_PreservesExternalFailure(t *testing.T) {
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	base.RunWithStatus(t.Context(), s, id, "helmrelease",
		func(_ context.Context, _ *manifest.HelmRelease) error {
			s.UpdateStatus(id, store.StatusFailed, "dependency cycle detected")
			return nil
		},
	)
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusFailed {
		t.Errorf("status = %v, want Failed", got.Status)
	}
	if got.Message != "dependency cycle detected" {
		t.Errorf("external failure message was clobbered: %q", got.Message)
	}
}

// TestController_CloseDrainsConcurrentAddListener exercises the
// Close-vs-AddListener race. Many goroutines register listeners while
// a single goroutine runs Close. The contract is that exactly ONE
// Close call must leave no listener registered against the store —
// either Close drained the listener directly OR the closed flag
// caused AddListener to refuse / roll back. A listener that registers
// AFTER Close's snapshot but BEFORE Close releases the unsubMu lock
// must NOT leak.
//
// We verify by firing EventObjectAdded AFTER Close returns and
// counting handler invocations; we deliberately do NOT call Close
// twice (a second Close would mask the leak by draining the slice
// that grew between the first Close's snapshot and its lock release).
// Run under -race over many iterations to catch the snapshot-after-
// append window that the closed flag closes.
func TestController_CloseDrainsConcurrentAddListener(t *testing.T) {
	t.Parallel()

	const goroutines = 256

	for iter := range 100 {
		s := store.New()
		c := base.New(s, nil)

		var fired atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		closeDone := make(chan struct{})

		for range goroutines {
			wg.Go(func() {
				<-start
				c.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) {
					fired.Add(1)
				})
			})
		}

		// Release goroutines and run a single Close concurrently. Wait
		// for both the Close and every AddListener to finish before
		// firing the post-close event — that way the test asks "after
		// the dust settles, has Close drained everything that managed
		// to register?". A pre-fix run that snapshotted before an
		// AddListener landed leaves that listener registered, and the
		// post-close event will fire it.
		close(start)
		go func() {
			c.Close()
			close(closeDone)
		}()
		wg.Wait()
		<-closeDone

		// Snapshot-replay during AddListener may have fired the handler
		// at registration time. Only NEW events after Close matter for
		// the leak check.
		fired.Store(0)

		hr := &manifest.HelmRelease{Name: "post-close", Namespace: "ns"}
		s.AddObject(hr)

		if got := fired.Load(); got != 0 {
			t.Fatalf("iter %d: %d listeners still active after Close — leak", iter, got)
		}
	}
}

// TestController_AddListenerAfterCloseIsNoOp pins the post-Close
// refusal: a fresh AddListener once Close has returned must register
// nothing in the underlying store.
func TestController_AddListenerAfterCloseIsNoOp(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := base.New(s, nil)

	c.Close()

	var fired atomic.Int64
	c.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) {
		fired.Add(1)
	})

	// Fire an event. The refused listener must not run.
	hr := &manifest.HelmRelease{Name: "after-close", Namespace: "ns"}
	s.AddObject(hr)

	if got := fired.Load(); got != 0 {
		t.Fatalf("AddListener after Close fired %d times; want 0 (registration must be refused)", got)
	}
}

// TestController_DoubleCloseIsSafe pins the Swap-based double-Close
// idempotency: a second Close must not re-iterate an already-drained
// slice (which would call each unsub twice) and must not panic.
func TestController_DoubleCloseIsSafe(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := base.New(s, nil)

	var fired atomic.Int64
	c.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) {
		fired.Add(1)
	})

	// First Close drains the registered listener.
	c.Close()
	// Second Close must be a no-op — Swap-on-closed returns true so
	// the drain loop is skipped entirely. If the slice were re-iterated
	// after c.unsub = nil, this would be a benign nil-range; the real
	// regression we are guarding against is a future refactor that
	// captures the slice outside the Swap branch and double-fires.
	c.Close()

	// Fire an event to confirm the listener was unsubscribed exactly
	// once (the post-Close store has no listener and won't fire).
	hr := &manifest.HelmRelease{Name: "double-close", Namespace: "ns"}
	s.AddObject(hr)

	if got := fired.Load(); got != 0 {
		t.Fatalf("listener fired %d times after double Close; want 0", got)
	}
}

// TestDispatchNode pins the dag scheduler's per-node entry point: it must bail
// (via PreGate) on a suspended object — Ready/"suspended", no reconcile — mark a
// preflight-failed id Failed without running it, and run the reconcile only for
// a non-suspended, preflight-clean object, reporting ready accordingly. (The
// scheduler's Dispatcher routes by Kind, so DispatchNode does no match check.)
func TestDispatchNode(t *testing.T) {
	s := store.New()
	ts := task.New()
	c := base.New(s, ts)
	c.SetPreflight(func(id manifest.NamedResource) (string, bool) {
		return "synthetic cycle", id.Name == "preflightfail"
	})
	c.StartLifecycle()
	t.Cleanup(func() { c.Close(); ts.BlockTillDone() })

	var ran atomic.Int64
	suspended := func(hr *manifest.HelmRelease) bool { return hr.Name == "suspended" }
	reconcile := func(_ context.Context, _ *manifest.HelmRelease) error { ran.Add(1); return nil }
	dispatch := func(id manifest.NamedResource) bool {
		_, ready := base.DispatchNode(t.Context(), c, id, 0, suspended, "helmrelease", reconcile)
		return ready
	}

	// Suspended → PreGate bails to Ready/"suspended", no reconcile.
	susHR := &manifest.HelmRelease{Name: "suspended", Namespace: "ns"}
	s.AddObject(susHR)
	if !dispatch(susHR.Named()) {
		t.Error("suspended HR: want ready=true (PreGate→Ready)")
	}
	if info, _ := s.GetStatus(susHR.Named()); info.Message != "suspended" {
		t.Errorf("suspended HR status = %+v; want Ready/suspended via PreGate", info)
	}

	// Preflight-failed → Failed, ready=false, no reconcile.
	pfHR := &manifest.HelmRelease{Name: "preflightfail", Namespace: "ns"}
	s.AddObject(pfHR)
	if dispatch(pfHR.Named()) {
		t.Error("preflight-failed HR: want ready=false")
	}
	if info, _ := s.GetStatus(pfHR.Named()); info.Status != store.StatusFailed {
		t.Errorf("preflight-failed HR status = %+v; want Failed", info)
	}

	// Clean → reconcile runs → Ready.
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	if !dispatch(hr.Named()) {
		t.Error("clean HR: want ready=true")
	}
	if info, _ := s.GetStatus(hr.Named()); info.Status != store.StatusReady {
		t.Errorf("clean HR status = %+v; want Ready", info)
	}

	if got := ran.Load(); got != 1 {
		t.Errorf("reconcile ran %d times; want 1 (only the clean, preflight-clean HR)", got)
	}
}

// TestFingerprintDedup pins the shared dedup short-circuit: it skips (returns
// handled=true) and replays the cached docs through emit only when a rendered
// artifact with a matching non-empty fingerprint exists; otherwise it returns
// handled=false so the caller renders. A preflight error surfaces as
// (handled=true, err) without emitting.
func TestFingerprintDedup(t *testing.T) {
	s := store.New()
	c := base.New(s, nil)
	ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
	s.AddObject(ks)
	id := ks.Named()
	docs := []map[string]any{{"kind": "ConfigMap"}}
	s.SetArtifact(id, &store.KustomizationArtifact{Manifests: docs, Fingerprint: "fp1"})

	var emitted int
	emit := func([]map[string]any) { emitted++ }

	// Fingerprint mismatch → render (not handled), no emit.
	if handled, err := c.FingerprintDedup(id, "other", "kustomization", emit); handled || err != nil {
		t.Errorf("mismatch: handled=%v err=%v; want false,nil", handled, err)
	}
	// Match → handled, emit replays the cached docs.
	if handled, err := c.FingerprintDedup(id, "fp1", "kustomization", emit); !handled || err != nil {
		t.Errorf("match: handled=%v err=%v; want true,nil", handled, err)
	}
	// No artifact for the id → render (not handled).
	none := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "none"}
	if handled, _ := c.FingerprintDedup(none, "fp1", "kustomization", emit); handled {
		t.Error("no-artifact id: handled=true; want false")
	}
	if emitted != 1 {
		t.Errorf("emit called %d times; want 1 (only on the match)", emitted)
	}

	// Empty fingerprint never matches (safe re-render), even fp==""==stored.
	s.SetArtifact(id, &store.KustomizationArtifact{Manifests: docs, Fingerprint: ""})
	if handled, _ := c.FingerprintDedup(id, "", "kustomization", emit); handled {
		t.Error("empty fingerprint matched; want never-match (false)")
	}
}

// TestFingerprintDedup_PreflightError: a match that coincides with a
// mid-flight preflight failure returns (handled=true, err) and does not emit.
func TestFingerprintDedup_PreflightError(t *testing.T) {
	s := store.New()
	c := base.New(s, nil)
	ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
	s.AddObject(ks)
	id := ks.Named()
	s.SetArtifact(id, &store.KustomizationArtifact{Fingerprint: "fp1"})
	c.SetPreflight(func(manifest.NamedResource) (string, bool) { return "cycle detected", true })

	handled, err := c.FingerprintDedup(id, "fp1", "kustomization", func([]map[string]any) {
		t.Error("emit must not run when a preflight error is surfaced")
	})
	if !handled || err == nil {
		t.Errorf("preflight error: handled=%v err=%v; want true, non-nil", handled, err)
	}
}
