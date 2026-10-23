package base_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDispatchNode_Success(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	out, _ := base.New(s, nil, "helmrelease").DispatchNode(t.Context(), id, manifest.ReconcileAttempt{}, func(_ context.Context, obj *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
		if gated {
			return schedule.Outcome{}, nil
		}
		if obj.Name != "app" {
			t.Errorf("re-read got %q, want app", obj.Name)
		}
		return schedule.Outcome{}, nil
	},
	)
	assert.Diff(t, out, schedule.Outcome{})
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusReady {
		t.Errorf("status = %v, want Ready", got.Status)
	}
}

func TestDispatchNode_Failure(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	out, _ := base.New(s, nil, "helmrelease").DispatchNode(t.Context(), id, manifest.ReconcileAttempt{}, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
		if gated {
			return schedule.Outcome{}, nil
		}
		return schedule.Outcome{}, errors.New("render failed")
	},
	)
	assert.Diff(t, out, schedule.Outcome{})
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusFailed {
		t.Errorf("status = %v, want Failed", got.Status)
	}
	if got.Message != "render failed" {
		t.Errorf("message = %q, want %q", got.Message, "render failed")
	}
}

func TestDispatchNode_Panic(t *testing.T) {
	t.Parallel()
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
		base.New(s, nil, "helmrelease").DispatchNode(t.Context(), id, manifest.ReconcileAttempt{}, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
			if gated {
				return schedule.Outcome{}, nil
			}
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

func TestDispatchNode_MissingObject(t *testing.T) {
	t.Parallel()
	for _, wrongType := range []bool{false, true} {
		t.Run(fmt.Sprint(wrongType), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			id := testutil.HelmReleaseID("ns", "ghost")
			if wrongType {
				s.AddObject(&manifest.RawObject{APIVersion: manifest.HelmReleaseDomain + "/v2", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace})
			}
			called := false
			fn := func(context.Context, *manifest.HelmRelease, manifest.ReconcileAttempt, bool) (schedule.Outcome, error) {
				called = true
				return schedule.Outcome{Noop: true, Rerun: true}, nil
			}
			c := base.New(s, nil, "helmrelease")
			out, _ := c.DispatchNode(t.Context(), id, manifest.ReconcileAttempt{}, fn)
			assert.Diff(t, out, schedule.Outcome{})
			assert.Equal(t, called, false)
			if _, ok := s.GetStatus(id); ok {
				t.Error("unavailable typed object acquired a status entry")
			}
			s.UpdateStatus(id, store.StatusPending, store.ReasonRefetching, "re-fetching")
			out, _ = c.DispatchNode(t.Context(), id, manifest.ReconcileAttempt{}, fn)
			assert.Diff(t, out, schedule.Outcome{})
			assert.Equal(t, called, false)
			got, _ := s.GetStatus(id)
			if got.Outcome() != store.OutcomeSkipped || got.Message != "object not in store at reconcile time" {
				t.Fatalf("missing-object status = %+v", got)
			}
		})
	}
}

func TestDispatchNode_PreservesInformativeReadyMessage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		reason  store.StatusReason
		message string
	}{
		{name: "skipped", reason: store.ReasonSkipped, message: "missing secret"},
		{name: "empty skip", reason: store.ReasonSkipped},
		{name: "empty unchanged", reason: store.ReasonUnchanged},
		{name: "empty suspended", reason: store.ReasonSuspended},
		{name: "informative ready", message: "skipped: a literal explanation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
			s.AddObject(hr)
			id := hr.Named()
			s.UpdateStatus(id, store.StatusReady, tc.reason, tc.message)
			before, _ := s.GetStatus(id)
			out, outWait := base.New(s, nil, "helmrelease").DispatchNode(t.Context(), id, manifest.ReconcileAttempt{}, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
				if gated {
					return schedule.Outcome{Noop: true, Rerun: true}, nil
				}
				return schedule.Outcome{Noop: true, Rerun: true}, nil
			})
			got, _ := s.GetStatus(id)
			if out.Kind != schedule.Terminal || !outWait.Empty() || out.Noop || !out.Rerun {
				t.Fatalf("preserved output facts: %+v", out)
			}
			if got != before {
				t.Fatalf("Ready snapshot changed: %+v, want %+v", got, before)
			}
		})
	}
	for _, tc := range []struct {
		name          string
		reason        store.StatusReason
		message, want string
	}{
		{name: "nested skip", reason: store.ReasonSkipped, message: "skipped: missing auth", want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth"},
		{name: "boundary whitespace", reason: store.ReasonSkipped, message: "  missing auth \n", want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth"},
		{name: "empty explanation", reason: store.ReasonSkipped, want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:"},
		{name: "misleading message", message: "skipped: missing auth"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
			s.AddObject(hr)
			c := base.New(s, nil, "helmrelease")
			src := &manifest.OCIRepository{Name: "source", Namespace: "ns"}
			s.AddObject(src)
			s.UpdateStatus(src.Named(), store.StatusReady, tc.reason, tc.message)
			c.DispatchNode(t.Context(), hr.Named(), manifest.ReconcileAttempt{}, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
				if gated {
					return schedule.Outcome{}, nil
				}
				return schedule.Outcome{}, c.SourceSkipped(src.Named(), "source")
			})
			got, _ := s.GetStatus(hr.Named())
			want := store.OutcomePassed
			if tc.reason == store.ReasonSkipped {
				want = store.OutcomeSkipped
			}
			if got.Outcome() != want || got.Message != tc.want {
				t.Fatalf("propagated status = %+v, want %s/%q", got, want, tc.want)
			}
		})
	}
	for _, message := range []string{" \t missing auth \n", " skipped: missing auth "} {
		t.Run(fmt.Sprintf("direct/%q", message), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app"}
			s.AddObject(hr)
			out, _ := base.New(s, nil, "test").DispatchNode(t.Context(), hr.Named(), manifest.ReconcileAttempt{}, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
				if gated {
					return schedule.Outcome{}, nil
				}
				return schedule.Outcome{Noop: true}, fmt.Errorf("%w: %s", manifest.ErrSourceSkipped, message)
			})
			assert.Diff(t, out, schedule.Outcome{})
			got, _ := s.GetStatus(hr.Named())
			if got.Outcome() != store.OutcomeSkipped || got.Message != strings.TrimSpace(message) {
				t.Fatalf("direct skip = %+v", got)
			}
		})
	}
}

func TestDispatchNode_PreservesExternalFailure(t *testing.T) {
	t.Parallel()
	for _, dependency := range []bool{false, true} {
		t.Run(fmt.Sprint(dependency), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
			s.AddObject(hr)
			id := hr.Named()
			dep := testutil.HelmReleaseID("ns", "dependency")
			out, outWait := base.New(s, nil, "helmrelease").DispatchNode(t.Context(), id, manifest.ReconcileAttempt{}, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
				if gated {
					return schedule.Outcome{}, nil
				}
				if dependency {
					s.UpdateFailure(id, &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}})
				} else {
					s.UpdateStatus(id, store.StatusFailed, "", "dependency cycle detected")
				}
				return schedule.Outcome{Noop: true, Rerun: true}, nil
			})
			got, _ := s.GetStatus(id)
			assert.Equal(t, got.Status, store.StatusFailed)
			assert.Equal(t, out.Noop, false)
			assert.Equal(t, out.Rerun, true)
			if dependency {
				assert.Equal(t, out.Kind, schedule.DependencyFailed)
				assert.Diff(t, outWait.Deps, []manifest.NamedResource{dep})
			} else {
				assert.Equal(t, got.Message, "dependency cycle detected")
				assert.Equal(t, out.Kind, schedule.Terminal)
				assert.Equal(t, outWait.Empty(), true)
			}
		})
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
		c := base.New(s, nil, "test")

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
	c := base.New(s, nil, "test")

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
	c := base.New(s, nil, "test")

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
// (via preGate) on a suspended object — Ready/"suspended", no reconcile — mark a
// preflight-failed id Failed without running it, and run the reconcile only for
// a non-suspended, preflight-clean object. (The scheduler's Dispatcher routes by
// Kind, so DispatchNode does no match check.)
func TestDispatchNode(t *testing.T) {
	t.Parallel()
	s := store.New()
	ts := task.NewBounded(0)
	c := base.New(s, ts, "test")
	c.Configure(base.Options{PreflightFailure: func(id manifest.NamedResource) (string, bool) {
		return "synthetic cycle", id.Name == "preflightfail"
	}})
	c.StartLifecycle()
	t.Cleanup(func() { c.Close(); ts.BlockTillDone() })

	var ran atomic.Int64
	reconcile := func(_ context.Context, _ *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
		if gated {
			return schedule.Outcome{}, nil
		}
		ran.Add(1)
		return schedule.Outcome{}, nil
	}
	dispatch := func(id manifest.NamedResource) {
		c.DispatchNode(t.Context(), id, manifest.ReconcileAttempt{Drain: manifest.DrainNone}, reconcile)
	}

	// Suspended → preGate bails to Ready/"suspended", no reconcile.
	susHR := &manifest.HelmRelease{Name: "suspended", Namespace: "ns", Suspend: true}
	s.AddObject(susHR)
	dispatch(susHR.Named())
	if info, _ := s.GetStatus(susHR.Named()); info.Outcome() != store.OutcomeSuspended || info.Message != "suspended" {
		t.Errorf("suspended HR status = %+v; want Ready/suspended via preGate", info)
	}

	// Preflight-failed → Failed, no reconcile.
	pfHR := &manifest.HelmRelease{Name: "preflightfail", Namespace: "ns"}
	s.AddObject(pfHR)
	dispatch(pfHR.Named())
	if info, _ := s.GetStatus(pfHR.Named()); info.Status != store.StatusFailed {
		t.Errorf("preflight-failed HR status = %+v; want Failed", info)
	}

	// Clean → reconcile runs → Ready.
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	dispatch(hr.Named())
	if info, _ := s.GetStatus(hr.Named()); info.Status != store.StatusReady {
		t.Errorf("clean HR status = %+v; want Ready", info)
	}

	if got := ran.Load(); got != 1 {
		t.Errorf("reconcile ran %d times; want 1 (only the clean, preflight-clean HR)", got)
	}
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	chart.Suspend = true
	for _, tc := range []struct {
		name      string
		obj       manifest.BaseManifest
		filtered  bool
		preflight bool
		message   string
		outcome   store.Outcome
		run       bool
	}{
		{"filtered", &manifest.HelmRelease{Name: "active"}, true, false, "unchanged", store.OutcomeUnchanged, false},
		{"suspended before filter", &manifest.HelmRelease{Name: "suspended", Suspend: true}, true, false, "suspended", store.OutcomeSuspended, false},
		{"suspended before preflight", &manifest.HelmRelease{Name: "suspended", Suspend: true}, false, true, "suspended", store.OutcomeSuspended, false},
		{"suspended chart before filter", chart, true, false, "suspended", store.OutcomeSuspended, false},
		{"suspended chart before preflight", chart, false, true, "suspended", store.OutcomeSuspended, false},
		{"active chart", &manifest.HelmChartSource{Name: "active-chart"}, false, false, "", store.OutcomePassed, true},
		{"suspended repository before filter", &manifest.HelmRepository{Name: "repo", Suspend: true}, true, false, "suspended", store.OutcomeSuspended, false},
		{"suspended repository before preflight", &manifest.HelmRepository{Name: "repo", Suspend: true}, false, true, "suspended", store.OutcomeSuspended, false},
		{"active repository", &manifest.HelmRepository{Name: "repo"}, false, false, "", store.OutcomePassed, true},
		{"data", &manifest.ConfigMap{Name: "data"}, false, false, "", store.OutcomePassed, true},
		{"secret", &manifest.Secret{Name: "secret"}, false, false, "", store.OutcomePassed, true},
		{"raw object", &manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "raw", Spec: map[string]any{"suspend": true}}, false, false, "", store.OutcomePassed, true},
		{"input provider", &manifest.ResourceSetInputProvider{Name: "provider"}, false, false, "", store.OutcomePassed, true},
		{"external artifact", &manifest.ExternalArtifact{Name: "source"}, false, false, "", store.OutcomePassed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.AddObject(tc.obj)
			id := tc.obj.Named()
			c := base.New(st, nil, "test")
			if tc.filtered {
				c.Configure(base.Options{Filter: change.NewFilter(change.NewSet([]string{"other.yaml"}), &manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "consumer.yaml"}},
				}, st)})
			}
			if tc.preflight {
				c.Configure(base.Options{PreflightFailure: func(manifest.NamedResource) (string, bool) { return "cycle", true }})
			}
			c.StartLifecycle()
			ran := false
			c.DispatchNode(t.Context(), id, manifest.ReconcileAttempt{Drain: manifest.DrainNone}, func(_ context.Context, _ manifest.BaseManifest, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
				if gated {
					return schedule.Outcome{}, nil
				}
				ran = true
				return schedule.Outcome{}, nil
			})
			info, _ := st.GetStatus(id)
			if ran != tc.run || info.Outcome() != tc.outcome || info.Message != tc.message {
				t.Fatalf("ran=%t status=%+v, want ran=%t Ready/%s", ran, info, tc.run, tc.message)
			}
		})
	}
}

func TestFingerprintDedup(t *testing.T) {
	s := store.New()
	c := base.New(s, nil, "test")
	ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
	s.AddObject(ks)
	id := ks.Named()
	docs := []map[string]any{{"kind": "ConfigMap"}}
	fp, handled, noop, err := c.FingerprintDedup(id, []string{"one"})
	if fp == "" || handled || noop || err != nil {
		t.Fatalf("first key = %q, handled=%v, err=%v", fp, handled, err)
	}
	s.SetArtifact(id, &store.Emission{Manifests: docs, Fingerprint: fp})
	if _, handled, noop, err := c.FingerprintDedup(id, "other"); handled || noop || err != nil {
		t.Errorf("mismatch: handled=%v err=%v; want false,nil", handled, err)
	}
	foreign := id
	foreign.Group = "other.example"
	if _, handled, noop, err := c.FingerprintDedup(foreign, []string{"one"}); handled || noop || err != nil {
		t.Fatalf("foreign group: handled=%v err=%v; want false,nil", handled, err)
	}
	if got, handled, noop, err := c.FingerprintDedup(id, []string{"one"}); !handled || !noop || err != nil || got != fp {
		t.Errorf("match: key=%q handled=%v noop=%v err=%v; want %q,true,true,nil", got, handled, noop, err, fp)
	}
	none := testutil.KustomizationID("ns", "none")
	if _, handled, noop, _ := c.FingerprintDedup(none, []string{"one"}); handled || noop {
		t.Error("no-artifact id: handled=true; want false")
	}
	for _, tc := range []struct {
		name    string
		content any
	}{
		{"nil content", nil}, {"unmarshalable content", []any{make(chan int)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, handled, noop, err := c.FingerprintDedup(id, tc.content)
			if got != "" || handled || noop || err != nil {
				t.Fatalf("invalid content key=%q, handled=%v, err=%v", got, handled, err)
			}
		})
	}
	s.SetArtifact(id, &store.Emission{Manifests: docs})
	if _, handled, noop, _ := c.FingerprintDedup(id, []string{"one"}); handled || noop {
		t.Error("empty stored fingerprint replayed")
	}
	if fp, handled, noop, err := c.FingerprintDedup(id, []any{make(chan int)}); fp != "" || handled || noop || err != nil {
		t.Fatalf("invalid content replayed an empty stored key: key=%q, handled=%v, err=%v", fp, handled, err)
	}
	_, retained := s.GetEmission(id)
	child := &manifest.ConfigMap{Name: "output", Namespace: "ns"}
	retained.Children = []store.Child{{Obj: child}}
	retained.Fingerprint, retained.Complete = fp, true
	s.RecordEmitted(id, &retained)
	fresh := ks.Clone()
	fresh.Path = "changed"
	s.AddObject(fresh)
	if s.GetArtifact(id) != nil {
		t.Fatal("changed declaration retained a current artifact")
	}
	if _, handled, noop, err := c.FingerprintDedup(id, []string{"one"}); !handled || noop || err != nil {
		t.Fatalf("retained replay: handled=%v noop=%v err=%v", handled, noop, err)
	}
}

func TestFingerprintDedup_PreflightError(t *testing.T) {
	t.Parallel()
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprint(replay), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			c := base.New(s, nil, "test")
			ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
			s.AddObject(ks)
			id := ks.Named()
			fp, _, _, _ := c.FingerprintDedup(id, []string{"one"})
			_, emission := s.GetEmission(id)
			emission.Fingerprint, emission.Complete = fp, true
			s.RecordEmitted(id, &emission)
			if replay {
				fresh := ks.Clone()
				fresh.Path = "changed"
				s.AddObject(fresh)
			}
			c.Configure(base.Options{PreflightFailure: func(manifest.NamedResource) (string, bool) { return "cycle detected", true }})
			_, handled, noop, err := c.FingerprintDedup(id, []string{"one"})
			if !handled || noop || err == nil {
				t.Errorf("preflight error: handled=%v noop=%v err=%v", handled, noop, err)
			}
		})
	}
}

func TestReconcile_TypedWait(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Group: manifest.HelmReleaseDomain, Name: "reader", Namespace: "apps"}
	for _, obj := range []manifest.BaseManifest{
		&manifest.Kustomization{Name: "reader", Namespace: "apps"},
		&manifest.HelmRelease{Name: "reader", Namespace: "apps"},
		&manifest.ResourceSet{Name: "reader", Namespace: "apps"},
		&manifest.GitRepository{Name: "reader", Namespace: "apps"},
	} {
		for _, tc := range []struct {
			name string
			wait manifest.ReconcileWait
		}{
			{"nil", manifest.ReconcileWait{}},
			{"empty content", manifest.ReconcileWait{Content: &manifest.ContentWait{}}},
			{"deps", manifest.ReconcileWait{Deps: []manifest.NamedResource{id}}},
			{"targets", manifest.ReconcileWait{Content: &manifest.ContentWait{Targets: []manifest.NamedResource{id}}}},
			{"suppliers", manifest.ReconcileWait{Content: &manifest.ContentWait{Suppliers: []manifest.Opportunity{{NamedResource: id}}}}},
			{"declaration", manifest.ReconcileWait{Content: &manifest.ContentWait{Declaration: &manifest.HelmRelease{Name: id.Name, Namespace: id.Namespace}}}},
		} {
			t.Run(obj.Named().Kind+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				id := obj.Named()
				st := store.New()
				st.AddObject(obj)
				st.UpdateStatus(id, store.StatusPending, "", "waiting")
				c := base.New(st, task.NewBounded(2), "test")
				invocation := manifest.ReconcileAttempt{Drain: manifest.DrainForce, Content: &manifest.ContentWait{Declaration: obj}}
				wait, waitWait := c.DispatchNode(t.Context(), id, invocation, func(_ context.Context, _ manifest.BaseManifest, invocation manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
					if gated {
						return schedule.Outcome{}, nil
					}
					got := invocation
					assert.Equal(t, got.Drain, invocation.Drain)
					assert.Equal(t, got.Content.Declaration, invocation.Content.Declaration)
					return schedule.Outcome{}, &depwait.ErrBlocked{ReconcileWait: tc.wait}
				})
				wantKind := schedule.Blocked
				if tc.wait.Empty() {
					wantKind = schedule.Terminal
				}
				assert.Equal(t, wait.Kind, wantKind)
				assert.Equal(t, waitWait.Content, tc.wait.Content)
				assert.Diff(t, waitWait, tc.wait)
				assert.Equal(t, waitWait.Empty(), tc.name == "nil" || tc.name == "empty content")
				info, _ := st.GetStatus(id)
				assert.Equal(t, info.Status, store.StatusPending)
			})
		}
	}
}

func TestDispatchNode_GateReplacement(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "reader", Namespace: "apps"}
	hr.Chart.Name = "before"
	s.AddObject(hr)
	s.UpdateFailure(hr.Named(), &manifest.DependencyFailedError{Failed: []manifest.NamedResource{testutil.KustomizationID("apps", "dependency")}})
	fresh := hr.Clone()
	fresh.Chart.Name = "after"
	unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == hr.Named() && payload.(store.StatusInfo).Status == store.StatusPending {
			s.AddObject(fresh)
		}
	}, false)
	t.Cleanup(unsub)
	c := base.New(s, task.NewBounded(2), "test")
	called := false
	_, waitWait := c.DispatchNode(t.Context(), hr.Named(), manifest.ReconcileAttempt{}, func(_ context.Context, obj *manifest.HelmRelease, _ manifest.ReconcileAttempt, gated bool) (schedule.Outcome, error) {
		if gated {
			return schedule.Outcome{}, nil
		}
		called = true
		assert.Equal(t, obj.Chart.Name, "after")
		return schedule.Outcome{}, nil
	})
	assert.Equal(t, called, true)
	assert.Equal(t, waitWait.Empty(), true)
}
