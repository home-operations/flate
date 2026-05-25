package base_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
