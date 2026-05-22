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
		func(ctx context.Context, obj *manifest.HelmRelease) error {
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
		func(ctx context.Context, obj *manifest.HelmRelease) error {
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

	// Panic must be caught and converted into StatusFailed; no re-panic.
	base.RunWithStatus(t.Context(), s, id, "helmrelease",
		func(ctx context.Context, obj *manifest.HelmRelease) error {
			panic("kaboom")
		},
	)
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
		func(ctx context.Context, obj *manifest.HelmRelease) error {
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
