package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

func newCM(name, ns string) *manifest.ConfigMap {
	return &manifest.ConfigMap{Name: name, Namespace: ns}
}

func TestStore_AddObjectIdempotent(t *testing.T) {
	s := New()
	cm := newCM("a", "ns")
	id := cm.Named()

	count := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)

	s.AddObject(cm)
	s.AddObject(cm)
	if count != 1 {
		t.Errorf("expected exactly 1 event for two identical adds, got %d", count)
	}
	if got := s.GetObject(id); got == nil {
		t.Errorf("GetObject after AddObject returned nil")
	}
}

func TestStore_AddListener_Flush(t *testing.T) {
	s := New()
	s.AddObject(newCM("a", "ns"))
	s.AddObject(newCM("b", "ns"))

	var got []string
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		got = append(got, id.Name)
	}, true)
	if len(got) != 2 {
		t.Errorf("expected 2 replayed, got %d", len(got))
	}
}

func TestStore_UpdateStatus_Idempotent(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Namespace: "flux-system", Name: "apps"}

	count := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.UpdateStatus(id, StatusPending, "starting")
	s.UpdateStatus(id, StatusPending, "starting")
	s.UpdateStatus(id, StatusReady, "done")
	if count != 2 {
		t.Errorf("expected 2 status events, got %d", count)
	}
	info, ok := s.GetStatus(id)
	if !ok || info.Status != StatusReady {
		t.Errorf("status: %+v ok=%v", info, ok)
	}
}

func TestStore_SetCondition_NonReadyDoesNotFireStatusEvent(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Name: "k", Namespace: "ns"}
	s.UpdateStatus(id, StatusPending, "starting")
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) {
		statusEvents++
	}, false)
	// A non-Ready condition should land silently.
	s.SetCondition(id, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue, Reason: "Healthy"})
	if statusEvents != 0 {
		t.Errorf("non-Ready SetCondition fired StatusUpdated event %d times", statusEvents)
	}
	got := s.GetConditions(id)
	if len(got) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(got))
	}
}

func TestStore_SetCondition_ReadyFiresStatusEvent(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Name: "k", Namespace: "ns"}
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		statusEvents++
		info, ok := payload.(StatusInfo)
		if !ok || info.Status != StatusReady {
			t.Errorf("expected Ready payload, got %+v", payload)
		}
	}, false)
	s.SetCondition(id, Condition{
		Type: ConditionReady, Status: metav1.ConditionTrue,
		Reason: ReasonSucceeded, Message: "ok",
	})
	if statusEvents != 1 {
		t.Errorf("Ready SetCondition fired %d events; want 1", statusEvents)
	}
}

func TestStore_SetCondition_IdenticalIsNoOp(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Name: "k", Namespace: "ns"}
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) { statusEvents++ }, false)

	cond := Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded}
	s.SetCondition(id, cond)
	s.SetCondition(id, cond)
	if statusEvents != 1 {
		t.Errorf("identical SetCondition fired %d events; want 1", statusEvents)
	}
}

func TestStore_HasFailedResources(t *testing.T) {
	s := New()
	if s.HasFailedResources() {
		t.Errorf("empty store should not have failures")
	}
	id := manifest.NamedResource{Kind: "Kustomization", Name: "x"}
	s.UpdateStatus(id, StatusFailed, "boom")
	if !s.HasFailedResources() {
		t.Errorf("expected failures after Failed status")
	}
	if len(s.FailedResources()) != 1 {
		t.Errorf("FailedResources count: %d", len(s.FailedResources()))
	}
}

func TestStore_WatchReady_AlreadyReady(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	s.UpdateStatus(id, StatusReady, "")
	info, err := s.WatchReady(context.Background(), id)
	if err != nil {
		t.Fatalf("WatchReady: %v", err)
	}
	if info.Status != StatusReady {
		t.Errorf("status: %v", info.Status)
	}
}

func TestStore_WatchReady_TransitionsToReady(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}

	go s.UpdateStatus(id, StatusReady, "")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.WatchReady(ctx, id); err != nil {
		t.Fatalf("WatchReady: %v", err)
	}
}

func TestStore_WatchReady_FailedYieldsError(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	s.UpdateStatus(id, StatusFailed, "denied")

	_, err := s.WatchReady(context.Background(), id)
	var rfe *manifest.ResourceFailedError
	if !errors.As(err, &rfe) {
		t.Fatalf("expected ResourceFailedError, got %v", err)
	}
	if rfe.Reason != "denied" {
		t.Errorf("reason: %q", rfe.Reason)
	}
}

func TestStore_WatchReady_ContextCancel(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.WatchReady(ctx, id)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestStore_WatchExists(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "ConfigMap", Namespace: "ns", Name: "a"}

	// Pre-existing: returns immediately.
	s.AddObject(newCM("a", "ns"))
	obj, err := s.WatchExists(context.Background(), id)
	if err != nil || obj == nil {
		t.Fatalf("WatchExists existing: %v %v", obj, err)
	}

	// Subscribe first, then add: WatchExists rechecks the store after
	// subscribing, so add-then-subscribe and subscribe-then-add both
	// resolve correctly.
	id2 := manifest.NamedResource{Kind: "ConfigMap", Namespace: "ns", Name: "b"}
	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = s.WatchExists(ctx, id2)
		close(done)
	}()
	s.AddObject(newCM("b", "ns"))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WatchExists never returned")
	}
}

func TestStore_WatchAdded(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.WatchAdded(ctx, "ConfigMap", 16)

	s.AddObject(newCM("a", "ns"))
	s.AddObject(newCM("b", "ns"))
	// A non-CM object should not appear.
	s.AddObject(&manifest.Secret{Name: "z", Namespace: "ns"})

	got := make(map[string]struct{})
	timeout := time.After(time.Second)
	for len(got) < 2 {
		select {
		case ev := <-ch:
			got[ev.ID.Name] = struct{}{}
		case <-timeout:
			t.Fatalf("timed out, got %v", got)
		}
	}
	if _, ok := got["a"]; !ok {
		t.Errorf("missing 'a'")
	}
	if _, ok := got["b"]; !ok {
		t.Errorf("missing 'b'")
	}
}

func TestStore_AddListener_Unsubscribe(t *testing.T) {
	s := New()
	count := 0
	unsub := s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.AddObject(newCM("a", "ns"))
	unsub()
	s.AddObject(newCM("b", "ns"))
	if count != 1 {
		t.Errorf("expected 1 event after unsubscribe, got %d", count)
	}
}

func TestStore_SetArtifact(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	count := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // idempotent
	if count != 1 {
		t.Errorf("expected 1 artifact event, got %d", count)
	}
	got := s.GetArtifact(id)
	if got == nil {
		t.Errorf("GetArtifact: nil")
	}
}

func TestStore_ListenerPanicIsolated(t *testing.T) {
	s := New()
	other := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		panic("boom")
	}, false)
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		other++
	}, false)
	s.AddObject(newCM("a", "ns")) // should not crash
	if other != 1 {
		t.Errorf("other listener should still have run, got %d", other)
	}
}

func TestStore_Concurrency(_ *testing.T) {
	s := New()
	const N = 200

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range N {
			s.AddObject(newCM("a", "ns"))
		}
	}()
	go func() {
		defer wg.Done()
		for range N {
			_ = s.ListObjects("ConfigMap")
		}
	}()
	wg.Wait()
}
