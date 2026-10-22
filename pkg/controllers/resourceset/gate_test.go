package resourceset

import (
	"sync/atomic"
	"testing"

	_ "github.com/home-operations/flate/internal/kustomize"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_SharedAdmission(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, task.NewBounded(2), false)
	c.Configure(base.Options{})
	rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n"}
	s.AddObject(rs)
	if err := c.newExecution(rs.Named()).reconcile(t.Context(), rs, manifest.DrainNone); err != nil {
		t.Fatal(err)
	}
	if s.GetArtifact(rs.Named()) == nil {
		t.Fatal("missing completed output")
	}
}

func TestReconcile_SingleFailureTransition(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, task.NewBounded(2), false)
	c.Configure(base.Options{})
	rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", ResourcesTemplate: "<< missingFunction >>"}
	s.AddObject(rs)
	var failures atomic.Int32
	unsubscribe := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == rs.Named() && payload.(store.StatusInfo).Status == store.StatusFailed {
			failures.Add(1)
		}
	}, false)
	defer unsubscribe()
	if err := c.newExecution(rs.Named()).reconcile(t.Context(), rs, manifest.DrainNone); err == nil {
		t.Fatal("malformed template succeeded")
	}
	if info, _ := s.GetStatus(rs.Named()); info.Status == store.StatusFailed || failures.Load() != 0 {
		t.Fatalf("render body terminalized before dispatch: status=%+v, failure events=%d", info, failures.Load())
	}
	if blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone).Blocked; len(blocked) != 0 {
		t.Fatalf("blocked: %v", blocked)
	}
	info, _ := s.GetStatus(rs.Named())
	if info.Status != store.StatusFailed || failures.Load() != 1 {
		t.Fatalf("status=%+v, failure events=%d", info, failures.Load())
	}
}
