package resourceset

import (
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/schedule"

	"github.com/home-operations/flate/internal/assert"

	_ "github.com/home-operations/flate/internal/kustomizestate"

	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReconcile_SharedAdmission(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, task.NewBounded(2), nil)
	c.Configure(base.Options{})
	rs := &manifest.ResourceSet{
		Name:              "app",
		Namespace:         "apps",
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n",
	}
	s.AddObject(rs)
	{
		err := c.newExecution(rs.Named()).reconcile(t.Context(), rs, manifest.DrainNone)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assert.FatalIf(t, s.GetArtifact(rs.Named(), store.ArtifactContent) == nil, "%v", "missing completed output")
}

func TestReconcile_SingleFailureTransition(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, task.NewBounded(2), nil)
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
	{
		err := c.newExecution(rs.Named()).reconcile(t.Context(), rs, manifest.DrainNone)
		assert.FatalIf(t, err == nil, "%v", "malformed template succeeded")
	}
	if info, _ := s.GetStatus(rs.Named()); info.Status == store.StatusFailed || failures.Load() != 0 {
		t.Fatalf("render body terminalized before dispatch: status=%+v, failure events=%d", info, failures.Load())
	}
	{
		blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone, schedule.Result{}).Blocked
		assert.FatalIf(t, len(blocked) != 0, "blocked: %v", blocked)
	}
	info, _ := s.GetStatus(rs.Named())
	assert.FatalIf(t, info.Status != store.StatusFailed || failures.Load() != 1,
		"status=%+v, failure events=%d", info, failures.Load())
}
