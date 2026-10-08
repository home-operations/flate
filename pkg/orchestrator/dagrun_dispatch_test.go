package orchestrator

import (
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDAGDispatch_ResourceSetDedup(t *testing.T) {
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, true)
	c.Configure(resourceset.Options{})
	c.Start(t.Context())
	t.Cleanup(c.Close)
	rs := &manifest.ResourceSet{Name: "dedup", Namespace: "ns", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: value}
`}
	s.AddObject(rs)
	var arrivals atomic.Int64
	unsub := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals.Add(1) }, false)
	t.Cleanup(unsub)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
	out, blocked := d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone)
	if out != schedule.OutcomeTerminal || len(blocked) != 0 || arrivals.Load() != 1 {
		t.Fatalf("fresh result=%v blocked=%v arrivals=%d", out, blocked, arrivals.Load())
	}
	before := s.GetArtifact(rs.Named())
	out, blocked = d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone)
	if out != schedule.OutcomeTerminalNoop || len(blocked) != 0 || arrivals.Load() != 1 || s.GetArtifact(rs.Named()) != before {
		t.Fatalf("dedup result=%v blocked=%v arrivals=%d artifact changed=%v", out, blocked, arrivals.Load(), s.GetArtifact(rs.Named()) != before)
	}
}
