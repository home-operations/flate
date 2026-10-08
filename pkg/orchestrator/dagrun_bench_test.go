package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func BenchmarkDAGDispatch_ResourceSet(b *testing.B) {
	for _, mode := range []string{"controller", "dispatcher", "observation_reads"} {
		b.Run(mode, func(b *testing.B) {
			s := store.New()
			c := resourceset.New(s, task.NewBounded(2), true)
			c.Configure(resourceset.Options{})
			c.Start(b.Context())
			b.Cleanup(c.Close)
			rs := &manifest.ResourceSet{Name: "bench", Namespace: "ns"}
			s.AddObject(rs)
			d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
			d.Dispatch(b.Context(), rs.Named(), schedule.DrainNone)
			b.ReportAllocs()
			for b.Loop() {
				switch mode {
				case "controller":
					c.ReconcileNode(b.Context(), rs.Named(), schedule.DrainNone)
				case "dispatcher":
					d.Dispatch(b.Context(), rs.Named(), schedule.DrainNone)
				case "observation_reads":
					before, _ := s.GetArtifact(rs.Named()).(*store.ResourceSetArtifact)
					if s.GetArtifact(rs.Named()) != before {
						b.Fatal("artifact changed")
					}
					info, _ := s.GetStatus(rs.Named())
					_, present := s.Get[*manifest.ResourceSet](rs.Named())
					if info.Status != store.StatusReady || !present {
						b.Fatal("resource disappeared")
					}
				}
			}
		})
	}
}
