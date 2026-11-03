package orchestrator

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func BenchmarkRender_Namespace(b *testing.B) {
	for _, peers := range []int{2, 64} {
		b.Run(fmt.Sprintf("peers=%d", peers), func(b *testing.B) {
			tree, artifact := namespaceChildrenFixture(b, peers)
			cfg := Config{
				Concurrency: 2,
				Selection: manifest.Selection{
					Roots: []manifest.NamedResource{
						testutil.KustomizationID("apps", ""),
						testutil.HelmReleaseID("apps", ""),
						{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps"},
					},
				},
			}
			prepared, err := newTestOrchestrator(b, tree, cfg)
			if err != nil {
				b.Fatal(err)
			}
			var renders, fetches int64
			for b.Loop() {
				o := newOrchestrator(prepared.cfg.Config, prepared.cfg.Tree, task.NewBounded(2))
				fetcher := &generatedOCIFetcher{root: artifact}
				o.src.Fetchers[manifest.KindOCIRepository] = fetcher
				var emissions atomic.Int64
				unsub := o.store.AddListener(store.EventArtifactUpdated, func(_ manifest.NamedResource, payload any) {
					if _, ok := payload.(*store.Emission); ok {
						emissions.Add(1)
					}
				}, false)
				_, err = o.render(b.Context())
				unsub()
				if err != nil {
					b.Fatal(err)
				}
				renders += emissions.Load()
				fetches += fetcher.calls.Load()
			}
			b.ReportMetric(float64(renders)/float64(b.N), "renders/op")
			b.ReportMetric(float64(fetches)/float64(b.N), "fetches/op")
		})
	}
}
