package store

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkListenerFire_ChangedWrite(b *testing.B) {
	for _, listeners := range []int{0, 1, 50} {
		b.Run(fmt.Sprintf("listeners=%d", listeners), func(b *testing.B) {
			s := New()
			for range listeners {
				s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) {}, false)
			}
			objects := [2]*manifest.RawObject{
				{APIVersion: "v1", Kind: "ConfigMap", Name: "app", Spec: map[string]any{"data": "first"}},
				{APIVersion: "v1", Kind: "ConfigMap", Name: "app", Spec: map[string]any{"data": "second"}},
			}
			var i int
			for b.Loop() {
				s.AddObject(objects[i%len(objects)])
				i++
			}
		})
	}
}

func BenchmarkAddRendered(b *testing.B) {
	for _, listeners := range []bool{false, true} {
		b.Run(fmt.Sprintf("listeners=%t", listeners), func(b *testing.B) {
			for _, distinct := range []bool{false, true} {
				b.Run(fmt.Sprintf("distinct=%t", distinct), func(b *testing.B) {
					s := New()
					if listeners {
						s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) {}, false)
					}
					obj := &manifest.RawObject{
						APIVersion: "apps/v1", Kind: "Deployment", Name: "app", Namespace: "ns",
						Spec: map[string]any{"replicas": 2, "template": map[string]any{
							"spec": map[string]any{"containers": []any{map[string]any{
								"name": "app", "image": "example/app:1.0",
							}}},
						}},
					}
					s.AddObject(obj)
					twin := obj
					if distinct {
						twin = obj.Clone()
					}
					b.ReportAllocs()
					for b.Loop() {
						s.AddObject(twin)
					}
				})
			}
		})
	}
}

// BenchmarkAddObject_Contended exercises the hot path with a single
// Kind under writer contention — measures the within-shard cost
// (shard sharding can't help when every goroutine hits the same
// shard) so a regression in the dedup / setLocked / fireUnderLock
// chain shows up clean.
func BenchmarkAddObject_Contended(b *testing.B) {
	for _, p := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("parallel=%d", p), func(b *testing.B) {
			s := New()
			b.SetParallelism(p)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					s.AddObject(&manifest.Kustomization{
						Name:      fmt.Sprintf("ks-%d", i),
						Namespace: "ns",
					})
					i++
				}
			})
		})
	}
}

// BenchmarkAddObject_MixedKinds is the Phase 3.1 sharding's key
// benefit: writes on KS and HR (distinct Kinds, distinct shards) no
// longer serialize. Compare parallel scaling against
// BenchmarkAddObject_Contended (single Kind, single shard) — the
// mixed-Kind variant should scale closer to linearly with GOMAXPROCS,
// while the single-Kind variant plateaus at one shard's
// throughput ceiling.
func BenchmarkAddObject_MixedKinds(b *testing.B) {
	for _, p := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("parallel=%d", p), func(b *testing.B) {
			s := New()
			b.SetParallelism(p)
			var workerID atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				myID := workerID.Add(1)
				// Half the workers write KS, the other half HR — splits
				// the parallel goroutine set across two shards (KS hashes
				// to 14, HR to 10 in shardCount=16).
				writeKS := myID%2 == 0
				i := 0
				for pb.Next() {
					i++
					if writeKS {
						s.AddObject(&manifest.Kustomization{
							Name:      fmt.Sprintf("ks-w%d-%d", myID, i),
							Namespace: "ns",
						})
					} else {
						s.AddObject(&manifest.HelmRelease{
							Name:      fmt.Sprintf("hr-w%d-%d", myID, i),
							Namespace: "ns",
						})
					}
				}
			})
		})
	}
}

func BenchmarkGetObjectByName_LongNames(b *testing.B) {
	s := New()
	obj := &manifest.Kustomization{
		Namespace: strings.Repeat("n", 31),
		Name:      strings.Repeat("a", 31),
	}
	s.AddObject(obj)
	b.ReportAllocs()
	for b.Loop() {
		s.GetObjectByName(manifest.KindKustomization, obj.Namespace, obj.Name)
	}
}
