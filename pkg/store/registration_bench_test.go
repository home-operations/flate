package store

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

const registrationReads = 10_000

// One operation is a complete read attempt plus competing writes. Registration
// and retirement must stay inside this loop when an attempt API is available.
func BenchmarkReadRegistration_Optional10000(b *testing.B) {
	for _, mode := range []string{"fixed", "produced", "absent", "repeated"} {
		for _, workers := range []int{2, 16, runtime.NumCPU() * 4} {
			b.Run(fmt.Sprintf("%s/workers=%d", mode, workers), func(b *testing.B) {
				s, ids, versions := registrationFixture(mode)
				tasks := task.NewBounded(workers)
				consumer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "consumer"}
				s.AddObject(&manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace})
				other := &manifest.Secret{Name: "unqueried", Namespace: "ns"}
				unrelated := &manifest.ConfigMap{Name: "unrelated", Namespace: "ns"}
				reads := make([]int, workers-1)
				generation := 0
				b.ReportAllocs()
				for b.Loop() {
					started, written := make(chan struct{}), make(chan struct{})
					start := sync.OnceFunc(func() { close(started) })
					for worker := range workers - 1 {
						tasks.Go(b.Context(), "reader", func(context.Context) {
							if worker == 0 {
								defer start()
							}
							reads[worker] = 0
							for i := worker; i < len(ids); i += workers - 1 {
								_, present := s.Get[*manifest.Secret](ids[i])
								if present {
									reads[worker]++
								}
								if i == 0 {
									start()
								}
							}
							// The attempt cannot finish before the competing writer.
							tasks.YieldSlot(func() { <-written })
						})
					}
					tasks.Go(b.Context(), "writer", func(context.Context) {
						defer close(written)
						tasks.YieldSlot(func() { <-started })
						s.AddObject(versions[generation%2])
						freshSecret := *other
						freshSecret.StringData = versions[generation%2].StringData
						s.AddObject(&freshSecret)
						freshCM := *unrelated
						freshCM.Data = versions[generation%2].StringData
						s.AddObject(&freshCM)
					})
					tasks.BlockTillDone()
					s.UpdateStatus(consumer, StatusReady, "")
					if mode == "absent" {
						s.DeleteObject(ids[0])
					}
					generation++
				}
				if tasks.Failures() != 0 {
					b.Fatal("worker panicked")
				}
				present := 0
				for _, n := range reads {
					present += n
				}
				if mode != "absent" && present != registrationReads {
					b.Fatalf("present reads = %d, want %d", present, registrationReads)
				}
			})
		}
	}
}

func BenchmarkReadRegistration_FanIn10000(b *testing.B) {
	for _, workers := range []int{2, 16, runtime.NumCPU() * 4} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			s, ids, versions := registrationFixture("repeated")
			consumers := make([]manifest.NamedResource, registrationReads)
			for i := range consumers {
				consumers[i] = manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: fmt.Sprintf("consumer-%05d", i)}
				s.AddObject(&manifest.Kustomization{Name: consumers[i].Name, Namespace: consumers[i].Namespace})
				s.UpdateStatus(consumers[i], StatusReady, "")
			}
			tasks := task.NewBounded(workers)
			generation := 0
			b.ReportAllocs()
			for b.Loop() {
				for worker := range workers {
					tasks.Go(b.Context(), "observe", func(context.Context) {
						for i := worker; i < len(consumers); i += workers {
							s.GetObject(ids[i])
							s.UpdateStatus(consumers[i], StatusReady, "")
						}
					})
				}
				tasks.BlockTillDone()
				started, written := make(chan struct{}), make(chan struct{})
				start := sync.OnceFunc(func() { close(started) })
				for worker := range workers - 1 {
					tasks.Go(b.Context(), "completion", func(context.Context) {
						if worker == 0 {
							defer start()
						}
						for i := worker; i < len(consumers); i += workers - 1 {
							s.GetObject(ids[i])
							if i == 0 {
								start()
							}
							s.UpdateStatus(consumers[i], StatusPending, "completion")
							s.UpdateStatus(consumers[i], StatusReady, "")
						}
						tasks.YieldSlot(func() { <-written })
					})
				}
				tasks.Go(b.Context(), "mutation", func(context.Context) {
					defer close(written)
					tasks.YieldSlot(func() { <-started })
					s.AddObject(versions[generation%2])
				})
				tasks.BlockTillDone()
				generation++
			}
			if tasks.Failures() != 0 {
				b.Fatal("worker panicked")
			}
		})
	}
}

func registrationFixture(mode string) (*Store, []manifest.NamedResource, [2]*manifest.Secret) {
	s := New()
	ids := make([]manifest.NamedResource, registrationReads)
	for i := range ids {
		name := fmt.Sprintf("optional-%05d", i)
		if mode == "repeated" {
			name = "optional-00000"
		}
		ids[i] = manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: name}
	}
	if mode == "produced" || mode == "repeated" {
		producer := &manifest.ResourceSet{Name: "producer", Namespace: "ns"}
		s.AddObject(producer)
		s.UpdateStatus(producer.Named(), StatusReady, "")
		s.RecordEmitted(producer.Named(), ids)
	}
	if mode != "absent" {
		for _, id := range slices.Compact(slices.Clone(ids)) {
			s.AddObject(&manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"value": "initial"}})
		}
	}
	versions := [2]*manifest.Secret{
		{Name: ids[0].Name, Namespace: "ns", StringData: map[string]any{"value": "a"}},
		{Name: ids[0].Name, Namespace: "ns", StringData: map[string]any{"value": "b"}},
	}
	return s, ids, versions
}
