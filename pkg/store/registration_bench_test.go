package store

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
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
				s, ids, versions := registrationFixture(b, mode)
				tasks := task.NewBounded(workers)
				consumer := testutil.KustomizationID("ns", "consumer")
				s.AddObject(&manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace})
				other := &manifest.Secret{Name: "unqueried", Namespace: "ns"}
				unrelated := &manifest.ConfigMap{Name: "unrelated", Namespace: "ns"}
				reads := make([]int, workers-1)
				state := &struct {
					sets   []ReadSet
					inputs [][]Input
				}{make([]ReadSet, workers-1), make([][]Input, workers-1)}
				for worker := range state.inputs {
					state.inputs[worker] = make([]Input, len(ids)/(workers-1)+1)
				}
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
							set := &state.sets[worker]
							set.Init(s, consumer, nil)
							clear(state.inputs[worker])
							for i := worker; i < len(ids); i += workers - 1 {
								input := &state.inputs[worker][i/(workers-1)]
								input.Ref.NamedResource = ids[i]
								_, present := set.Capture(input).(*manifest.Secret)
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
					for i := range state.sets {
						state.sets[i].Valid()
						state.sets[i].Retire()
					}
					for range s.Sweep() {
					}
					s.UpdateStatus(consumer, StatusReady, "", "")
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
			s, ids, versions := registrationFixture(b, "repeated")
			consumers := make([]manifest.NamedResource, registrationReads)
			for i := range consumers {
				consumers[i] = testutil.KustomizationID("ns", fmt.Sprintf("consumer-%05d", i))
				s.AddObject(&manifest.Kustomization{Name: consumers[i].Name, Namespace: consumers[i].Namespace})
				s.UpdateStatus(consumers[i], StatusReady, "", "")
			}
			tasks := task.NewBounded(workers)
			state := &struct {
				sets   []ReadSet
				inputs []Input
			}{make([]ReadSet, len(consumers)), make([]Input, len(consumers))}
			generation := 0
			b.ReportAllocs()
			for b.Loop() {
				for worker := range workers {
					tasks.Go(b.Context(), "observe", func(context.Context) {
						for i := worker; i < len(consumers); i += workers {
							clear(state.inputs[i : i+1])
							state.sets[i].Init(s, consumers[i], nil)
							state.inputs[i].Ref.NamedResource = ids[i]
							state.sets[i].Capture(&state.inputs[i])
							s.UpdateStatus(consumers[i], StatusReady, "", "")
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
							state.sets[i].GetObject(ids[i])
							if i == 0 {
								start()
							}
							s.UpdateStatus(consumers[i], StatusPending, "", "completion")
							s.UpdateStatus(consumers[i], StatusReady, "", "")
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
				for i := range state.sets {
					state.sets[i].Valid()
					state.sets[i].Retire()
				}
				for range s.Sweep() {
				}
				generation++
			}
			if tasks.Failures() != 0 {
				b.Fatal("worker panicked")
			}
		})
	}
}

func registrationFixture(tb testing.TB, mode string) (*Store, []manifest.NamedResource, [2]*manifest.Secret) {
	tb.Helper()
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
		s.UpdateStatus(producer.Named(), StatusReady, "", "")
		recordForTest(tb, s, producer.Named(), ids)
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
