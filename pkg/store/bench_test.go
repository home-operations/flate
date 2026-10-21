package store

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// BenchmarkAddObject_Cold measures AddObject on a fresh id — the
// no-dedup path: RLock-miss, then WLock-insert. Fresh store per
// iteration so the path stays the cold one.
func BenchmarkAddObject_Cold(b *testing.B) {
	// Pre-build the objects so allocation cost is fixture, not
	// measured. Each iteration adds one fresh id.
	objs := make([]*manifest.ConfigMap, b.N)
	for i := range objs {
		objs[i] = &manifest.ConfigMap{Name: fmt.Sprintf("cm-%d", i), Namespace: "ns"}
	}
	s := New()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		s.AddObject(objs[i])
	}
}

// BenchmarkAddObject_Warm measures AddObject for an existing id
// with an identical payload — the reflect.DeepEqual short-circuit
// path. This is the dominant cost when a parent KS re-renders and
// re-emits unchanged children.
func BenchmarkAddObject_Warm(b *testing.B) {
	s := New()
	cm := &manifest.ConfigMap{Name: "warm", Namespace: "ns", Data: map[string]any{"k": "v"}}
	s.AddObject(cm)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s.AddObject(cm)
	}
}

// BenchmarkListObjects_Sorted measures an exact Kustomization group/kind listing
// against a 1000-KS store. The sort dominates; the byName-index walk
// supplies the inputs.
func BenchmarkListObjects_Sorted(b *testing.B) {
	s := New()
	for i := range 1000 {
		s.AddObject(&manifest.Kustomization{
			Name: fmt.Sprintf("app-%04d", i), Namespace: "flux-system",
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out := s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)
		if len(out) != 1000 {
			b.Fatalf("expected 1000, got %d", len(out))
		}
	}
}

// BenchmarkSetArtifact_DeepEqual measures SetArtifact against a
// previous identical artifact — the no-op dedup path that decides
// the write is redundant. Pre-Item-7 this was reflect.DeepEqual
// across the full structure; Item 7 adds a pointer-identity
// short-circuit ahead of DeepEqual so the SAME-pointer re-set (the
// shape this bench drives — fetchers cache their own SourceArtifact
// and re-publish it on every tick) skips reflection entirely.
//
// The bench keeps the same name + shape as the Phase 0 baseline so
// the delta in PR descriptions is apples-to-apples. The
// distinct-pointer dedup path (KS / HR re-emit with re-decoded
// maps) is measured by BenchmarkSetArtifact_DistinctDedup below.
func BenchmarkSetArtifact_DeepEqual(b *testing.B) {
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	// Build a rendered-style artifact with realistic shape so DeepEqual
	// has actual work to do (not a one-field comparison).
	manifests := make([]map[string]any, 0, 20)
	for i := range 20 {
		manifests = append(manifests, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      fmt.Sprintf("cm-%d", i),
				"namespace": "default",
			},
			"data": map[string]any{
				"key": fmt.Sprintf("value-%d", i),
			},
		})
	}
	art := &Emission{Manifests: manifests}
	s.SetArtifact(id, art)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s.SetArtifact(id, art)
	}
}

// BenchmarkSetArtifact_DistinctDedup measures the KS / HR re-emit
// dedup path: a fresh-pointer artifact with byte-identical content.
// Item 7's pointer-identity short-circuit can't help here (pointers
// differ); reflect.DeepEqual still runs. Companion to
// BenchmarkSetArtifact_DeepEqual which measures the same-pointer
// fast path.
func BenchmarkSetArtifact_DistinctDedup(b *testing.B) {
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	manifests := make([]map[string]any, 0, 20)
	for i := range 20 {
		manifests = append(manifests, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      fmt.Sprintf("cm-%d", i),
				"namespace": "default",
			},
			"data": map[string]any{
				"key": fmt.Sprintf("value-%d", i),
			},
		})
	}
	art := &Emission{Manifests: manifests}
	s.SetArtifact(id, art)
	// Distinct-pointer twin with shared sub-slice — matches the
	// re-emit shape where docs are decoded into fresh outer
	// structs but their backing maps survive across reconciles.
	twin := &Emission{Manifests: art.Manifests}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s.SetArtifact(id, twin)
	}
}

func BenchmarkUpdateFailure_Write(b *testing.B) {
	id := testutil.HelmReleaseID("", "consumer")
	dep := testutil.HelmReleaseID("", "dep")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"Primary", manifest.ErrInput},
		{"Dependency", &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			st := New()
			st.UpdateFailure(id, tc.err)
			b.ReportAllocs()
			for b.Loop() {
				st.UpdateStatus(id, StatusPending, "", "retrying")
				st.UpdateFailure(id, tc.err)
			}
		})
	}
}

func BenchmarkStatusInfo_Read(b *testing.B) {
	id := testutil.HelmReleaseID("", "consumer")
	dep := testutil.HelmReleaseID("", "dep")
	for _, tc := range []struct {
		name  string
		write func(*Store)
	}{
		{"Ready", func(st *Store) { st.UpdateStatus(id, StatusReady, "", "done") }},
		{"Primary", func(st *Store) { st.UpdateFailure(id, manifest.ErrInput) }},
		{"Dependency", func(st *Store) {
			st.UpdateFailure(id, &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}})
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			st := New()
			tc.write(st)
			b.ReportAllocs()
			for b.Loop() {
				info, _ := st.GetStatus(id)
				_ = info.IsDependencyFailure()
			}
		})
	}
}

func BenchmarkStatusFlip(b *testing.B) {
	for _, withListener := range []bool{false, true} {
		name := "NoListener"
		if withListener {
			name = "OneListener"
		}
		b.Run(name, func(b *testing.B) {
			st := New()
			id := testutil.HelmReleaseID("a", "b")
			st.AddObject(&manifest.HelmRelease{Name: id.Name, Namespace: id.Namespace})
			if withListener {
				st.AddListener(EventStatusUpdated, func(manifest.NamedResource, any) {}, false)
			}
			b.ReportAllocs()
			for b.Loop() {
				st.UpdateStatus(id, StatusPending, "", "x")
				st.UpdateStatus(id, StatusReady, "", "y")
			}
		})
	}
}

func BenchmarkGetStatus(b *testing.B) {
	st := New()
	id := testutil.HelmReleaseID("a", "b")
	st.AddObject(&manifest.HelmRelease{Name: id.Name, Namespace: id.Namespace})
	st.UpdateStatus(id, StatusReady, "", "y")
	b.ReportAllocs()
	for b.Loop() {
		info, _ := st.GetStatus(id)
		if info.Status != StatusReady {
			b.Fatal("expected ready status")
		}
	}
}
