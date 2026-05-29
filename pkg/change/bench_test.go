package change

import (
	"fmt"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// BenchmarkFilterResolve_LargeRepo measures NewFilter (which runs
// resolve) against a 1000-KS store plus a small file-change set —
// this is the construction-time hot path the orchestrator pays once
// per change-driven run.
func BenchmarkFilterResolve_LargeRepo(b *testing.B) {
	s, sourceFiles := seedKSStore(1000)
	// Small change set: 3 files spread across distinct app subpaths.
	changeSet := NewSet([]string{
		"apps/app-7/ks.yaml",
		"apps/app-42/ks.yaml",
		"apps/app-913/ks.yaml",
	})
	repoRoot := "/repo"

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = NewFilter(changeSet, sourceFiles, repoRoot, s)
	}
}

// BenchmarkBuildOwnership measures the per-resolve buildOwnership
// helper against the same 1000-KS store. resolve() runs this once at
// the top of each Filter construction; isolating it surfaces the
// ksClaim + slices.SortStableFunc cost.
func BenchmarkBuildOwnership(b *testing.B) {
	s, _ := seedKSStore(1000)
	repoRoot := "/repo"

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = buildOwnership(s, repoRoot, nil)
	}
}

// BenchmarkTransitiveDeps measures transitiveDeps for a single
// Kustomization with several substituteFrom CMs and a sourceRef —
// the unit cost the resolve BFS pays per kept entry.
func BenchmarkTransitiveDeps(b *testing.B) {
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
			{Kind: manifest.KindConfigMap, Name: "cluster-secrets"},
			{Kind: manifest.KindConfigMap, Name: "common-config"},
			{Kind: manifest.KindConfigMap, Name: "common-secrets"},
		},
	}
	s.AddObject(ks)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = transitiveDeps(s, id)
	}
}

// seedKSStore creates a store with n Flux Kustomizations whose
// spec.paths are well-distributed under apps/. Returns the store plus
// a SourceFiles map keying each KS to its on-disk ks.yaml path so
// Filter.resolve has the same shape it sees in the orchestrator.
func seedKSStore(n int) (*store.Store, map[manifest.NamedResource]string) {
	s := store.New()
	sourceFiles := make(map[manifest.NamedResource]string, n)
	for i := range n {
		name := fmt.Sprintf("app-%d", i)
		path := fmt.Sprintf("./apps/app-%d", i)
		ks := &manifest.Kustomization{
			Name: name, Namespace: "flux-system",
			KustomizationSpec: kustomizev1.KustomizationSpec{Path: path},
			SourceKind:        manifest.KindGitRepository,
			SourceName:        "flux-system",
			SourceNamespace:   "flux-system",
		}
		s.AddObject(ks)
		sourceFiles[ks.Named()] = fmt.Sprintf("apps/app-%d/ks.yaml", i)
	}
	return s, sourceFiles
}
