package change

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
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
	index := filterFixture{files: sourceFiles, root: "/repo", objects: s}.index()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = NewFilter(changeSet, index, s)
	}
}

func BenchmarkFilterResolve_ProducerChain(b *testing.B) {
	input := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "input"}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input: {File: "input.yaml"},
	}}
	previous := input
	for i := range 8 {
		declaration := manifest.NamedResource{Group: "example.test", Kind: "Producer", Namespace: "apps", Name: fmt.Sprintf("producer-%d", i)}
		output := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: fmt.Sprintf("output-%d", i)}
		file := fmt.Sprintf("producer-%d.yaml", i)
		index.Records[declaration] = manifest.DiscoveryRecord{File: file, Refs: []manifest.NamedResource{previous}}
		index.Records[output] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Declaration: declaration, File: file}}}
		previous = output
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	index.Records[consumer] = manifest.DiscoveryRecord{File: "consumer.yaml", Refs: []manifest.NamedResource{previous}}
	objects := make(mapLister)
	for id := range index.Records {
		objects[id] = refObj(id)
	}
	changes := NewSet([]string{"input.yaml"})
	b.ReportAllocs()
	for b.Loop() {
		_ = NewFilter(changes, index, objects)
	}
}

// BenchmarkBuildOwnership measures canonical claim grouping for 1000 Kustomizations.
// Discovery prepares this immutable state before Filter construction.
func BenchmarkBuildOwnership(b *testing.B) {
	s, _ := seedKSStore(1000)
	l := loader.New(s)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		claims := l.Claims("/repo")
		byPrefix := make(map[string][]manifest.NamedResource, len(claims))
		for _, c := range claims {
			byPrefix[c.Prefix] = append(byPrefix[c.Prefix], c.ID)
		}
	}
}

// BenchmarkTransitiveDeps measures selection references for a single
// Kustomization with several substituteFrom CMs and a sourceRef —
// the unit cost the resolve BFS pays per kept entry.
func BenchmarkTransitiveDeps(b *testing.B) {
	s := store.New()
	id := testutil.KustomizationID("flux-system", "apps")
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system"},
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
			{Kind: manifest.KindConfigMap, Name: "cluster-secrets"},
			{Kind: manifest.KindConfigMap, Name: "common-config"},
			{Kind: manifest.KindConfigMap, Name: "common-secrets"},
		}},
	}
	s.AddObject(ks)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var refs [8]manifest.NamedResource
		if deps := appendReferences(refs[:0], s.GetObject(id), false); len(deps) != 5 {
			b.Fatalf("dependencies=%v", deps)
		}
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
			Path:      path,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system"},
		}
		s.AddObject(ks)
		sourceFiles[ks.Named()] = fmt.Sprintf("apps/app-%d/ks.yaml", i)
	}
	return s, sourceFiles
}

func BenchmarkDetect(b *testing.B) {
	for _, name := range []string{"separate", "nested", "large", "vendor"} {
		b.Run(name, func(b *testing.B) {
			after := b.TempDir()
			before := b.TempDir()
			if name == "nested" {
				before = filepath.Join(after, ".cache", "baselines", "base")
			}
			if name == "large" || name == "vendor" {
				prefix := "apps"
				if name == "vendor" {
					prefix = "vendor"
				}
				for i := range 12000 {
					rel := fmt.Sprintf("%s/app-%d/file-%d.yaml", prefix, i/8, i)
					testutil.WriteFile(b, before, rel, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: example}\n")
					testutil.WriteFile(b, after, rel, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: example}\n")
				}
			}
			testutil.WriteFile(b, before, "mod.yaml", "AAA")
			testutil.WriteFile(b, after, "mod.yaml", "BBB")
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Detect(b.Context(), testInput(before), testInput(after)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCollectInputs_LargeFile(b *testing.B) {
	root := b.TempDir()
	testutil.WriteFile(b, root, "input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: large, namespace: apps}\ndata:\n  value: "+strings.Repeat("x", 256*1024)+"\n")
	input := os.DirFS(root)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "large"}
	b.ReportAllocs()
	for b.Loop() {
		var set Set
		if err := set.CollectInputs(b.Context(), input, "input.yaml"); err != nil {
			b.Fatal(err)
		}
		if _, ok := set.inputs[id]; !ok || len(set.inputs) != 1 {
			b.Fatalf("collected inputs=%v", set.inputs)
		}
	}
}
