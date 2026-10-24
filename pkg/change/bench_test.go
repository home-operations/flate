package change

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// BenchmarkFilterResolve_LargeRepo measures construction for 1000 Kustomizations.
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
		_ = NewFilter(changeSet, index, s, "/repo")
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
	objects := make(filtertest.MapLister)
	for id := range index.Records {
		objects[id] = refObj(id)
	}
	changes := NewSet([]string{"input.yaml"})
	b.ReportAllocs()
	for b.Loop() {
		_ = NewFilter(changes, index, objects, "")
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

// BenchmarkTransitiveDeps measures content-input projection for a kept Kustomization.
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
		for range manifest.ContentInputs(s.GetObject(id), false) {
		}
	}
}

func BenchmarkAddEmitted_PrimaryFanOut(b *testing.B) {
	const fanOut = 64
	for _, unrelated := range []int{1000, 4000} {
		b.Run(fmt.Sprintf("unrelated=%d/children=%d", unrelated, fanOut), func(b *testing.B) {
			s, files := seedKSStore(unrelated + 1)
			index := filterFixture{files: files, root: "/repo", objects: s}.index()
			changes := NewSet([]string{"apps/app-0/ks.yaml"})
			parent := testutil.KustomizationID("flux-system", "app-0")
			children := make([]store.Child, fanOut)
			for i := range children {
				children[i] = store.Child{Obj: &manifest.ConfigMap{Name: fmt.Sprintf("child-%d", i), Namespace: "apps"}}
			}
			filter := NewFilter(changes, index, s, "/repo")
			next := 0
			b.ReportAllocs()
			for b.Loop() {
				if next == fanOut {
					b.StopTimer()
					for _, child := range children {
						delete(filter.keep, child.Obj.Named())
					}
					next = 0
					b.StartTimer()
				}
				filter.AddEmitted(parent, children[next], filter.objs)
				next++
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/child")
			b.ReportMetric(1, "children/op")
		})
	}
}

func BenchmarkFilterResolve_OneSidedSuppliers(b *testing.B) {
	for _, suppliers := range []int{1, 16} {
		for _, match := range []bool{false, true} {
			b.Run(fmt.Sprintf("suppliers=%d/match=%t", suppliers, match), func(b *testing.B) {
				changes, st, index, root := oneSidedSuppliers(b, suppliers)
				name := "unrelated"
				if match {
					name = "optional"
				}
				consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: name, Optional: true}}}}
				st.AddObject(consumer)
				index.Records[consumer.Named()] = manifest.DiscoveryRecord{File: "flux/consumer.yaml"}
				selected := 0
				b.ReportAllocs()
				for b.Loop() {
					filter := NewFilter(changes, index, st, root)
					if filter.ShouldReconcile(consumer.Named()) {
						selected++
					}
				}
				if !match && selected != 0 {
					b.Fatal("nonmatching supplier selected consumer")
				}
				b.ReportMetric(float64(selected)/float64(b.N), "selected/op")
			})
		}
	}
}

func BenchmarkAddEmitted_OneSidedSuppliers(b *testing.B) {
	for _, suppliers := range []int{1, 16} {
		for _, match := range []bool{false, true} {
			b.Run(fmt.Sprintf("suppliers=%d/match=%t", suppliers, match), func(b *testing.B) {
				changes, st, index, root := oneSidedSuppliers(b, suppliers)
				emitter := &manifest.Kustomization{Name: "emitter", Namespace: "apps", Path: "unchanged"}
				st.AddObject(emitter)
				index.Records[emitter.Named()] = manifest.DiscoveryRecord{File: "flux/emitter.yaml"}
				filter := NewFilter(changes, index, st, root)
				name := "unrelated"
				if match {
					name = "optional"
				}
				var children [64]store.Child
				for i := range children {
					children[i] = store.Child{Obj: &manifest.Kustomization{Name: fmt.Sprintf("late-%d", i), Namespace: "apps", Path: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: name, Optional: true}}}}}
				}
				setup := selection{filter: filter, objects: st, child: children[0]}
				input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: name, Namespace: "apps"}
				if filter.ShouldReconcile(emitter.Named()) || filter.ShouldReconcile(input) || filter.ShouldReconcile(children[0].Obj.Named()) || setup.affectedRoot(children[0].Obj) {
					b.Fatal("supplier scan would be bypassed")
				}
				for _, production := range st.IncomingProductions(children[0], index) {
					relative, indexed := setup.coordinates(production)
					if setup.changedProduction(production, relative, indexed) {
						b.Fatal("incoming production would bypass supplier scan")
					}
				}
				selected, next := 0, 0
				b.ReportAllocs()
				for b.Loop() {
					if next == len(children) {
						b.StopTimer()
						clear(filter.keep)
						filter.revision = 0
						next = 0
						b.StartTimer()
					}
					child := children[next]
					var reads store.ReadSet
					reads.Init(st, child.Obj.Named(), nil)
					filter.AddEmitted(emitter.Named(), child, &reads)
					reads.Close()
					if filter.ShouldReconcile(child.Obj.Named()) {
						selected++
					}
					next++
				}
				if !match && selected != 0 {
					b.Fatal("nonmatching supplier admitted child")
				}
				b.ReportMetric(float64(selected)/float64(b.N), "selected/op")
			})
		}
	}
}

func oneSidedSuppliers(b *testing.B, count int) (*Set, *store.Store, *manifest.DiscoveryIndex, string) {
	b.Helper()
	_, stores, roots, source := optionalSupplierPair(b, "apps")
	for i := 1; i < count; i++ {
		supplier := *source
		supplier.Name = fmt.Sprintf("supplier-%d", i)
		for _, st := range stores {
			st.AddObject(&supplier)
			st.SetArtifact(supplier.Named(), st.GetArtifact(source.Named()))
		}
	}
	changes, err := Qualify(b.Context(), nil, stores[0], stores[1])
	if err != nil {
		b.Fatal(err)
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	if !NewFilter(changes, index, stores[1], roots[1]).hasInputs {
		b.Fatal("supplier fixture has no one-sided inputs")
	}
	return changes, stores[1], index, roots[1]
}

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
		if err := set.CollectInputs(b.Context(), input, "input.yaml", false); err != nil {
			b.Fatal(err)
		}
		if _, ok := set.inputs[id]; !ok || len(set.inputs) != 1 {
			b.Fatalf("collected inputs=%v", set.inputs)
		}
	}
}
