package kustomization

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func BenchmarkCollectDeps(b *testing.B) {
	id := testutil.KustomizationID("apps", "target")
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: id.Name, Namespace: id.Namespace}}}
	st := store.New()
	c := New(st, nil, nil, false)
	c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"target.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}}}, st)}, sourcecache.RetryConfig{})
	execution := c.newExecution(ks.Named())
	defer execution.run.Close()
	b.ReportAllocs()
	for b.Loop() {
		execution.collectDeps(ks)
	}
}

func BenchmarkSubstitutionProvider_Lookup(b *testing.B) {
	for _, supplied := range []bool{false, true} {
		name := "repository"
		if supplied {
			name = "supplied"
		}
		b.Run(name, func(b *testing.B) {
			st := store.New()
			cm := &manifest.ConfigMap{Name: "external", Namespace: "apps"}
			c := New(st, nil, nil, false)
			opts := Options{}
			if supplied {
				opts.SubstituteFrom = map[manifest.NamedResource]manifest.BaseManifest{cm.Named(): cm}
			} else {
				st.AddObject(cm)
			}
			c.Configure(opts, sourcecache.RetryConfig{})
			execution := c.newExecution(testutil.KustomizationID("apps", "consumer"))
			defer execution.run.Close()
			provider := values.NewStoreProvider(&execution.run.Reads, c.WipeSecrets, c.substitutionSources)
			b.ReportAllocs()
			for b.Loop() {
				if provider.Data(cm.Named()) != cm {
					b.Fatal("lookup failed")
				}
			}
		})
	}
}

func TestCollectDeps_Allocations(t *testing.T) {
	id := testutil.KustomizationID("apps", "target")
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: id.Name, Namespace: id.Namespace}}}
	st := store.New()
	c := New(st, nil, nil, false)
	c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"target.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}}}, st)}, sourcecache.RetryConfig{})
	execution := c.newExecution(ks.Named())
	defer execution.run.Close()
	if allocs := testing.AllocsPerRun(100, func() { execution.collectDeps(ks) }); allocs > 3 {
		t.Fatalf("dependency collection allocated %g times, want at most 3", allocs)
	}
}
