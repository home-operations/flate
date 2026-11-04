package kustomization

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func BenchmarkSubstitutionProvider_Lookup(b *testing.B) {
	for _, supplied := range []bool{false, true} {
		name := "repository"
		if supplied {
			name = "supplied"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkSubstitutionLookup(b, &supplied)
		})
	}
}

func benchmarkSubstitutionLookup(b *testing.B, supplied *bool) {
	b.Helper()
	st := store.New()
	cm := &manifest.ConfigMap{Name: "external", Namespace: "apps"}
	c := New(st, nil, nil)
	opts := Options{}
	if *supplied {
		st.AddDeclaration(
			store.Declaration{Obj: cm, Doc: testutil.ObjectDocument(cm), Origin: manifest.SuppliedDeclaration},
		)
	} else {
		st.AddObject(cm)
	}
	c.Configure(opts, sourcecache.RetryConfig{})
	execution := c.newExecution(testutil.KustomizationID("apps", "consumer"))
	defer execution.run.Close()
	provider := values.NewStoreProvider(&execution.run.Reads)
	expected := provider.Data(cm.Named())
	if expected == nil {
		b.Fatal("lookup failed")
	}
	b.ReportAllocs()
	for b.Loop() {
		if provider.Data(cm.Named()) != expected {
			b.Fatal("lookup failed")
		}
	}
}
