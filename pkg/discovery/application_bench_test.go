package discovery

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkBindEntry_Evidence(b *testing.B) {
	for _, name := range []string{"publication", "equal_publication", "accepted_history"} {
		b.Run(name, func(b *testing.B) {
			const repo = "/virtual/entry-evidence-benchmark"
			input := &store.SourceArtifact{LocalPath: repo, FS: fstest.MapFS{}}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "applier", Namespace: "apps", Path: "./entry"}
			history := name == "accepted_history"
			if !history {
				st.AddObject(owner)
			}
			evidence := owner
			if name == "equal_publication" {
				evidence = owner.Clone()
			}
			record := manifest.DiscoveryRecord{Application: `{"apiVersion":"kustomize.toolkit.fluxcd.io/v1","kind":"Kustomization","metadata":{"name":"applier","namespace":"apps"},"spec":{"path":"./entry"}}`}
			b.ReportAllocs()
			for b.Loop() {
				a := &Admission{input: input, path: filepath.Join(repo, "entry")}
				if history {
					a.AddHistory(st, owner.Named(), record)
				} else {
					a.AddApplication(st, evidence)
				}
				root, _, err := a.BindEntry(b.Context(), st)
				if err != nil || root == nil {
					b.Fatalf("entry binding failed: %v", err)
				}
			}
		})
	}
}
