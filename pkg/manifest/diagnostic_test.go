package manifest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDiagnosticFor_EntryContext(t *testing.T) {
	t.Parallel()
	for _, owner := range []NamedResource{{}, {Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "flux-system", Name: "sync"}, {Group: "fluxcd.controlplane.io", Kind: "FluxInstance", Namespace: "flux-system", Name: "flux"}} {
		t.Run(owner.String(), func(t *testing.T) {
			t.Parallel()
			entry := NewEntryKustomization(BootstrapSourceID, "clusters/test", owner)
			subject := DiagnosticFor(entry.Named(), entry)
			if subject.ID != entry.Named() || subject.Resource != owner || subject.Path != "clusters/test" || strings.Contains(subject.String(), "/entry") {
				t.Fatalf("entry diagnostic = %+v (%s)", subject, subject)
			}
			for _, copy := range []BaseManifest{entry.Clone(), entry.WithNamespace("projected")} {
				got := DiagnosticFor(copy.Named(), copy)
				if got.Resource != owner || got.Path != subject.Path {
					t.Fatalf("copy lost frozen context: %+v", got)
				}
			}
			plain := entry.Clone()
			plain.diagnosticOwner, plain.diagnosticPath, plain.hasDiagnostic = NamedResource{}, "", false
			if Equal(entry, plain) || entry.ProjectionCompatible(plain) {
				t.Fatal("attribution replacement reused stale provenance")
			}
			before, err := entry.RenderDocument()
			if err != nil {
				t.Fatal(err)
			}
			after, err := plain.RenderDocument()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, after); diff != "" {
				t.Fatal(diff)
			}
			if Fingerprint(entry.Content("source")) != Fingerprint(plain.Content("source")) {
				t.Fatal("display context entered render identity")
			}
			raw, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			ordinary, err := json.Marshal(plain)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != string(ordinary) {
				t.Fatal("display context entered serialized declaration")
			}
		})
	}
	authored := &Kustomization{Name: "entry", Namespace: "apps"}
	for _, obj := range []BaseManifest{nil, authored} {
		if got := DiagnosticFor(authored.Named(), obj); got.Resource != authored.Named() || got.Path != "" {
			t.Fatalf("authored entry changed: %+v", got)
		}
	}
}

func TestDiagnosticFor_StackAllocation(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		ks := &Kustomization{Name: "app", Namespace: "apps"}
		id := ks.Named()
		got := DiagnosticFor(id, ks)
		if got.ID != id || got.Resource != id || got.Path != "" {
			t.Fatal("ordinary diagnostic identity changed")
		}
	})
	if allocs != 0 {
		t.Fatalf("diagnostic allocations = %g, want 0", allocs)
	}
}
