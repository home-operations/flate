package manifest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/google/go-cmp/cmp"
)

func TestDiagnosticFor_EntryContext(t *testing.T) {
	t.Parallel()
	for _, owner := range []NamedResource{{}, {Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "flux-system", Name: "sync"}, {Group: "fluxcd.controlplane.io", Kind: "FluxInstance", Namespace: "flux-system", Name: "flux"}} {
		t.Run(owner.String(), func(t *testing.T) {
			t.Parallel()
			testDiagnosticForEntryContextCase(t, &owner)
		},
		)
	}
	authored := &Kustomization{Name: "entry", Namespace: "apps"}
	for _, obj := range []BaseManifest{nil, authored} {
		{
			got := DiagnosticFor(authored.Named(), obj)
			assert.FatalIf(t, got.Resource != authored.Named() || got.Path != "", "authored entry changed: %+v", got)
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestManifest_Allocations(t *testing.T) {
	t.Run("DiagnosticFor_StackAllocation", func(t *testing.T) {
		allocs := testing.AllocsPerRun(100, func() {
			ks := &Kustomization{Name: "app", Namespace: "apps"}
			id := ks.Named()
			got := DiagnosticFor(id, ks)
			if got.ID != id || got.Resource != id || got.Path != "" {
				t.Fatal("ordinary diagnostic identity changed")
			}
		})

		assert.FatalIf(t, allocs != 0, "diagnostic allocations = %g, want 0", allocs)
	})
	t.Run("WorkRecorder_NestedAllocations", func(t *testing.T) {
		var root WorkRecorder
		var first *WorkRecord
		for range 2 {
			scope := root.Scope()
			for _, input := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
				record := scope.Record(WorkPreparation, input)
				if first == nil {
					first = record
				}
			}
		}
		allocations := testing.AllocsPerRun(10, func() {
			for range 2 {
				scope := root.Scope()
				for _, input := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
					scope.Record(WorkPreparation, input).Add(&WorkCounts{Attempts: 1, Completed: 1})
				}
			}
		})
		assert.Equal(t, allocations, float64(0))
		result := root.Scope().Snapshot(0)
		assert.Equal(t, len(result), 9)
		assert.Equal(t, cap(result), len(result))
		assert.Equal(t, result[0].Counts, WorkCounts{Attempts: 22, Completed: 22})
		first.Counts.Attempts = 100
		assert.Equal(t, result[0].Counts.Attempts, uint64(22))
	})
}

func testDiagnosticForEntryContextCase(t *testing.T, testCaseInput *NamedResource,
) {
	t.Helper()

	entry := NewEntryKustomization(BootstrapSourceID, "clusters/test", (*testCaseInput))
	subject := DiagnosticFor(entry.Named(), entry)

	assert.FatalIf(
		t,
		subject.ID != entry.Named() || subject.Resource != (*testCaseInput) || subject.Path != "clusters/test" ||
			strings.Contains(subject.String(), "/entry"),
		"entry diagnostic = %+v (%s)",
		subject,
		subject,
	)

	for _, cloned := range []BaseManifest{entry.Clone(), entry.WithNamespace("projected")} {
		got := DiagnosticFor(cloned.Named(), cloned)

		assert.FatalIf(
			t,
			got.Resource != (*testCaseInput) || got.Path != subject.Path,
			"copy lost frozen context: %+v",
			got,
		)
	}
	plain := entry.Clone()
	plain.diagnosticOwner, plain.diagnosticPath, plain.hasDiagnostic = NamedResource{}, "", false

	assert.FatalIf(
		t,
		Equal(entry, plain) || entry.ProjectionCompatible(plain),
		"%s",
		"attribution replacement reused stale provenance",
	)

	before, err := entry.RenderDocument()

	assert.NoError(t, err)

	after, err := plain.RenderDocument()

	assert.NoError(t, err)

	{
		diff := cmp.Diff(before, after)
		assert.FatalIf(t, diff != "", "%v", diff)
	}

	assert.FatalIf(
		t,
		Fingerprint(entry.Content("source", nil, false)) != Fingerprint(plain.Content("source", nil, false)),
		"%s",
		"display context entered render identity",
	)

	raw, err := json.Marshal(entry)

	assert.NoError(t, err)

	ordinary, err := json.Marshal(plain)

	assert.NoError(t, err)

	assert.FatalIf(t, string(raw) != string(ordinary), "%s", "display context entered serialized declaration")
}
