package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func assertSuppliedSourceReference(t *testing.T, o *Orchestrator, result *Result, mode string) {
	t.Helper()
	id := o.cfg.Supply[0].Obj.Named()
	unused := id
	unused.Name = "unused"
	if _, known := o.store.GetStatus(unused); known {
		t.Fatal("unreferenced supplied source executed")
	}
	if mode == "selected reference" {
		if _, known := o.store.GetStatus(id); known {
			t.Fatal("explicit selector executed supplied source")
		}
		membershipResult1 := o.selection.Snapshot()
		needed, selected := membershipResult1.Reconcile(id), membershipResult1.Output(id)
		if needed || selected {
			t.Fatal("reference selected itself or unrelated consumers")
		}
		return
	}
	if status, known := o.store.GetStatus(id); !known || status.Status != store.StatusReady {
		t.Fatalf("supplied source not ready: %+v", status)
	}
	if !o.store.ReportHidden(id) || o.store.HasInventory(id) {
		t.Fatal("supplied source became output")
	}
	if mode != "full" {
		assertSuppliedSourceMembership(t, o, result, id)
	}
	if len(result.Manifests) == 0 {
		t.Fatal("consumer produced no output")
	}
}

func assertSuppliedSourceMembership(t *testing.T, o *Orchestrator, result *Result, id manifest.NamedResource) {
	t.Helper()
	if _, included := selectionNamespacesForTest(o.selection, o.store, o.index)["supply-only"]; included {
		membershipResult2 := o.selection.Snapshot()
		needed, selected, dirty := membershipResult2.Reconcile(
			id,
		), membershipResult2.Output(
			id,
		), membershipResult2.Changed(
			id,
		)
		t.Fatalf(
			"needed source selected its namespace: needed=%t selected=%t dirty=%t filter=%v",
			needed,
			selected,
			dirty,
			o.selection,
		)
	}
	roots := o.cfg.Selection.Roots
	if len(roots) == 0 {
		for kind := range manifest.Kinds() {
			if kind.Render {
				roots = append(roots, manifest.NamedResource{Group: kind.Group, Kind: kind.Kind})
			}
		}
	}
	projected, failures := Select(
		[]Rendered{{Orchestrator: o, Result: result}}, nil, roots, o.cfg.Selection.Labels,
	)
	if failures.Selected != nil || failures.Remaining != nil {
		t.Fatalf("source projection: selected=%v remaining=%v", failures.Selected, failures.Remaining)
	}
	for _, object := range projected[0].Objects {
		if object.Named() == id {
			t.Fatal("source selected as a consumer")
		}
	}
}
