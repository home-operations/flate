package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/pkg/store"
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
		membershipResult1 := o.filter.Membership(id)
		needed, selected, _, _ := membershipResult1.Needed, membershipResult1.Selected, membershipResult1.Dirty, membershipResult1.Revision
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
		if _, included := o.filter.KeepNamespaces()["supply-only"]; included {
			membershipResult2 := o.filter.Membership(id)
			needed, selected, dirty, _ := membershipResult2.Needed, membershipResult2.Selected, membershipResult2.Dirty, membershipResult2.Revision
			t.Fatalf(
				"needed source selected its namespace: needed=%t selected=%t dirty=%t filter=%v",
				needed,
				selected,
				dirty,
				o.filter,
			)
		}
		membershipResult3 := o.filter.Membership(id)
		_, selected, _, _ := membershipResult3.Needed, membershipResult3.Selected, membershipResult3.Dirty, membershipResult3.Revision
		if selected {
			t.Fatal("source selected as a consumer")
		}
	}
	if len(result.Manifests) == 0 {
		t.Fatal("consumer produced no output")
	}
}
