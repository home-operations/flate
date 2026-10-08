package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestSetBlocked_ReplaceAndDelete(t *testing.T) {
	st := New()
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "consumer"}
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "dep"}
	deps := []manifest.NamedResource{dep}
	if st.HasBlocked(id) {
		t.Fatal("unblocked resource has blockers")
	}
	st.SetBlocked(id, deps)
	if !st.HasBlocked(id) {
		t.Fatal("dependency-derived failure has no blockers")
	}
	deps[0].Name = "mutated"
	got := st.BlockedBy(id)
	if !slices.Equal(got, []manifest.NamedResource{dep}) {
		t.Fatalf("caller mutated blockers: %v", got)
	}
	got[0].Name = "mutated again"
	if st.BlockedBy(id)[0] != dep {
		t.Fatal("reader mutated blockers")
	}
	st.SetBlocked(id, nil)
	if st.HasBlocked(id) {
		t.Fatal("cleared dependency-derived failure retains blockers")
	}
	if got := st.BlockedBy(id); got != nil {
		t.Fatalf("empty blocker set was not deleted: %v", got)
	}
	if _, ok := st.shardFor(id).blocked[id]; ok {
		t.Fatal("empty blocker entry remains")
	}
}
