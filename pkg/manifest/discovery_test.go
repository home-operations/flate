package manifest

import (
	"slices"
	"testing"
)

func TestDiscoveryIndex_DataOwnersNamespaceBridge(t *testing.T) {
	raw := NamedResource{Kind: KindConfigMap, Name: "settings"}
	projected := raw
	projected.Namespace = "apps"
	owner := NamedResource{Kind: KindKustomization, Namespace: "apps", Name: "producer"}
	index := &DiscoveryIndex{Records: map[NamedResource]DiscoveryRecord{
		raw:       {Owners: []NamedResource{owner}},
		projected: {},
	}}
	for _, id := range []NamedResource{raw, projected} {
		if got := index.DataOwners(id); !slices.Equal(got, []NamedResource{owner}) {
			t.Errorf("DataOwners(%s) = %v; want [%s]", id, got, owner)
		}
	}
	delete(index.Records, raw)
	index.Records[projected] = DiscoveryRecord{Owners: []NamedResource{owner}}
	other := projected
	other.Namespace = "other"
	if got := index.DataOwners(other); len(got) != 0 {
		t.Errorf("unequal explicit namespaces matched: %v", got)
	}
}

func BenchmarkDiscoveryIndex_Lookup(b *testing.B) {
	id := NamedResource{Kind: KindConfigMap, Namespace: "apps", Name: "settings"}
	owner := NamedResource{Kind: KindKustomization, Namespace: "apps", Name: "producer"}
	index := &DiscoveryIndex{Records: map[NamedResource]DiscoveryRecord{
		id: {Owners: []NamedResource{owner}},
	}}
	b.ReportAllocs()
	for b.Loop() {
		r, ok := index.Lookup(id)
		if !ok || len(r.Owners) != 1 || len(index.DataOwners(id)) != 1 {
			b.Fatal("discovery facts missing")
		}
	}
}
