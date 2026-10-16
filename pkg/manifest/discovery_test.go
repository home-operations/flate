package manifest

import (
	"slices"
	"testing"
)

func TestDiscoveryIndex_ReadersOfFileReplay(t *testing.T) {
	t.Parallel()
	exact := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "exact"}
	ancestor := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "ancestor"}
	index := &DiscoveryIndex{Readers: map[string][]NamedResource{
		"missing/nested/input.yaml": {exact},
		"missing":                   {ancestor},
	}}
	readers := index.ReadersOfFile("missing/nested/input.yaml")
	for range 2 {
		if got := slices.Collect(readers); !slices.Equal(got, []NamedResource{exact, ancestor}) {
			t.Fatalf("reader query changed on replay: %v", got)
		}
	}
}
