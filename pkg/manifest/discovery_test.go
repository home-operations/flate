package manifest

import (
	"slices"
	"testing"
)

func TestDiscoveryIndex_CommittedClaimsExcludeCheckoutRoot(t *testing.T) {
	t.Parallel()
	checkout := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "checkout"}
	parent := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "committed-parent"}
	child := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "committed-child"}
	index := &DiscoveryIndex{Claims: map[string][]NamedResource{
		"": {checkout}, "../.flate-sources/pin/": {parent}, "../.flate-sources/pin/app/": {child},
	}}
	for _, tc := range []struct {
		file              string
		owners, ancestors []NamedResource
	}{
		{"../.flate-sources/pin/app/config.yaml", []NamedResource{child}, []NamedResource{parent}},
		{"../.flate-sources/other/app/config.yaml", nil, nil},
		{"apps/config.yaml", []NamedResource{checkout}, nil},
	} {
		t.Run(tc.file, func(t *testing.T) {
			t.Parallel()
			owners, ancestors := index.OwnersOfFile(tc.file)
			if !slices.Equal(owners, tc.owners) || !slices.Equal(ancestors, tc.ancestors) {
				t.Fatalf("ownership for %s = (%v, %v), want (%v, %v)", tc.file, owners, ancestors, tc.owners, tc.ancestors)
			}
		})
	}
}

func TestDiscoveryIndex_ReadersOfFileReplay(t *testing.T) {
	t.Parallel()
	exact := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "exact"}
	ancestor := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "ancestor"}
	other := BootstrapSourceID
	other.Name = "other"
	want := []FileReader{{Reader: exact, Source: BootstrapSourceID}, {Reader: exact, Source: other}, {Reader: ancestor, Source: BootstrapSourceID}}
	index := &DiscoveryIndex{Readers: map[string][]FileReader{
		"missing/nested/input.yaml": want[:2],
		"missing":                   {{Reader: ancestor, Source: BootstrapSourceID}, {Reader: exact, Source: other, Exact: true}},
	}}
	readers := index.ReadersOfFile("missing/nested/input.yaml")
	count := 0
	for reader := range readers {
		count++
		if reader != want[0] {
			t.Fatalf("first reader = %v, want %v", reader, want[0])
		}
		break
	}
	if count != 1 {
		t.Fatalf("early stop yielded %d readers, want 1", count)
	}
	for range 2 {
		if got := slices.Collect(readers); !slices.Equal(got, want) {
			t.Fatalf("reader query changed on replay: %v", got)
		}
	}
	owners, ancestors := index.OwnersOfFile("missing/nested/input.yaml")
	if !slices.Equal(owners, []NamedResource{ancestor, exact}) || len(ancestors) != 0 {
		t.Fatalf("source pairs changed owner projection: %v, %v", owners, ancestors)
	}
}
