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
		{"..", nil, nil},
		{"../", nil, nil},
		{"", nil, nil},
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
	want := []FileReader{{Reader: exact, Source: BootstrapSourceID}, {Reader: exact, Source: other}, {Reader: ancestor, Source: BootstrapSourceID}, {Reader: ancestor, Source: other}}
	index := &DiscoveryIndex{Readers: map[string][]FileReader{
		"missing/nested/input.yaml": want[:2],
		"missing":                   {{Reader: ancestor, Source: BootstrapSourceID}, {Reader: exact, Source: other, Exact: true}},
		".":                         {{Reader: ancestor, Source: other}, {Reader: exact, Source: other, Exact: true}},
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

func TestDiscoveryIndex_OwnersOfFilePrefixGroups(t *testing.T) {
	t.Parallel()
	root := NamedResource{Name: "root"}
	parentA := NamedResource{Name: "parent-a"}
	parentB := NamedResource{Name: "parent-b"}
	child := NamedResource{Name: "child"}
	index := &DiscoveryIndex{Claims: map[string][]NamedResource{
		"": {root}, "apps/": {parentB, parentA}, "apps/nested/": {child},
	}}
	for _, tc := range []struct {
		file              string
		owners, ancestors []NamedResource
	}{
		{"apps/nested/config.yaml", []NamedResource{child}, []NamedResource{parentA, parentB, root}},
		{"apps/nested", []NamedResource{child}, []NamedResource{parentA, parentB, root}},
		{"apps", []NamedResource{parentA, parentB}, []NamedResource{root}},
		{"apps2/config.yaml", []NamedResource{root}, nil},
		{"/", []NamedResource{root}, nil},
	} {
		t.Run(tc.file, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				owners, ancestors := index.OwnersOfFile(tc.file)
				if !slices.Equal(owners, tc.owners) || !slices.Equal(ancestors, tc.ancestors) {
					t.Fatalf("ownership = (%v, %v), want (%v, %v)", owners, ancestors, tc.owners, tc.ancestors)
				}
				if !slices.Equal(index.Claims["apps/"], []NamedResource{parentB, parentA}) {
					t.Fatal("query mutated borrowed peer group")
				}
			}
		})
	}
}

func TestDiscoveryIndex_OwnersOfFileUnclaimed(t *testing.T) {
	index := &DiscoveryIndex{Claims: map[string][]NamedResource{"other/": {{Name: "owner"}}}}
	var owners, ancestors []NamedResource
	allocations := testing.AllocsPerRun(1, func() {
		owners, ancestors = index.OwnersOfFile("apps/file.yaml")
	})
	if len(owners) != 0 || len(ancestors) != 0 {
		t.Fatalf("unclaimed ownership = (%v, %v)", owners, ancestors)
	}
	if allocations != 0 {
		t.Fatalf("unclaimed allocations = %g, want 0", allocations)
	}
}

func TestDiscoveryIndex_IsFileIndexed(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, recorded, requested string
		path, file                string
		want                      bool
	}{
		{name: "exact", recorded: "apps", requested: "apps", path: "target.yaml", want: true},
		{name: "inherited", requested: "apps", path: "target.yaml", want: true},
		{name: "other_namespace", recorded: "other", requested: "apps", path: "target.yaml"},
		{name: "bare_query", recorded: "apps", path: "target.yaml"},
		{name: "generated", recorded: "apps", requested: "apps", file: "target.yaml"},
		{name: "bare_generated", requested: "apps", file: "target.yaml"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id := NamedResource{Group: HelmReleaseDomain, Kind: KindHelmRelease, Namespace: tt.recorded, Name: "target"}
			index := &DiscoveryIndex{Records: map[NamedResource]DiscoveryRecord{id: {Path: tt.path, File: tt.file}}}
			id.Namespace = tt.requested
			if got := index.IsFileIndexed(id); got != tt.want {
				t.Fatalf("IsFileIndexed(%s) = %v, want %v", id, got, tt.want)
			}
		})
	}
	var index *DiscoveryIndex
	if index.IsFileIndexed(NamedResource{Name: "target", Namespace: "apps"}) {
		t.Fatal("nil index has a file-indexed target")
	}
}
