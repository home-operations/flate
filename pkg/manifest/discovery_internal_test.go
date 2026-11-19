package manifest

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
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
			ownership := index.OwnersOfFile(tc.file)
			owners, ancestors := ownership.Owners, ownership.Ancestors
			assert.FatalIf(
				t,
				!slices.Equal(owners, tc.owners) || !slices.Equal(ancestors, tc.ancestors),
				"ownership for %s = (%v, %v), want (%v, %v)",
				tc.file,
				owners,
				ancestors,
				tc.owners,
				tc.ancestors,
			)
		})
	}
}

func TestDiscoveryIndex_ReadersOfFileReplay(t *testing.T) {
	t.Parallel()
	exact := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "exact"}
	ancestor := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "ancestor"}
	other := BootstrapSourceID
	other.Name = "other"
	named := FileReader{
		Reader:      NamedResource{Name: "named"},
		Source:      BootstrapSourceID,
		Declaration: NamedResource{Kind: KindConfigMap, Name: "settings"},
	}
	want := []FileReader{
		{Reader: exact, Source: BootstrapSourceID},
		{Reader: exact, Source: other},
		named,
		{Reader: ancestor, Source: BootstrapSourceID},
		{Reader: ancestor, Source: other},
	}
	index := &DiscoveryIndex{Readers: map[string][]FileReader{
		"missing/nested/input.yaml": want[:3],
		"missing": {
			{Reader: ancestor, Source: BootstrapSourceID},
			{Reader: exact, Source: other, Exact: true},
		},
		".": {{Reader: ancestor, Source: other}, {Reader: exact, Source: other, Exact: true}},
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
	assert.FatalIf(t, count != 1, "early stop yielded %d readers, want 1", count)
	for range 2 {
		{
			got := slices.Collect(readers)
			assert.FatalIf(t, !slices.Equal(got, want), "reader query changed on replay: %v", got)
		}
	}
	ownership2 := index.OwnersOfFile("missing/nested/input.yaml")
	owners, ancestors := ownership2.Owners, ownership2.Ancestors
	assert.FatalIf(
		t,
		!slices.Equal(owners, []NamedResource{ancestor, exact}) || len(ancestors) != 0,
		"source pairs changed owner projection: %v, %v",
		owners,
		ancestors,
	)
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
				ownership3 := index.OwnersOfFile(tc.file)
				owners, ancestors := ownership3.Owners, ownership3.Ancestors
				assert.FatalIf(
					t,
					!slices.Equal(owners, tc.owners) || !slices.Equal(ancestors, tc.ancestors),
					"ownership = (%v, %v), want (%v, %v)",
					owners,
					ancestors,
					tc.owners,
					tc.ancestors,
				)
				assert.FatalIf(
					t,
					!slices.Equal(index.Claims["apps/"], []NamedResource{parentB, parentA}),
					"%s",
					"query mutated borrowed peer group",
				)
			}
		})
	}
}

//nolint:paralleltest // testing.AllocsPerRun measures process-wide allocation counters for unclaimed file ownership.
func TestDiscoveryIndex_OwnersOfFileUnclaimed(t *testing.T) {
	index := &DiscoveryIndex{Claims: map[string][]NamedResource{"other/": {{Name: "owner"}}}}
	var owners, ancestors []NamedResource
	allocations := testing.AllocsPerRun(1, func() {
		ownership4 := index.OwnersOfFile("apps/file.yaml")
		owners, ancestors = ownership4.Owners, ownership4.Ancestors
	})
	assert.FatalIf(t, len(owners) != 0 || len(ancestors) != 0, "unclaimed ownership = (%v, %v)", owners, ancestors)
	assert.FatalIf(t, allocations != 0, "unclaimed allocations = %g, want 0", allocations)
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
			{
				got := index.IsFileIndexed(id)
				assert.FatalIf(t, got != tt.want, "IsFileIndexed(%s) = %v, want %v", id, got, tt.want)
			}
		})
	}
	id := NamedResource{Group: "apps", Kind: "Deployment", Name: "inventory-only"}
	accepted := discoveryRecordFixture(t)
	indexWithEvidence := &DiscoveryIndex{
		Records: map[NamedResource]DiscoveryRecord{{Name: "accepted"}: accepted},
		Declarations: map[NamedResource]map[string]FileDeclarations{
			BootstrapSourceID: {
				"outside.yaml": {Present: true, Complete: true, Objects: map[NamedResource]string{id: "fingerprint"}},
			},
		},
	}
	assert.Equal(t, indexWithEvidence.IsFileIndexed(id), false)
	_, found := indexWithEvidence.Lookup(id)
	assert.Equal(t, found, false)
	assert.Equal(t, len(indexWithEvidence.Claims), 0)
	assert.Equal(t, len(indexWithEvidence.OwnersOfFile("outside.yaml").Owners), 0)
	assert.Equal(t, len(indexWithEvidence.Readers), 0)
	assert.Equal(t, accepted.Equal(indexWithEvidence.Records[NamedResource{Name: "accepted"}]), true)
	assert.Equal(t, indexWithEvidence.Records[NamedResource{Name: "accepted"}].Equal(accepted), true)
	var index *DiscoveryIndex
	assert.FatalIf(
		t,
		index.IsFileIndexed(NamedResource{Name: "target", Namespace: "apps"}),
		"%s",
		"nil index has a file-indexed target",
	)
}

func TestCaptureClaims_BorrowsUnchangedGroups(t *testing.T) {
	t.Parallel()
	owner := &Kustomization{Name: "replacement", Path: "before"}
	stable := (&Kustomization{Name: "stable"}).Named()
	another := (&Kustomization{Name: "another"}).Named()
	index := &DiscoveryIndex{
		Records: map[NamedResource]DiscoveryRecord{owner.Named(): {Declaration: owner}},
		Claims: map[string][]NamedResource{
			"before/":    {owner.Named(), stable},
			"unrelated/": {stable},
			"unsorted/":  {stable, another, stable},
		},
	}
	replacement := *owner
	replacement.Path = "after"
	captured := index.CaptureClaims([]ClaimInput{{Declaration: &replacement, Base: "after"}})
	assert.Diff(t, captured.Claims, map[string][]NamedResource{
		"before/":    {stable},
		"after/":     {owner.Named()},
		"unrelated/": {stable},
		"unsorted/":  {another, stable},
	})
	assert.Equal(t, &captured.Claims["unrelated/"][0], &index.Claims["unrelated/"][0])
	assert.Diff(t, index.Claims["before/"], []NamedResource{owner.Named(), stable})
	assert.Diff(t, index.Claims["unsorted/"], []NamedResource{stable, another, stable})
	assert.Equal(t, index.Records[owner.Named()].Declaration, BaseManifest(owner))
	assert.Equal(t, captured.Records[owner.Named()].Declaration, BaseManifest(&replacement))
}

func TestClaimOwners_SourceQualifiedGroups(t *testing.T) {
	t.Parallel()
	parent := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "apps", Name: "parent"}
	deep := parent
	deep.Name = "deep"
	peer := parent
	peer.Name = "peer"
	other := parent
	other.Name = "other-source"
	projected := parent
	projected.Name = "projected"
	child := NamedResource{Group: HelmReleaseDomain, Kind: KindHelmRelease, Namespace: "apps", Name: "child"}
	for _, tc := range []struct {
		name            string
		file            string
		source, subject NamedResource
		records         map[NamedResource]DiscoveryRecord
		excluded        NamedResource
		want            [][]NamedResource
	}{
		{name: "deleted descendant and sorted ties", file: "apps/deep/deleted.yaml", want: [][]NamedResource{{deep, peer}, {parent}}},
		{name: "excluded deeper group falls through", file: "apps/deep/deleted.yaml", excluded: deep, records: map[NamedResource]DiscoveryRecord{peer: {Pinned: true}}, source: BootstrapSourceID, want: [][]NamedResource{{parent}}},
		{name: "unqualified namespaced owners", file: "apps/deep/child.yaml", records: map[NamedResource]DiscoveryRecord{
			deep: {Declaration: &Kustomization{TargetNamespace: "other"}}, peer: {Declaration: &Kustomization{TargetNamespace: "other"}},
		}, want: [][]NamedResource{{deep, peer}, {parent}}},
		{name: "namespace before depth", file: "apps/deep/child.yaml", subject: child, records: map[NamedResource]DiscoveryRecord{
			deep: {Declaration: &Kustomization{TargetNamespace: "other"}}, peer: {Declaration: &Kustomization{TargetNamespace: "other"}},
		}, want: [][]NamedResource{{parent}}},
		{name: "self and peers before depth", file: "apps/deep/ks.yaml", subject: deep, want: [][]NamedResource{{parent}}},
		{name: "authored self and projected peers", file: "apps/deep/ks.yaml", subject: projected, records: map[NamedResource]DiscoveryRecord{
			projected: {Authored: deep}, peer: {Authored: deep},
		}, want: [][]NamedResource{{parent}}},
		{name: "component claim", file: "components/settings.yaml", want: [][]NamedResource{{parent}}},
		{name: "first source coordinate", file: "../source-a/apps/deleted.yaml", want: [][]NamedResource{{parent}}},
		{name: "second source coordinate", file: "../source-b/apps/deleted.yaml", want: [][]NamedResource{{other}}},
		{name: "source boundary excludes checkout root", file: "../source-c/deleted.yaml"},
		{name: "pinned claim excludes working tree", file: "apps/deep/deleted.yaml", source: BootstrapSourceID, records: map[NamedResource]DiscoveryRecord{deep: {Pinned: true}, peer: {Pinned: true}}, want: [][]NamedResource{{parent}}},
		{name: "pinned artifact retains claim", file: "../source-a/apps/deleted.yaml", source: other, records: map[NamedResource]DiscoveryRecord{parent: {Pinned: true}}, want: [][]NamedResource{{parent}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			index := DiscoveryIndex{Records: tc.records, Claims: map[string][]NamedResource{
				"apps/": {parent}, "apps/deep/": {peer, deep}, "components/": {parent},
				"../source-a/apps/": {parent}, "../source-b/apps/": {other}, "": {other},
			}}
			// The checkout-root claim must not cover mounted artifact coordinates.
			if filepath.IsLocal(tc.file) {
				delete(index.Claims, "")
			}
			before := slices.Clone(index.Claims["apps/deep/"])
			got := slices.Collect(
				index.ClaimOwners(
					tc.file,
					tc.source,
					tc.subject,
					func(id NamedResource) bool { return id != tc.excluded },
				),
			)
			assert.Diff(t, got, tc.want)
			assert.Diff(t, index.Claims["apps/deep/"], before)
		})
	}
}

func TestClaimOwners_BorrowedPrefixes(t *testing.T) {
	t.Parallel()
	groups := map[string][]NamedResource{
		"": {{Name: "0"}}, "apps/": {{Name: "1"}, {Name: "2"}}, "apps/nested/": {{Name: "3"}},
		"../pin/": {{Name: "4"}}, "../pin/app/": {{Name: "5"}}, "unused/": nil,
	}
	prefixes := map[string]string{"0": "", "1": "apps/", "3": "apps/nested/", "4": "../pin/", "5": "../pin/app/"}
	index := DiscoveryIndex{Claims: groups}
	for _, tc := range []struct {
		coordinate string
		want       []string
	}{
		{"apps/nested/file.yaml", []string{"apps/nested/", "apps/", ""}},
		{"apps/nested/", []string{"apps/nested/", "apps/", ""}},
		{"apps/nested", []string{"apps/", ""}},
		{"../pin/app/file.yaml", []string{"../pin/app/", "../pin/", ""}},
		{"..", []string{""}},
		{"../", []string{""}},
		{"", []string{""}},
		{"unused/file.yaml", []string{""}},
	} {
		t.Run(tc.coordinate, func(t *testing.T) {
			t.Parallel()
			query := index.ClaimOwners(tc.coordinate, BootstrapSourceID, NamedResource{}, nil)
			for range 2 {
				var got []string
				for group := range query {
					prefix := prefixes[group[0].Name]
					got = append(got, prefix)
					assert.FatalIf(t, &group[0] != &groups[prefix][0], "group %q was copied", prefix)
				}
				assert.FatalIf(t, !slices.Equal(got, tc.want), "prefixes = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClaimOwners_EarlyStop(t *testing.T) {
	t.Parallel()
	groups := map[string][]NamedResource{
		"":             {{Name: "0"}},
		"apps/":        {{Name: "1"}},
		"apps/nested/": {{Name: "2"}, {Name: "3"}},
	}
	index := DiscoveryIndex{Claims: groups}
	calls := 0
	index.ClaimOwners(
		"apps/nested/file.yaml",
		BootstrapSourceID,
		NamedResource{},
		nil,
	)(
		func(group []NamedResource) bool {
			calls++
			assert.FatalIf(t, !slices.Equal(group, groups["apps/nested/"]), "first group = %v", group)
			return false
		},
	)
	assert.FatalIf(t, calls != 1, "early stop called yield %d times, want 1", calls)
}

//nolint:paralleltest // allocation measurements require serial execution.
func TestClaimOwners_NoAllocations(t *testing.T) {
	prefix := strings.Repeat("directory/", 100)
	coordinate := prefix + "file.yaml"
	index := DiscoveryIndex{Claims: map[string][]NamedResource{"": {{Name: "1"}}, prefix: {{Name: "2"}, {Name: "3"}}}}
	for _, stop := range []bool{false, true} {
		allocations := testing.AllocsPerRun(100, func() {
			count := 0
			for group := range index.ClaimOwners(coordinate, BootstrapSourceID, NamedResource{}, nil) {
				count += len(group)
				if stop {
					break
				}
			}
			if count == 0 {
				t.Fatal("no groups yielded")
			}
		})
		assert.FatalIf(t, allocations != 0, "allocations with stop=%v = %g, want 0", stop, allocations)
	}
}
