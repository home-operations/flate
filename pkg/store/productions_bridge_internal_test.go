package store

import (
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_ProductionsScopedFileBridge(t *testing.T) {
	t.Parallel()

	t.Run(
		"empty_direct_needs_accepted_root",
		func(t *testing.T) { t.Parallel(); testStoreProductionsScopedFileBridgeEmptyDirectNeedsAcceptedRoot(t) },
	)

	a, b := ownerID("a"), ownerID("b")
	source := manifest.BootstrapSourceID
	other := source
	other.Name = "other"
	first := manifest.Production{Owner: a, Source: source, Path: "f.yaml", File: "f.yaml"}
	for _, tc := range []struct {
		name               string
		direct, bare, want []manifest.Production
	}{
		{
			name: "unrelated_source_chain", direct: []manifest.Production{{Source: source}},
			bare: []manifest.Production{{Source: other, Path: "e.yaml"}, {Owner: b, Source: other, Path: "p.yaml"}},
			want: []manifest.Production{{Source: source}},
		},
		{
			name: "foreign_source_chain",
			bare: []manifest.Production{first, {Source: other, Path: "e.yaml"}, {Owner: b, Source: other, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
		{
			name: "mixed_source_chain",
			bare: []manifest.Production{first, {Source: other, Path: "e.yaml"}, {Owner: b, Source: source, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
		{
			name: "direct_mixed_source_chain", direct: []manifest.Production{{Source: source}},
			bare: []manifest.Production{{Source: source, Path: "e.yaml"}, {Owner: b, Source: other, Path: "p.yaml"}},
			want: []manifest.Production{{Source: source}, {Source: source, Path: "e.yaml"}},
		},
		{
			name: "later_bridge_chain", direct: []manifest.Production{{Owner: a, Source: source}},
			bare: []manifest.Production{{Owner: b, Source: source, Path: "p.yaml"}, {Source: source, Path: "e.yaml"}},
			want: []manifest.Production{{Source: source, Path: "e.yaml"}, {Owner: a, Source: source}},
		},
		{
			name: "pathless_chain",
			bare: []manifest.Production{first, {Source: source}, {Owner: b, Source: source, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
		{
			name: "authored_namespace_chain",
			bare: []manifest.Production{first, {Source: source, Path: "e.yaml", Authored: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "other", Name: "input"}}, {Owner: b, Source: source, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStoreProductionsScopedFileBridgeCase(
				t,
				&testStoreProductionsScopedFileBridgeCaseInputs{a: &a, b: &b, tc: &tc},
			)
		},
		)
	}

	t.Run(
		"hidden_bare_only_after_withdrawal",
		func(t *testing.T) { t.Parallel(); testStoreProductionsScopedFileBridgeHiddenBareOnlyAfterWithdrawal(t) },
	)

	for _, differentSource := range []bool{false, true} {
		for _, explicitNamespace := range []bool{false, true} {
			for _, differentBinding := range []bool{false, true} {
				t.Run(
					fmt.Sprintf(
						"differentSource=%t/explicitNamespace=%t/differentBinding=%t",
						differentSource,
						explicitNamespace,
						differentBinding,
					),
					func(t *testing.T) {
						t.Parallel()
						testStoreProductionsScopedFileBridgeCase2(
							t,
							&testStoreProductionsScopedFileBridgeCase2Inputs{
								differentSource:   &differentSource,
								explicitNamespace: &explicitNamespace,
								differentBinding:  &differentBinding,
							},
						)
					},
				)
			}
		}
	}
	for _, tc := range []struct {
		name, path  string
		otherSource bool
	}{
		{name: "unowned_direct_source", otherSource: true},
		{name: "direct_path", path: "direct.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStoreProductionsScopedFileBridgeCase3(t, &tc)
		},
		)
	}

	for _, shadowed := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered_empty_direct/shadowed=%t", shadowed), func(t *testing.T) {
			t.Parallel()
			testStoreProductionsScopedFileBridgeCase4(
				t,
				&shadowed,
			)
		},
		)
	}
}

func testStoreProductionsScopedFileBridgeEmptyDirectNeedsAcceptedRoot(t *testing.T) {
	t.Helper()

	s := New()
	a, b := ownerID("a"), ownerID("b")
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	bare := target
	bare.Namespace = ""
	first := manifest.Production{Owner: a, Source: source, Path: "a.yaml", File: "a.yaml"}
	second := manifest.Production{Owner: b, Source: source, Path: "b.yaml", File: "b.yaml"}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {Productions: []manifest.Production{first, second}},
	}}
	s.SetDiscoveryIndex(index)
	s.coverage[a].binding.artifact = &SourceArtifact{Key: "a"}
	s.coverage[b].binding.artifact = &SourceArtifact{Key: "b"}
	assert.Diff(t, s.Productions(target, index), []manifest.Production{first})
}

type testStoreProductionsScopedFileBridgeCaseInputs struct {
	a  *manifest.NamedResource
	b  *manifest.NamedResource
	tc *struct {
		name   string
		direct []manifest.Production
		bare   []manifest.Production
		want   []manifest.Production
	}
}

func testStoreProductionsScopedFileBridgeCase(
	t *testing.T,
	testCaseInput *testStoreProductionsScopedFileBridgeCaseInputs,
) {
	t.Helper()

	s := New()
	target := emissionChild("input", "value").Obj.Named()
	bare := target
	bare.Namespace = ""
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: testCaseInput.tc.direct}, bare: {Productions: testCaseInput.tc.bare},
	}}
	s.SetDiscoveryIndex(index)
	for _, id := range []manifest.NamedResource{(*testCaseInput.a), (*testCaseInput.b)} {
		if c := s.coverage[id]; c != nil {
			c.binding.artifact = &SourceArtifact{Key: id.Name}
		}
	}
	assert.Diff(t, s.Productions(target, nil), testCaseInput.tc.want)
	assert.Diff(t, s.ReferenceFacts(target, nil, false, manifest.NamedResource{}).Productions, testCaseInput.tc.want)
}

func testStoreProductionsScopedFileBridgeHiddenBareOnlyAfterWithdrawal(t *testing.T) {
	t.Helper()

	s := New()
	p, q := ownerID("emitter"), ownerID("discovered")
	for _, id := range []manifest.NamedResource{p, q} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	child := emissionChild("withdrawn-bare-only", "value")
	target := child.Obj.Named()
	bare := target
	bare.Namespace = ""
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {
			Productions: []manifest.Production{{Owner: q, Source: manifest.BootstrapSourceID, Path: "input.yaml"}},
		},
	}}
	s.SetDiscoveryIndex(index)
	s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true}, nil)
	s.RecordEmitted(p, &Emission{Complete: true}, nil)
	sh := s.shardFor(target)
	sh.mu.RLock()
	hidden, members := sh.cells[target].hidden, len(sh.members[target])
	sh.mu.RUnlock()
	if !hidden || members != 0 || len(index.Records[target].Productions) != 0 {
		t.Fatalf(
			"invalid withdrawal fixture: hidden=%t members=%d direct=%v",
			hidden,
			members,
			index.Records[target].Productions,
		)
	}
	assert.Equal(t, len(s.Productions(target, nil)), 0)
	assert.Diff(t, s.ReferenceFacts(target, nil, false, manifest.NamedResource{}), ReferenceFacts{})
	parent, ok := s.RenderedParent(target, nil)
	assert.Equal(t, parent, manifest.NamedResource{})
	assert.Equal(t, ok, false)
}

type testStoreProductionsScopedFileBridgeCase2Inputs struct {
	differentSource   *bool
	explicitNamespace *bool
	differentBinding  *bool
}

func testStoreProductionsScopedFileBridgeCase2(
	t *testing.T,
	testCaseInput *testStoreProductionsScopedFileBridgeCase2Inputs,
) {
	t.Helper()

	s := New()
	renderer, fileOwner := ownerID("renderer"), ownerID("file-owner")
	for _, id := range []manifest.NamedResource{renderer, fileOwner} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	other := source
	if *testCaseInput.differentSource {
		other.Name = "other"
	}
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	authored := target
	authored.Namespace = ""
	if *testCaseInput.explicitNamespace {
		authored.Namespace = "other"
	}
	bare := target
	bare.Namespace = ""
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			target: {Productions: []manifest.Production{{Owner: renderer, Source: source}}},
			bare: {
				Productions: []manifest.Production{
					{
						Owner:    fileOwner,
						Source:   other,
						Authored: authored,
						Path:     "source/input.yaml",
						File:     "input.yaml",
					},
				},
			},
		},
	}
	s.SetDiscoveryIndex(index)
	if *testCaseInput.differentBinding {
		s.coverage[fileOwner].binding.artifact = &SourceArtifact{Key: "other"}
	}
	result := s.Productions(target, index)
	facts := s.ReferenceFacts(target, index, false, manifest.NamedResource{})
	assert.Diff(t, facts.Productions, result)
	parent, ok := s.RenderedParent(target, nil)
	{
		assert.FatalIf(
			t,
			!ok || parent != renderer,
			"rendered parent=%v/%t, want %v/true",
			parent,
			ok,
			renderer,
		)
	}
	want := !(*testCaseInput.differentSource) && !(*testCaseInput.explicitNamespace) &&
		!(*testCaseInput.differentBinding)
	{
		got := slices.ContainsFunc(
			result,
			func(p manifest.Production) bool { return p.Owner == fileOwner && p.Path != "" },
		)
		assert.FatalIf(t, got != want, "scoped bridge=%t want %t: %v", got, want, result)
	}
	proofs := []manifest.Production{{Owner: renderer, Source: source}}
	if want {
		proofs = append(proofs, index.Records[bare].Productions[0])
	}
	slices.SortFunc(proofs, compareProduction)
	assert.Diff(t, result, proofs)
	{
		_, ok := index.Lookup(
			manifest.NamedResource{Kind: target.Kind, Namespace: "unrelated", Name: target.Name},
		)
		assert.FatalIf(t, ok, "%s", "exact index lookup gained wildcard semantics")
	}
}

func testStoreProductionsScopedFileBridgeCase3(
	t *testing.T,
	testCaseInput *struct {
		name        string
		path        string
		otherSource bool
	},
) {
	t.Helper()

	s := New()
	target := emissionChild("input", "value").Obj.Named()
	bare := target
	bare.Namespace = ""
	source := manifest.BootstrapSourceID
	other := source
	if testCaseInput.otherSource {
		other.Name = "other"
	}
	proof := manifest.Production{Source: source, Path: testCaseInput.path, File: testCaseInput.path}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{proof}},
		bare: {
			Productions: []manifest.Production{
				{Owner: ownerID("parent"), Source: other, Path: "bare.yaml", File: "bare.yaml"},
			},
		},
	}}
	s.SetDiscoveryIndex(index)
	assert.Diff(t, s.Productions(target, nil), []manifest.Production{proof})
	{
		parent, ok := s.RenderedParent(target, nil)
		assert.FatalIf(
			t,
			ok || parent != (manifest.NamedResource{}),
			"suppressed bridge parent=%v/%t",
			parent,
			ok,
		)
	}
}

func testStoreProductionsScopedFileBridgeCase4(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	p, q, r := ownerID("p"), ownerID("q"), ownerID("r")
	for _, owner := range []manifest.NamedResource{p, q, r} {
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	}
	target := emissionChild("bridged", "value").Obj.Named()
	bare := target
	bare.Namespace = ""
	source := manifest.BootstrapSourceID
	other := source
	other.Name = "other"
	proofs := []manifest.Production{
		{Owner: r, Source: other},
		{Owner: p, Source: source, Authored: bare, Path: "first.yaml"},
		{Owner: r, Source: other, Path: "other.yaml"},
		{Owner: q, Source: source, Path: "matching.yaml"},
		{Source: source, Path: "unbound.yaml"},
		{Owner: r, Source: source, Path: "later.yaml"},
	}
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{bare: {Productions: proofs}},
		},
	)
	want := []manifest.Production{proofs[1]}
	if *testCaseInput {
		_, _, next := s.GetEmission(p)
		next.Complete = true
		s.RecordEmitted(p, &next, nil)
		next.Finish()
		want = nil
	}
	slices.SortFunc(want, compareProduction)
	assert.Diff(t, s.Productions(target, nil), want)
	parent, ok := s.RenderedParent(target, nil)
	{
		assert.FatalIf(
			t,
			ok || parent != (manifest.NamedResource{}),
			"authored bridge parent=%v/%t",
			parent,
			ok,
		)
	}
}
