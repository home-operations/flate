package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
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

func TestRecordEmitted_AbsentProducerStaysPrivate(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("absent")
	child := emissionChild("private", "value")
	s.AddObject(child.Obj)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {Productions: []manifest.Production{{Owner: p}}},
	}})
	s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "static"})
	s.UpdateStatus(child.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	{
		assert.FatalIf(
			t,
			s.GetArtifact(child.Obj.Named(), ArtifactContent) == nil,
			"%s",
			"fixture has no retained static artifact",
		)
	}
	accept(
		t,
		s,
		p,
		&Emission{
			Children:  []Child{child},
			Manifests: testutil.DocumentOccurrences([]map[string]any{child.Doc}),
			Complete:  true,
		},
	)
	s.UpdateFailure(p, manifest.NewError(manifest.ErrInput, nil, "private failure"))
	s.SetArtifact(p, &SourceArtifact{Key: "private"})
	assert.Equal(t, s.EmissionOwner(ownerID("missing")), manifest.NamedResource{})
	for _, id := range []manifest.NamedResource{p, child.Obj.Named()} {
		assert.Equal(t, s.EmissionOwner(id), manifest.NamedResource{})
		{
			assert.FatalIf(
				t,
				s.GetObject(id) != nil || s.GetArtifact(id, ArtifactContent) != nil,
				"%s",
				"absent producer leaked live object or artifact",
			)
		}
		{
			_, ok := s.GetStatus(id)
			assert.FatalIf(t, ok, "%s", "absent producer leaked terminal state")
		}
		var observation Observation
		obj, observation := s.Snapshot(id, true)
		info := observation.StatusInfo
		conditions := observation.Conditions
		{
			assert.FatalIf(
				t,
				obj != nil || info != (StatusInfo{}) || len(conditions) != 0,
				"%s",
				"absent producer leaked snapshot",
			)
		}
		{
			obj, _, attempt := s.GetEmission(
				id,
			)
			assert.FatalIf(t, obj != nil || len(attempt.Children) != 0 ||
				len(attempt.Manifests) != 0, "%s", "absent producer leaked accepted output")
		}
	}
	{
		assert.FatalIf(
			t,
			len(s.FailedResources()) != 0 || len(s.ListObjects("", "")) != 0,
			"%s",
			"absent producer leaked inventory",
		)
	}
	for event := EventObjectAdded; event <= EventArtifactUpdated; event++ {
		s.AddListener(
			event,
			func(manifest.NamedResource, any) { t.Error("absent producer leaked listener replay") },
			true,
		)()
	}
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	{
		assert.FatalIf(
			t,
			s.GetObject(child.Obj.Named()) == nil,
			"%s",
			"external anchor failed to activate privately completed output",
		)
	}
	{
		assert.FatalIf(
			t,
			s.GetArtifact(child.Obj.Named(), ArtifactContent) != nil,
			"%s",
			"external anchor activated a retained static artifact",
		)
	}
	{
		_, ok := s.GetStatus(child.Obj.Named())
		assert.FatalIf(t, ok, "%s", "external anchor activated retained static Ready")
	}
	{
		assert.FatalIf(
			t,
			!s.shardFor(child.Obj.Named()).cells[child.Obj.Named()].retired,
			"%s",
			"actual membership restored shadowed static authority",
		)
	}
	{
		_, ok := s.GetStatus(p)
		assert.FatalIf(t, ok, "%s", "external anchor activated private Ready/Failed state")
	}
}

func TestStore_AddListener_ReplayWithdrawnDeclaredSecret(t *testing.T) {
	t.Parallel()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	for _, owner := range []manifest.NamedResource{p, q} {
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	}
	secret := &manifest.Secret{Name: "shared", Namespace: "apps"}
	external := &manifest.RawObject{
		Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "external", Namespace: "apps",
		Spec: map[string]any{"target": map[string]any{"name": secret.Name}},
	}
	accept(t, s, p, &Emission{Children: []Child{{Obj: secret}}, Complete: true})
	accept(t, s, q, &Emission{Children: []Child{{Obj: external}}, Complete: true})
	accept(t, s, p, &Emission{Complete: true})
	{
		assert.FatalIf(
			t,
			s.GetObject(secret.Named()) != nil || s.GetObject(external.Named()) != external,
			"%s",
			"withdrawal did not preserve only the effective ExternalSecret",
		)
	}
	assert.Equal(t, s.EmissionOwner(secret.Named()), manifest.NamedResource{})
	assert.Equal(t, s.EmissionOwner(external.Named()), q)
	proofs := s.Productions(secret.Named(), nil)
	{
		assert.FatalIf(
			t,
			len(proofs) != 1 || proofs[0].Owner != q || proofs[0].Declaration != external.Named(),
			"withdrawal lost declaration evidence: %v",
			proofs,
		)
	}
	inventory := s.ListObjects("", "")
	{
		assert.FatalIf(
			t,
			len(inventory) != 3,
			"effective inventory has %d objects, want two owners and ExternalSecret",
			len(inventory),
		)
	}
	replayed := make(map[manifest.NamedResource][]any)
	var notifications int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
		replayed[id] = append(replayed[id], payload)
		notifications++
	}, true)
	{
		assert.FatalIf(
			t,
			len(replayed) != len(inventory) || notifications != len(inventory),
			"replayed %d identities and %d notifications, want %d",
			len(replayed),
			notifications,
			len(inventory),
		)
	}
	for _, obj := range inventory {
		if payloads := replayed[obj.Named()]; len(payloads) != 1 || payloads[0] != obj {
			t.Fatalf("replay for %v: got %v, want effective object %v once", obj.Named(), payloads, obj)
		}
	}
	{
		assert.FatalIf(t, len(replayed[secret.Named()]) != 0, "%s", "replay revived the withdrawn Secret")
	}
	{
		got := s.ListObjects("", "")
		assert.FatalIf(t, !slices.Equal(got, inventory), "%s", "registration changed the effective inventory")
	}
	live := emissionChild("live", "value")
	s.AddObject(live.Obj)
	{
		assert.FatalIf(
			t,
			s.GetObject(live.Obj.Named()) != live.Obj,
			"%s",
			"store did not retain an object added after replay",
		)
	}
	{
		payloads := replayed[live.Obj.Named()]
		assert.FatalIf(t, len(payloads) != 1 || payloads[0] != live.Obj ||
			notifications != len(
				inventory,
			)+1, "live arrival: got %v and %d notifications, want one new notification", payloads, notifications)
	}
	unsub()
}

func TestStore_ReferenceFacts_ReplacementConsistency(t *testing.T) {
	t.Parallel()

	for _, namespace := range []string{"apps", ""} {
		t.Run("authored_namespace="+namespace, func(t *testing.T) {
			t.Parallel()

			s := New()
			owner := ownerID("owner")
			s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			secret := &manifest.Secret{Name: "input", Namespace: "apps", StringData: map[string]any{"VALUE": "visible"}}
			target := secret.Named()
			authored := target
			authored.Namespace = namespace
			proof := manifest.Production{
				Owner:    owner,
				Source:   manifest.BootstrapSourceID,
				Authored: authored,
				Path:     "input.yaml",
				File:     "input.yaml",
			}
			records := map[manifest.NamedResource]manifest.DiscoveryRecord{
				authored: {Productions: []manifest.Production{proof}},
			}
			proofs := []manifest.Production{proof}
			if namespace == "" {
				scoped := manifest.Production{Owner: owner, Source: manifest.BootstrapSourceID}
				records[target] = manifest.DiscoveryRecord{Productions: []manifest.Production{scoped}}
				proofs = append([]manifest.Production{scoped}, proof)
			}
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
			s.AddObject(secret)
			before := s.ReferenceFacts(target, nil)
			{
				assert.FatalIf(t, before.Data != secret || before.Accepted, "file-backed facts=%+v", before)
			}
			assert.Diff(t, before.Productions, proofs)

			external := &manifest.RawObject{
				Kind:       "ExternalSecret",
				APIVersion: "external-secrets.io/v1",
				Name:       "external",
				Namespace:  target.Namespace,
				Spec: map[string]any{
					"target": map[string]any{"name": target.Name},
					"data":   []any{map[string]any{"secretKey": "VALUE"}},
				},
			}
			_, _, next := s.GetEmission(owner)
			defer next.Finish()
			next.Children, next.Complete = []Child{{Obj: external}}, true
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				s.ReferenceFacts(target, nil)
			})
			wg.Go(func() {
				<-start
				s.RecordEmitted(owner, &next, nil)
			})
			close(start)
			wg.Wait()

			want := []manifest.Production{
				{Owner: owner, Source: manifest.BootstrapSourceID, Declaration: external.Named()},
			}
			assert.Diff(
				t,
				s.ReferenceFacts(target, nil),
				ReferenceFacts{
					Productions:   want,
					ReferenceData: []manifest.ReferenceData{{Production: want[0], DeclaredKeys: []string{"VALUE"}}},
				},
			)
			assert.Diff(t, s.Productions(target, nil), want)
			{
				assert.FatalIf(
					t,
					s.GetObject(target) != nil || s.GetObject(external.Named()) != external,
					"%s",
					"replacement did not leave only the declaration",
				)
			}
			parent, ok := s.RenderedParent(target, nil)
			assert.Equal(t, parent, manifest.NamedResource{})
			assert.Equal(t, ok, false)
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestStore_ProductionsQueryAllocations(t *testing.T) {
	s := New()
	id := ownerID("known")
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		id: {Productions: []manifest.Production{{Owner: ownerID("parent")}}},
	}})
	for _, target := range []manifest.NamedResource{ownerID("missing"), id} {
		t.Run(target.Name, func(t *testing.T) {
			{
				got := testing.AllocsPerRun(10, func() {
					s.Productions(target, nil)
				})
				assert.FatalIf(t, got != 0, "production query allocations=%v, want 0", got)
			}
			{
				got := testing.AllocsPerRun(10, func() {
					s.ReferenceFacts(target, nil)
				})
				assert.FatalIf(t, got != 0, "reference facts allocations=%v, want 0", got)
			}
		})
	}
	obj := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
	s.AddObject(obj)
	var reads ReadSet
	measure := func(productions bool) float64 {
		return testing.AllocsPerRun(10, func() {
			reads.Init(s, obj.Named(), nil)
			reads.GetObject(obj.Named())
			if productions {
				reads.Productions(obj.Named(), nil)
			}
			reads.Retire()
			for range s.Sweep() {
			}
		})
	}
	baseline := measure(false)
	{
		got := measure(true)
		assert.FatalIf(
			t,
			got != baseline,
			"empty captured production query allocations=%v, capture alone=%v",
			got,
			baseline,
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestStore_RenderedParentAllocations(t *testing.T) {

	for _, name := range []string{"thin", "frozen", "bridge", "missing"} {
		t.Run(name, func(t *testing.T) {

			testStoreRenderedParentAllocationsCase(t, &name)
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
	assert.Diff(t, s.ReferenceFacts(target, nil).Productions, testCaseInput.tc.want)
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
	assert.Diff(t, s.ReferenceFacts(target, nil), ReferenceFacts{})
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
	facts := s.ReferenceFacts(target, index)
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
