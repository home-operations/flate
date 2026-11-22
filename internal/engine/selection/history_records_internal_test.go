package selection

import (
	"iter"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRefineHistory_OrderingClosure(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := manifest.BootstrapSourceID
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := testutil.KustomizationID("apps", "target")
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}},
	}
	for _, object := range []manifest.BaseManifest{producer, consumer} {
		st.AddObject(object)
	}
	st.SetArtifact(
		source,
		&store.SourceArtifact{LocalPath: "/repo", FS: fstest.MapFS{"app/cm.yaml": {Data: []byte("unchanged")}}},
	)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		target:           {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	f := newHistoryEvaluation(
		t,
		historyEvaluationConfig{
			changes:   detectChanges(t, []string{"consumer.yaml"}, []string{"consumer.yaml"}),
			index:     index,
			inventory: st,
			root:      "/repo",
		},
	)
	historyOwner := testutil.KustomizationID("apps", "history-owner")
	f.refine(
		map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{
			historyOwner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				yield(
					consumer.Named(),
					manifest.DiscoveryRecord{
						Refs:         []manifest.NamedResource{source},
						ContentRoots: map[manifest.NamedResource][]string{source: {"app"}},
					},
				)
			},
		},
		nil,
	)
	assert.Equal(t, f.closure.contentRecord(consumer.Named()).ContentRoots != nil, true)
	assert.Diff(t, f.proposal.Readiness, []manifest.NamedResource{target})
	assert.Diff(t, f.closure.record(consumer.Named()).Refs, []manifest.NamedResource{source})
	assert.Diff(
		t,
		f.roles(),
		map[manifest.NamedResource]uint8{
			consumer.Named(): needed | selected,
			source:           needed,
		},
	)
}

func TestRefineHistory_OrderingReferences(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"consumer.yaml", "target.yaml"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			source := manifest.BootstrapSourceID
			target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
			consumer := &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}},
			}
			for _, object := range []manifest.BaseManifest{target, consumer} {
				st.AddObject(object)
			}
			st.SetArtifact(
				source,
				&store.SourceArtifact{LocalPath: "/repo", FS: fstest.MapFS{"app/cm.yaml": {Data: []byte("unchanged")}}},
			)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {File: "consumer.yaml"},
				target.Named():   {File: "target.yaml"},
			}}
			f := newHistoryEvaluation(
				t,
				historyEvaluationConfig{
					changes:   detectChanges(t, []string{changed}, []string{changed}),
					index:     index,
					inventory: st,
					root:      "/repo",
				},
			)
			historyOwner := testutil.KustomizationID("apps", "history-owner")
			f.refine(
				map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{
					historyOwner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
						yield(
							consumer.Named(),
							manifest.DiscoveryRecord{
								Refs:         []manifest.NamedResource{source, target.Named()},
								ContentRoots: map[manifest.NamedResource][]string{source: {"app"}},
							},
						)
					},
				},
				nil,
			)
			assert.Equal(t, f.closure.contentRecord(consumer.Named()).ContentRoots != nil, true)
			assert.Diff(t, f.closure.record(consumer.Named()).Refs, []manifest.NamedResource{source, target.Named()})
			want := make(map[manifest.NamedResource]uint8)
			if changed == "consumer.yaml" {
				want[consumer.Named()] = needed | selected
				want[source] = needed
				assert.Diff(t, f.proposal.Readiness, []manifest.NamedResource{target.Named()})
			} else {
				want[target.Named()] = needed | selected
				assert.Diff(t, f.proposal.Fatal, []manifest.NamedResource{consumer.Named()})
			}
			assert.Diff(t, f.roles(), want)
		})
	}
}

func TestRefineHistory_RenamedOrderingTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		side     manifest.TreeSide
		complete bool
	}{
		{name: "head confirmed", side: manifest.TreeHead, complete: true},
		{name: "base confirmed", side: manifest.TreeBase, complete: true},
		{name: "head incomplete", side: manifest.TreeHead},
		{name: "base incomplete", side: manifest.TreeBase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := manifest.BootstrapSourceID
			targets := [2]manifest.NamedResource{
				testutil.KustomizationID("apps", "before"),
				testutil.KustomizationID("apps", "after"),
			}
			missing := targets[1-tc.side]
			intermediate := &manifest.Kustomization{
				Name: "intermediate", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: missing.Name}},
			}
			root := &manifest.Kustomization{
				Name: "root", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: intermediate.Name}},
			}
			stores := [2]*store.Store{store.New(), store.New()}
			indexes := [2]*manifest.DiscoveryIndex{}
			artifacts := [2]*store.SourceArtifact{}
			const file = "target.yaml"
			for side, st := range stores {
				target := targets[side]
				artifacts[side] = &store.SourceArtifact{
					LocalPath: t.TempDir(), FS: fstest.MapFS{file: {Data: []byte(target.Name)}},
				}
				st.SetArtifact(source, artifacts[side])
				st.AddObject(root)
				st.AddObject(intermediate)
				// The file census can know a target before it has an indexed declaration.
				indexes[side] = &manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						root.Named():         {File: "root.yaml"},
						intermediate.Named(): {File: "intermediate.yaml"},
					},
					CompleteSources: map[manifest.NamedResource]bool{
						source: manifest.TreeSide(side) != tc.side || tc.complete,
					},
					Declarations: map[manifest.NamedResource]map[string]manifest.FileDeclarations{
						source: {file: {Present: true, Complete: true, Objects: map[manifest.NamedResource]string{
							target: manifest.Fingerprint(target.Name),
						}}},
					},
				}
			}

			changes, err := change.Detect(t.Context(), artifacts[0], artifacts[1], change.DetectConfig{})
			assert.NoError(t, err)
			changes, err = change.Qualify(
				t.Context(), changes, change.SourcePair{Stores: stores, Indexes: indexes}, manifest.WorkScope{},
			)
			assert.NoError(t, err)
			changes, err = changes.WithDeclarations(t.Context(), change.DeclarationInputs{Stores: stores})
			assert.NoError(t, err)
			changes, err = changes.WithReaders(
				t.Context(),
				indexes,
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			assert.Equal(t, changes.DeclarationChanged(tc.side, source, missing, nil), change.Comparison{
				Known: tc.complete, Changed: tc.complete,
			})

			st := stores[tc.side]
			facts := &solverFacts{
				store: st, changes: changes, index: *indexes[tc.side],
				context: SourceContext{
					Root: artifacts[tc.side].LocalPath, SideKnown: true, Side: tc.side,
					InputReaders: map[manifest.NamedResource][]manifest.NamedResource{
						missing: {intermediate.Named()}, intermediate.Named(): {root.Named()},
					},
				},
				sources: map[manifest.NamedResource]SourceFacts{source: {Paired: true}},
			}
			session, err := New(st, facts, Request{Changed: true, Roots: []manifest.NamedResource{root.Named()}})
			assert.NoError(t, err)
			proposal, err := prepareSolver(t.Context(), t, session, PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, len(proposal.Selected), 0)
			assert.Equal(t, len(proposal.Ordinary), 0)
			if tc.complete {
				assert.Diff(t, proposal.Fatal, []manifest.NamedResource{root.Named()})
				assert.Diff(
					t,
					proposal.Readiness,
					[]manifest.NamedResource{missing, intermediate.Named(), root.Named()},
				)
				assert.Diff(t, proposal.ordering[root.Named()], []store.OrderingEdge{{Target: intermediate.Named()}})
				assert.Diff(t, proposal.ordering[intermediate.Named()], []store.OrderingEdge{{Target: missing}})
			} else {
				assert.Equal(t, len(proposal.Fatal), 0)
				assert.Equal(t, len(proposal.Readiness), 0)
			}
		})
	}
}

func TestRefineHistory_RestoredSourceQualification(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := testutil.KustomizationID("apps", "owner")
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	source := &manifest.GitRepository{Name: "pinned", Namespace: "apps"}
	for _, object := range []manifest.BaseManifest{source, child} {
		st.AddObject(object)
	}
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{
			LocalPath: "/pinned",
			LocalRoot: "/repo",
			Key:       "commit",
			Content:   "commit",
			FS:        fstest.MapFS{"child.yaml": {Data: []byte("unchanged")}},
		},
	)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner: {File: "owner.yaml"},
		child.Named(): {
			File:        "child.yaml",
			Builder:     "builder.yaml",
			Declaration: child,
			Productions: []manifest.Production{{Owner: owner, Source: source.Named(), File: "child.yaml"}},
		},
	}}
	f := newHistoryEvaluation(
		t,
		historyEvaluationConfig{
			changes:   detectChanges(t, []string{"owner.yaml", "child.yaml"}, []string{"owner.yaml", "child.yaml"}),
			index:     index,
			inventory: st,
			root:      "/repo",
			sources:   map[manifest.NamedResource]SourceFacts{source.Named(): {Pinned: true}},
		},
	)
	f.refine(
		map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{
			owner: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {},
		},
		map[manifest.NamedResource]bool{owner: true},
	)
	assert.Equal(t, f.closure.replace[owner], false)
	assert.Equal(t, f.reconcile(owner), true)
	assert.Equal(t, f.closure.states[child.Named()].changed, false)
	assert.Equal(t, f.reconcile(child.Named()), false)
}

type historyEvaluationConfig struct {
	changes   *change.Set
	index     *manifest.DiscoveryIndex
	inventory *store.Store
	root      string
	sources   map[manifest.NamedResource]SourceFacts
}

type historyEvaluation struct {
	tb       *testing.T
	closure  admissionClosure
	proposal *Admission
}

func newHistoryEvaluation(t *testing.T, cfg historyEvaluationConfig) *historyEvaluation {
	t.Helper()
	facts := &solverFacts{
		changes: cfg.changes,
		index:   *cfg.index,
		context: SourceContext{Root: cfg.root},
		sources: map[manifest.NamedResource]SourceFacts{},
	}
	ids := []manifest.NamedResource{manifest.BootstrapSourceID}
	for _, object := range cfg.inventory.Objects() {
		if manifest.KindOf(object).IsSource() {
			ids = append(ids, object.Named())
		}
	}
	for _, id := range ids {
		input, _ := cfg.inventory.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		location, supplied := cfg.sources[id]
		if !supplied {
			location.Changes, location.Delta, location.Files = cfg.changes, cfg.changes, sourceFilesForTest(
				cfg.changes.Paths(),
			)
		}
		location.Context, location.Source, location.Input, location.Initial, location.InitialKnown, location.Origin = facts.context, id, input, input, true, cfg.root
		location.Declaration = cfg.inventory.GetDeclaration(id)
		facts.sources[id] = location
	}
	f := &historyEvaluation{tb: t, closure: solverClosure(t, cfg.inventory, facts)}
	proposal, err := evaluateSolver(t, &f.closure, &PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	f.proposal = proposal
	return f
}

func (f *historyEvaluation) refine(
	records map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
	complete map[manifest.NamedResource]bool,
) {
	f.tb.Helper()
	request := PrepareRequest{Invocation: true, History: map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}{}}
	for owner, records := range records {
		request.History[owner] = struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{records, complete[owner]}
	}
	proposal, err := evaluateSolver(f.tb, &f.closure, &request)
	assert.NoError(f.tb, err)
	f.proposal = proposal
}

func (f *historyEvaluation) reconcile(id manifest.NamedResource) bool {
	return slices.Contains(f.proposal.Needed, id)
}

func (f *historyEvaluation) roles() map[manifest.NamedResource]uint8 {
	result := map[manifest.NamedResource]uint8{}
	for _, id := range f.proposal.Needed {
		result[id] = needed
	}
	for _, id := range f.proposal.Selected {
		result[id] |= selected
	}
	return result
}

func TestHistory_SourceRootReplacement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		available bool
	}{
		{"mutable", true},
		{"alias", true},
		{"pinned", true},
		{"paired_pinned", true},
		{"external", true},
		{"unavailable", false},
		{"component", true},
		{"symlink", true},
		{"unresolved", false},
		{"remote", false},
		{"escaped", false},
		{"source_root", true},
		{"symlink_escape", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			generator := &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator",
				Name: "generator", Namespace: "apps",
				Spec: map[string]any{"sources": []any{
					map[string]any{"kind": manifest.KindGitRepository, "name": "content"},
					map[string]any{"kind": manifest.KindGitRepository, "name": "unused"},
				}},
			}
			st.AddObject(generator)
			source := (&manifest.GitRepository{Name: "content", Namespace: "apps"}).Named()
			owner := testutil.KustomizationID("apps", "emitter")
			child := testutil.KustomizationID("apps", "child")
			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "old"}
			input := &store.SourceArtifact{LocalPath: "/content", FS: fstest.MapFS{}}
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						target: {Productions: []manifest.Production{{Owner: owner}}},
					},
				},
				sources: map[manifest.NamedResource]SourceFacts{source: {Source: source, Input: input}},
			}
			c := solverClosure(t, st, facts)
			assert.Diff(t, slices.Collect(c.sources(generator.Named())), []manifest.NamedResource{source})
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			c.proposal = &Admission{reads: &reads, preparation: &Preparation{}}
			demand := Demand{Source: source, Root: "apps/red", History: true, Artifact: input, Reads: &reads}
			c.proposal.preparation.acceptAcquired(
				[]Acquisition{{Demand: demand, Root: "apps/red", Available: tc.available, Reads: &reads}},
			)
			c.history = map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{owner: {Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				yield(
					child,
					manifest.DiscoveryRecord{ContentRoots: map[manifest.NamedResource][]string{source: {"apps/red"}}},
				)
			}, Complete: true}}
			c.readHistory()
			remaining := c.productions(target)
			assert.Equal(
				t,
				slices.ContainsFunc(remaining, func(p manifest.Production) bool { return p.Owner == owner }),
				!tc.available,
			)
			assert.Diff(t, c.contentRecord(child).ContentRoots[source], []string{"apps/red"})
		})
	}
}

func TestHistory_StaticProductionSurvivesReplacement(t *testing.T) {
	t.Parallel()
	owner := testutil.KustomizationID("apps", "owner")
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	proofs := []manifest.Production{{Owner: owner}, {Source: manifest.BootstrapSourceID, File: "static.yaml"}}
	facts := &solverFacts{
		index: manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target: {Productions: proofs}},
		},
	}
	c := solverClosure(t, store.New(), facts)
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {}}
	c.replace = map[manifest.NamedResource]bool{owner: true}
	assert.Equal(t, len(c.productions(target)), 1)
	assert.Equal(t, len(facts.index.Records[target].Productions), len(proofs))
}

func TestHistory_PeerContentRoots(t *testing.T) {
	t.Parallel()
	owner := testutil.KustomizationID("apps", "emitter")
	changed := (&manifest.GitRepository{Name: "changed", Namespace: "apps"}).Named()
	unchanged := (&manifest.GitRepository{Name: "unchanged", Namespace: "apps"}).Named()
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	for _, source := range []manifest.NamedResource{changed, unchanged} {
		id := testutil.KustomizationID("apps", "projected-"+source.Name)
		records[id] = manifest.DiscoveryRecord{
			Refs:         []manifest.NamedResource{source},
			ContentRoots: map[manifest.NamedResource][]string{source: {"apps/red"}},
			Productions:  []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID}},
		}
	}
	c := solverClosure(t, store.New(), &solverFacts{})
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{owner: records}
	c.replace = map[manifest.NamedResource]bool{owner: true}
	for _, source := range []manifest.NamedResource{changed, unchanged} {
		id := testutil.KustomizationID("apps", "projected-"+source.Name)
		assert.Diff(t, c.contentRecord(id).ContentRoots[source], []string{"apps/red"})
	}
}
