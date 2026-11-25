package selection

import (
	"context"
	"path"
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_NarrowerSuccessorKeepsCompleteEvidence(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
	)
	first := &manifest.Kustomization{Name: "first", Namespace: "apps", Path: "first"}
	second := &manifest.Kustomization{
		Name:      "second",
		Namespace: "apps",
		Path:      "second",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "values"}},
		},
	}
	input := &manifest.ConfigMap{Name: "values", Namespace: "apps", Data: map[string]any{"value": "before"}}
	for _, object := range []manifest.BaseManifest{first, second, input} {
		st.AddObject(object)
	}
	content := testutil.ObjectDocument(second)
	st.AddDeclaration(store.Declaration{Obj: second, Doc: content})
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{first.Named(), second.Named()}})
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	proposal, err := session.prepareTest(t.Context(), PrepareRequest{Invocation: true, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, true)
	_, capturedDoc, captured := proposal.reads.GetEmission(second.Named())
	assert.Diff(t, capturedDoc, content)
	captured.Finish()
	proposal.Retire()
	for range st.Sweep() {
	}
	var guard store.ReadSet
	guard.Init(st, first.Named(), nil)
	guard.RequireData(first.Named())
	defer guard.Close()
	proposal, err = session.prepareTest(t.Context(), PrepareRequest{Reads: &guard, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, true)
	assert.Equal(t, slices.Contains(slices.Collect(proposal.reads.Inputs()), input.Named()), true)
	_, retainedDoc, retained := proposal.reads.GetEmission(second.Named())
	assert.Diff(t, retainedDoc, content)
	retained.Finish()
	proposal.Retire()
	for range st.Sweep() {
	}
	st.AddObject(
		&manifest.ConfigMap{
			Name:      input.Named().Name,
			Namespace: input.Named().Namespace,
			Data:      map[string]any{"value": "after"},
		},
	)
	proposal, err = session.prepareTest(t.Context(), PrepareRequest{Reads: &guard, Preparation: &preparation})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, preparation.closure.states[first.Named()].initialKnown, true)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, false)
	assert.Equal(t, proposal.reads.Valid(), true)
}

func TestAdmission_UnknownDeclarationSkipsPreparedEvidence(t *testing.T) {
	t.Parallel()
	object := &manifest.ConfigMap{Name: "input", Namespace: "apps"}
	st := store.New()
	st.AddDeclaration(store.Declaration{Obj: object, Doc: testutil.ObjectDocument(object)})
	calls := 0
	closure := admissionClosure{
		objects: st,
		facts:   new(solverFacts),
		context: SourceContext{SideKnown: true, Side: manifest.TreeHead},
		states: map[manifest.NamedResource]admissionState{
			object.Named(): {productionsKnown: true},
		},
		proposal: &Admission{Request: PrepareRequest{Changes: detectChanges(t, nil, nil)}},
		declarationContents: func(map[string]any) store.DeclarationContent {
			calls++
			return store.DeclarationContent{Exact: "prepared"}
		},
	}
	assert.Equal(t, closure.declarationChanged(object.Named(), object), change.Comparison{})
	assert.Equal(t, calls, 0)
}

func TestAdmission_AuthoredDeclarationChangeOverridesOutput(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	indexes := [2]*manifest.DiscoveryIndex{}
	object := &manifest.ConfigMap{Name: "input", Namespace: "apps"}
	id := object.Named()
	for side, st := range stores {
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: t.TempDir(), FS: fstest.MapFS{}},
		)
		document := testutil.ObjectDocument(object)
		document["untracked"] = side
		fingerprint := manifest.Fingerprint(document)
		delete(document, "untracked")
		st.AddDeclaration(store.Declaration{Obj: object, Doc: document})
		indexes[side] = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Authored: id, File: "input.yaml", Declaration: object},
			},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
			Declarations: map[manifest.NamedResource]map[string]manifest.FileDeclarations{
				manifest.BootstrapSourceID: {
					"input.yaml": {Present: true, Complete: true, Objects: map[manifest.NamedResource]string{
						id: fingerprint,
					}},
				},
			},
		}
	}
	changes, err := change.Qualify(
		t.Context(),
		detectChanges(t, []string{"input.yaml"}, []string{"input.yaml"}),
		change.SourcePair{Stores: stores},
		manifest.WorkScope{},
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
	facts := &solverFacts{
		store: stores[1], changes: changes, index: *indexes[1],
		context: SourceContext{SideKnown: true, Side: manifest.TreeHead},
		sources: map[manifest.NamedResource]SourceFacts{manifest.BootstrapSourceID: {Paired: true}},
	}
	closure := solverClosure(t, stores[1], facts)
	closure.context = facts.context
	closure.proposal.Request.Changes = changes
	closure.states[id] = admissionState{productionsKnown: true}
	assert.Equal(t, closure.declarationChanged(id, object), change.Comparison{Known: true, Changed: true})
	assert.Equal(
		t,
		closure.outputChanged(id, new(manifest.Reference{NamedResource: id}), object),
		change.Comparison{Known: true},
	)
	assert.Equal(t, closure.declarationUnchanged(id, manifest.NamedResource{}, object), false)
}

func TestAdmission_ChangedReadSettlesWithPendingSource(t *testing.T) {
	t.Parallel()
	const file = "chart/values.yaml"
	for _, tc := range []struct {
		name    string
		reverse bool
		changed bool
	}{
		{name: "pending first", changed: true},
		{name: "changed first", reverse: true, changed: true},
		{name: "pending without a change"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			var stores [2]*store.Store
			for i := range stores {
				stores[i] = store.New()
				stores[i].SetArtifact(
					manifest.BootstrapSourceID,
					&store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}},
				)
			}
			files := []string{"unread.yaml"}
			if tc.changed {
				files = []string{file}
			}
			pair, err := change.Qualify(
				t.Context(), detectChanges(t, files, files), change.SourcePair{Stores: stores}, manifest.WorkScope{},
			)
			assert.NoError(t, err)
			index := &manifest.DiscoveryIndex{}
			pair, err = pair.WithReaders(
				t.Context(), [2]*manifest.DiscoveryIndex{index, index},
				[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
			)
			assert.NoError(t, err)
			consumer := &manifest.HelmRelease{
				Name: "consumer", Namespace: "apps",
				Chart: manifest.HelmChart{Source: manifest.BootstrapSourceID},
			}
			id := consumer.Named()
			pending := (&manifest.ExternalArtifact{Name: "pending", Namespace: "apps"}).Named()
			generator := manifest.NamedResource{
				Group:     manifest.SourceExtensionsDomain,
				Kind:      manifest.KindArtifactGenerator,
				Name:      "generator",
				Namespace: "apps",
			}
			refs := []manifest.Reference{
				{NamedResource: pending, Content: true},
				{
					NamedResource: manifest.BootstrapSourceID,
					Use:           "artifact",
					ArtifactOnly:  true,
					ChartPath:     "chart",
					Files:         []string{file},
				},
			}
			if tc.reverse {
				slices.Reverse(refs)
			}
			facts := &solverFacts{
				store: stores[1], changes: pair,
				context: SourceContext{Root: root, SideKnown: true, Side: manifest.TreeHead},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{
					id: {Identity: id, Object: consumer, CatalogKnown: true, Refs: refs},
				},
			}
			calls := 0
			chartFacts := &chartAdmissionFacts{
				solverFacts: facts,
				read: func(_ context.Context, _ *store.SourceArtifact, chart string) SourceReadPaths {
					calls++
					assert.Equal(t, chart, "chart")
					return SourceReadPaths{Files: []string{file}}
				},
			}
			closure := solverClosure(t, stores[1], facts)
			closure.facts = chartFacts
			closure.context, closure.evaluationContext = facts.context, t.Context()
			closure.proposal.Request.Changes = pair
			closure.states[pending] = admissionState{
				productionsKnown: true,
				productions:      []manifest.Production{{Owner: generator}},
			}
			assert.Equal(t, closure.affected(id, consumer), tc.changed)
			assert.Equal(t, closure.states[id].affectedKnown, tc.changed)
			for i := range refs {
				closure.referenceReadPaths(&refs[i])
			}
			assert.Equal(t, calls, 1)
		})
	}
}

func TestAdmission_DemandsPendingMaterializer(t *testing.T) {
	t.Parallel()
	cases := []materializerCase{
		{name: "base", side: manifest.TreeBase, local: true},
		{
			name: "base-only output change", side: manifest.TreeHead, local: true, peer: true,
			pair: [2]fstest.MapFS{{"removed.yaml": {}}, {}},
		},
		{
			name: "head-only output change", side: manifest.TreeBase, local: true, peer: true,
			pair: [2]fstest.MapFS{{}, {"added.yaml": {}}},
		},
		{
			name: "unrelated input", side: manifest.TreeHead, local: true, peer: true,
			pair: [2]fstest.MapFS{{}, {}},
		},
		{name: "head", side: manifest.TreeHead, local: true},
		{name: "peer", side: manifest.TreeHead, peer: true},
		{name: "same owner", side: manifest.TreeHead, local: true, peer: true},
		{name: "unrelated owner", side: manifest.TreeHead, peer: true, otherOwner: true},
		{name: "structural owner", side: manifest.TreeHead, local: true, peer: true, structural: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}

	for _, tc := range []struct {
		name     string
		supplied bool
	}{
		{name: "explicit lookup with supplied owner", supplied: true},
		{name: "explicit lookup with structural production"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			closure := solverClosure(t, st, &solverFacts{store: st})
			closure.session.request = Request{Roots: []manifest.NamedResource{
				(&manifest.HelmRelease{Name: "selected", Namespace: "apps"}).Named(),
			}}
			closure.proposal.preparation = new(Preparation)
			t.Cleanup(closure.proposal.preparation.Retire)
			source := (&manifest.ExternalArtifact{Name: "package", Namespace: "apps"}).Named()
			owner := (&manifest.Kustomization{Name: "package-owner", Namespace: "apps"}).Named()
			lookup := LookupSource{Source: source}
			if tc.supplied {
				lookup.Materializer = owner
			}
			closure.proposal.Request.LookupSources = []LookupSource{lookup}
			closure.states[source] = admissionState{
				productionsKnown: true,
				productions:      []manifest.Production{{Owner: owner}},
			}
			counted := &materializerStatusReads{Store: st, owner: owner}
			closure.objects = counted
			closure.qualifySelectionProducers()
			assert.Diff(t, closure.proposal.demands, []Demand{{Identity: owner, Materialize: true}})
			assert.Equal(t, counted.reads, 1)
		})
	}

	t.Run("owner evidence", testMaterializerOwnerEvidence)
}

type materializerCase struct {
	name                    string
	side                    manifest.TreeSide
	local, peer, structural bool
	otherOwner              bool
	pair                    [2]fstest.MapFS
}

func (tc materializerCase) run(t *testing.T) {
	t.Parallel()
	st := store.New()
	var owner manifest.BaseManifest = &manifest.RawObject{
		APIVersion: manifest.SourceExtensionsDomain + "/v1beta1",
		Kind:       manifest.KindArtifactGenerator, Name: "generator", Namespace: "apps",
	}
	if tc.structural {
		owner = &manifest.Kustomization{Name: "builder", Namespace: "apps"}
	}
	id := owner.Named()
	st.AddObject(owner)
	facts := &solverFacts{
		store: st, sources: map[manifest.NamedResource]SourceFacts{},
		occurrences: make(map[manifest.NamedResource]OccurrenceFacts),
	}
	closure := solverClosure(t, st, facts)
	facts.context.Side, facts.context.SideKnown = tc.side, true
	closure.context = facts.context
	closure.proposal.preparation = &Preparation{}
	defer closure.proposal.preparation.Retire()
	closure.proposal.Request.construction = true
	artifact := &manifest.ExternalArtifact{Name: "package", Namespace: "apps"}
	source := artifact.Named()
	facts.sources[source] = SourceFacts{Paired: true}
	closure.states[source] = admissionState{productionsKnown: true}
	stores := [2]*store.Store{}
	stores[tc.side], stores[1-tc.side] = st, store.New()
	bootstrap := &store.SourceArtifact{FS: fstest.MapFS{}}
	stores[manifest.TreeBase].SetArtifact(manifest.BootstrapSourceID, bootstrap)
	stores[manifest.TreeHead].SetArtifact(manifest.BootstrapSourceID, bootstrap)
	paired := tc.pair[0] != nil
	if paired {
		stores[manifest.TreeHead].SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
			FS: fstest.MapFS{"outside.yaml": {}},
		})
		st.AddObject(artifact)
		st.SetArtifact(source, &store.SourceArtifact{FS: tc.pair[tc.side]})
	}
	if tc.peer {
		peer := stores[1-tc.side]
		peer.AddObject(owner)
		files := tc.pair[1-tc.side]
		peer.RecordEmitted(id, &store.Emission{Complete: true, Children: []store.Child{{Obj: artifact}}},
			map[manifest.NamedResource]*store.SourceArtifact{source: {FS: files}})
	}
	switch {
	case tc.local:
		closure.states[source] = admissionState{
			productionsKnown: true,
			productions: []manifest.Production{
				{Owner: id, Declaration: id, Source: manifest.BootstrapSourceID},
			},
		}
	case tc.otherOwner:
		root := t.TempDir()
		closure.context.Root, facts.context.Root = root, root
		production, _ := unrelatedMaterializer(facts, stores, tc.side, source)
		closure.states[source] = admissionState{
			productionsKnown: true, productions: []manifest.Production{production},
		}
	}
	changes, err := change.Qualify(
		t.Context(), nil, change.SourcePair{Stores: stores}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	if tc.peer && !paired {
		assert.Equal(t, changes.PendingSourceOwner(source, tc.side), id)
	}
	facts.changes = changes
	closure.proposal.Request.Changes = changes
	if tc.otherOwner {
		var owners []manifest.NamedResource
		var required []bool
		for materializer, owed := range closure.generatedSourceOwners(source) {
			owners = append(owners, materializer)
			required = append(required, owed)
		}
		assert.Diff(t, owners, []manifest.NamedResource{id, closure.states[source].productions[0].Owner})
		assert.Diff(t, required, []bool{true, false})
	}
	counted := &materializerStatusReads{Store: st, owner: id}
	closure.objects = counted
	consumer := (&manifest.Kustomization{Name: "consumer", Namespace: "apps"}).Named()
	facts.occurrences[consumer] = OccurrenceFacts{
		Identity: consumer, CatalogKnown: true, Refs: []manifest.Reference{{NamedResource: source}},
	}
	closure.identities = []admissionIdentity{{id: consumer}}
	closure.states[consumer] = admissionState{listed: true}
	changed := len(tc.pair[0])+len(tc.pair[1]) != 0
	prepare := !tc.structural && (!paired || changed)
	var want []Demand
	if prepare {
		want = []Demand{{Identity: id, Materialize: true}}
	}
	closure.demandPendingSources()
	assert.Diff(t, slices.Collect(closure.proposal.Demands()), want)
	assert.Equal(t, counted.reads, len(want))
	assert.Equal(t, closure.role(id).ordinary, prepare)

	assert.Equal(t, closure.initialSourceChanged(source), changed)
	assert.Diff(t, slices.Collect(closure.proposal.Demands()), want)
	assert.Equal(t, counted.reads, len(want))
	if tc.peer && !tc.structural && !paired {
		closure.proposal = &Admission{
			Request: closure.proposal.Request, preparation: closure.proposal.preparation,
		}
		closure.resetDemand()
		assert.Equal(t, closure.initialSourceChanged(source), false)
		assert.Diff(t, closure.proposal.demands, want)
		assert.Equal(t, counted.reads, 2)
		assert.Equal(t, closure.initialSourceChanged(source), false)
		assert.Diff(t, closure.proposal.demands, want)
		assert.Equal(t, counted.reads, 2)

		st.RecordEmitted(id, &store.Emission{Complete: true, Children: []store.Child{{Obj: artifact}}},
			map[manifest.NamedResource]*store.SourceArtifact{source: {FS: fstest.MapFS{}}})
		settled, err := change.Qualify(
			t.Context(), changes, change.SourcePair{Stores: stores}, manifest.WorkScope{},
		)
		assert.NoError(t, err)
		facts.changes = settled
		closure.proposal.Request.Changes = settled
		closure.proposal.demands = nil
		closure.resetDemand()
		assert.Equal(t, closure.initialSourceChanged(source), false)
		assert.Equal(t, len(closure.proposal.demands), 0)
		assert.Equal(t, counted.reads, 2)
	}
}

func unrelatedMaterializer(
	facts *solverFacts, stores [2]*store.Store, side manifest.TreeSide, output manifest.NamedResource,
) (manifest.Production, *manifest.GitRepository) {
	other := &manifest.RawObject{
		APIVersion: manifest.SourceExtensionsDomain + "/v1beta1",
		Kind:       manifest.KindArtifactGenerator, Name: "unrelated", Namespace: "apps",
		Spec: map[string]any{
			"sources": []any{map[string]any{
				"alias": "repo", "kind": manifest.KindGitRepository, "name": "input",
			}},
			"artifacts": []any{map[string]any{
				"name": output.Name,
				"copy": []any{map[string]any{"from": "@repo/payload", "to": "@artifact/"}},
			}},
		},
	}
	otherID := other.Named()
	repository := &manifest.GitRepository{Name: "input", Namespace: "apps"}
	inputID := repository.Named()
	input := &store.SourceArtifact{FS: fstest.MapFS{"payload/unchanged.yaml": {}}}
	facts.sources[inputID] = SourceFacts{Paired: true}
	for i, st := range stores {
		files := fstest.MapFS{"unrelated.yaml": {}}
		if manifest.TreeSide(i) == side {
			files["outside.yaml"] = &fstest.MapFile{}
		}
		st.AddObject(other)
		st.AddObject(repository)
		st.SetArtifact(inputID, input)
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{FS: files, LocalPath: facts.context.Root})
	}
	facts.sources[manifest.BootstrapSourceID] = SourceFacts{Paired: true}
	facts.index = manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		otherID: {Authored: otherID, File: "unrelated.yaml", Declaration: other, StaticOutput: true},
	}}
	facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{
		otherID: {
			Declaration: OccurrenceID{Source: facts.context.Root, File: "unrelated.yaml"},
			Identity:    otherID, Authored: otherID, Object: other, CatalogKnown: true, Located: true,
			Refs: slices.Collect(manifest.References(other, manifest.ReferenceRelations)),
		},
	}
	return manifest.Production{Owner: otherID, Declaration: otherID, Source: manifest.BootstrapSourceID}, repository
}

type materializerStatusReads struct {
	*store.Store

	owner manifest.NamedResource
	reads int
}

func (r *materializerStatusReads) GetStatus(id manifest.NamedResource) (store.StatusInfo, bool) {
	if id == r.owner {
		r.reads++
	}
	return r.Store.GetStatus(id)
}

func TestAdmission_GeneratedKustomizeReadsSettleAcrossSnapshots(t *testing.T) {
	t.Parallel()
	const removedFile = "shared/removed.yaml"
	base := fstest.MapFS{
		removedFile: {Data: []byte("removed")}, "shared/retained.yaml": {Data: []byte("same")},
	}
	head := fstest.MapFS{"shared/retained.yaml": {Data: []byte("same")}}
	stores, source, changes := generatedReadSnapshots(t, [2]fstest.MapFS{base, head})
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "app"}
	consumer.SourceRef.Kind, consumer.SourceRef.Name = source.Kind, source.Name

	for _, tc := range []struct {
		name      string
		baseFiles []string
		complete  bool
		want      affectedResult
	}{
		{name: "pending non-match"},
		{name: "early base match", baseFiles: []string{removedFile}, want: affectedResult{changed: true, settled: true}},
		{name: "settled non-match", complete: true, want: affectedResult{settled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			facts := &solverFacts{
				store: stores[1], changes: changes,
				context: SourceContext{Side: manifest.TreeHead, SideKnown: true},
			}
			id := consumer.Named()
			c := solverClosure(t, stores[1], facts)
			c.context, c.evaluationContext = facts.context, t.Context()
			c.proposal.Request.Changes = changes
			calls := 0
			c.session.BindKustomizeInputs(func(
				_ context.Context, observed manifest.NamedResource, declaration manifest.BaseManifest, facts *SourceFacts,
			) ([2]SourceReadPaths, bool) {
				calls++
				assert.Equal(t, observed, id)
				assert.Equal(t, declaration, manifest.BaseManifest(consumer))
				assert.Equal(t, facts.Source, source)
				return [2]SourceReadPaths{{Files: tc.baseFiles}, {Files: []string{"shared/retained.yaml"}}}, tc.complete
			})
			assert.Equal(t, c.affectedContentReads(id, consumer, &manifest.DiscoveryRecord{}), tc.want)
			for ref := range manifest.References(consumer, manifest.ReferenceRelations) {
				assert.Equal(t, c.reverseChanged(id, consumer, &ref), tc.want.changed)
			}
			assert.Equal(t, calls, 1)
		})
	}
}

func TestAdmission_GeneratedChartReadsSettleAcrossSnapshots(t *testing.T) {
	t.Parallel()
	const removed, added = "chart/old.yaml", "dependency/new.yaml"
	stores, source, changes := generatedReadSnapshots(t, [2]fstest.MapFS{
		{removed: {Data: []byte("old")}},
		{added: {Data: []byte("new")}},
	})
	_, snapshots, _ := changes.ForSource(source)
	for _, tc := range []struct {
		name string
		side manifest.TreeSide
	}{
		{name: "head", side: manifest.TreeHead},
		{name: "base", side: manifest.TreeBase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			facts := &solverFacts{
				store: stores[tc.side], changes: changes,
				context:     SourceContext{Side: tc.side, SideKnown: true},
				sources:     map[manifest.NamedResource]SourceFacts{source: {Paired: true}},
				occurrences: make(map[manifest.NamedResource]OccurrenceFacts),
			}
			calls := make(map[string][2]int)
			chartFacts := &chartAdmissionFacts{
				solverFacts: facts,
				read: func(_ context.Context, input *store.SourceArtifact, chart string) SourceReadPaths {
					chart = path.Clean(chart)
					side := manifest.TreeBase
					if input == snapshots[manifest.TreeHead] {
						side = manifest.TreeHead
					}
					counts := calls[chart]
					counts[side]++
					calls[chart] = counts
					switch {
					case chart == "file" && side == manifest.TreeBase:
						return SourceReadPaths{Files: []string{removed}}
					case chart == "subtree" && side == manifest.TreeHead:
						return SourceReadPaths{Roots: []string{"dependency"}}
					default:
						return SourceReadPaths{}
					}
				},
			}
			c := solverClosure(t, stores[tc.side], facts)
			c.facts = chartFacts
			c.context, c.evaluationContext = facts.context, t.Context()
			c.proposal.Request.Changes = changes

			for _, chart := range []string{"file", "subtree", "empty"} {
				consumer := &manifest.HelmRelease{Name: chart, Namespace: "apps"}
				id := consumer.Named()
				ref := manifest.Reference{
					NamedResource: source, Use: "artifact", ArtifactOnly: true, ChartPath: "./" + chart,
				}
				facts.occurrences[id] = OccurrenceFacts{
					Identity: id, Object: consumer, CatalogKnown: true, Refs: []manifest.Reference{ref},
				}
				want := affectedResult{changed: true, settled: true}
				assert.Equal(t, c.affectedContentReads(id, consumer, &manifest.DiscoveryRecord{}), want)
				ref.ChartPath = chart
				assert.Equal(t, c.reverseChanged(id, consumer, &ref), true)
				assert.Equal(t, calls[chart], [2]int{1, 1})
			}
		})
	}
}

func TestAdmission_ChartReadsPreserveReferenceFiles(t *testing.T) {
	t.Parallel()
	source := manifest.BootstrapSourceID
	delta := detectChanges(t, []string{"file"}, nil)
	for _, tc := range []struct {
		name, content string
	}{
		{name: "mutable"},
		{name: "identified", content: "generation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := &store.SourceArtifact{FS: fstest.MapFS{}, Content: tc.content}
			files := make([]string, 1, 4)
			files[0] = "chart/Chart.yaml"
			roots := make([]string, 1, 4)
			roots[0] = "chart/dependency"
			facts := &solverFacts{
				sources: map[manifest.NamedResource]SourceFacts{source: {
					Input: input, Snapshots: [2]*store.SourceArtifact{input.WithReads(new(store.ReadSet)), input},
					Delta: delta, Paired: true,
				}},
			}
			calls := 0
			c := solverClosure(t, store.New(), facts)
			c.context, c.evaluationContext = SourceContext{Side: manifest.TreeHead, SideKnown: true}, t.Context()
			c.facts = &chartAdmissionFacts{
				solverFacts: facts,
				read: func(context.Context, *store.SourceArtifact, string) SourceReadPaths {
					calls++
					return SourceReadPaths{Files: files, Roots: roots}
				},
			}
			ref := manifest.Reference{
				NamedResource: source, Use: "artifact", ArtifactOnly: true, ChartPath: "./chart",
				Files: []string{"./first.yaml"},
			}
			first := c.referenceReadPaths(&ref)
			ref.ChartPath, ref.Files = "chart", []string{"second.yaml", "../invalid.yaml"}
			second := c.referenceReadPaths(&ref)
			assert.Equal(t, calls, 1)
			assert.Diff(t, first, SourceReadPaths{
				Files: []string{"chart/Chart.yaml", "chart/Chart.yaml", "first.yaml"},
				Roots: []string{"chart/dependency", "chart/dependency"},
			})
			assert.Diff(t, second, SourceReadPaths{
				Files: []string{"chart/Chart.yaml", "chart/Chart.yaml", "second.yaml"},
				Roots: []string{"chart/dependency", "chart/dependency"},
			})
			assert.Diff(t, files[:cap(files)], []string{"chart/Chart.yaml", "", "", ""})
			assert.Diff(t, roots[:cap(roots)], []string{"chart/dependency", "", "", ""})
		})
	}
}

func TestAdmission_ChartReadsRetryOnSamePreparation(t *testing.T) {
	t.Parallel()
	const changedFile = "chart/values.yaml"
	source := manifest.BootstrapSourceID
	changes := detectChanges(t, []string{changedFile}, nil)
	st := store.New()
	consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	consumer.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{
		Spec: helmv2.HelmChartTemplateSpec{
			Chart: "./chart", SourceRef: &helmv2.CrossNamespaceObjectReference{
				Kind: source.Kind, Name: source.Name, Namespace: source.Namespace,
			},
		},
	}
	input := &store.SourceArtifact{LocalPath: t.TempDir()}
	st.SetArtifact(source, input)
	facts := &solverFacts{store: st, changes: changes, context: SourceContext{Root: input.LocalPath}}

	calls := 0
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	chartFacts := &chartAdmissionFacts{
		solverFacts: facts,
		read: func(readCtx context.Context, artifact *store.SourceArtifact, chart string) SourceReadPaths {
			assert.Equal(t, artifact.FS != nil, true)
			assert.Equal(t, chart, "chart")
			calls++
			if readCtx == ctx {
				cancel()
				return SourceReadPaths{Files: []string{"chart/partial.yaml"}}
			}
			return SourceReadPaths{Files: []string{changedFile}}
		},
	}
	session, err := New(st, chartFacts, Request{Changed: true})
	assert.NoError(t, err)
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	request := PrepareRequest{
		Changes: changes, Generation: []store.Child{{Obj: consumer}},
		Preparation: &preparation,
	}

	proposal, err := session.prepareTest(t.Context(), request)
	assert.NoError(t, err)
	proposal.Retire()
	assert.Equal(t, calls, 0)

	st.SetArtifact(source, input.WithSourceView(fstest.MapFS{}))
	other := consumer.Clone()
	other.Name = "other"
	request.Generation = []store.Child{{Obj: consumer}, {Obj: other}}
	proposal, err = session.prepareTest(ctx, request)
	assert.Equal(t, err, context.Canceled)
	assert.Equal(t, proposal == nil, true)
	assert.Equal(t, calls, 2)

	request.Generation = []store.Child{{Obj: consumer}}
	for i := range 2 {
		facts.changes = detectChanges(t, []string{changedFile}, nil)
		request.Changes = facts.changes
		proposal, err = session.prepareTest(t.Context(), request)
		assert.NoError(t, err)
		assert.Diff(t, proposal.Selected, []manifest.NamedResource{consumer.Named()})
		proposal.Retire()
		assert.Equal(t, calls, i+3)
	}
}

type chartAdmissionFacts struct {
	*solverFacts

	read func(context.Context, *store.SourceArtifact, string) SourceReadPaths
}

func (f *chartAdmissionFacts) ChartReadPaths(
	ctx context.Context, input *store.SourceArtifact, chart string,
) SourceReadPaths {
	return f.read(ctx, input, chart)
}

func generatedReadSnapshots(
	t *testing.T, files [2]fstest.MapFS,
) ([2]*store.Store, manifest.NamedResource, *change.Set) {
	t.Helper()
	generator := &manifest.RawObject{
		APIVersion: manifest.SourceExtensionsDomain + "/v1beta1",
		Kind:       manifest.KindArtifactGenerator, Name: "generator", Namespace: "apps",
	}
	artifact := generator.ExternalArtifact(metav1.ObjectMeta{Name: "output", Namespace: "apps"})
	source := artifact.Named()

	var stores [2]*store.Store
	for side := range stores {
		stores[side] = store.New()
		stores[side].AddObject(generator)
		stores[side].SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{FS: fstest.MapFS{}})
		stores[side].RecordEmitted(
			generator.Named(),
			&store.Emission{Complete: true, Children: []store.Child{{Obj: artifact}}},
			map[manifest.NamedResource]*store.SourceArtifact{
				source: {FS: files[side], Key: "apps/output"},
			},
		)
	}
	changes, err := change.Qualify(t.Context(), nil, change.SourcePair{Stores: stores}, manifest.WorkScope{})
	assert.NoError(t, err)
	return stores, source, changes
}
