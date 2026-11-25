package selection

import (
	"strings"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_InputChangesQualifiedOnce(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"selected.yaml", "unrelated.yaml"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
			)
			changes := detectChanges(t, []string{changed}, []string{changed})
			facts := &solverFacts{
				store:   st,
				changes: changes,
				index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}
			facts.context.Root = root
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			inputs := make([]string, 100)
			for i := range inputs {
				inputs[i] = "other-" + strings.Repeat("x", i) + ".yaml"
			}
			inputs[len(inputs)-1] = "selected.yaml"
			facts.index.Records[id] = manifest.DiscoveryRecord{File: "settings.yaml", Inputs: inputs}
			var captured store.ReadSet
			captured.Init(st, id, nil)
			t.Cleanup(captured.Close)
			var work manifest.WorkRecorder
			captured.SetWork(&work)
			reads := &admissionArtifactReads{admissionReader: &captured}
			facts.reader = reads
			c := solverClosure(t, st, facts)
			c.context = facts.context
			c.objects = reads
			c.proposal.reads = &captured
			c.proposal.Request.Changes = changes
			reader := manifest.NamedResource{
				Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization,
				Namespace: "apps", Name: "reader",
			}
			ref := &manifest.Reference{}
			ref.Ordering = true
			assert.Equal(t, c.reverseInputChanged(reader, id, ref), false)
			assert.Equal(t, c.states[reader].readinessAffected, false)
			assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(0))
			if changed == "selected.yaml" {
				c.markChanged(id)
				assert.Equal(t, c.reverseInputChanged(reader, id, ref), false)
				assert.Equal(t, c.states[reader].readinessAffected, true)
				assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(0))
			}
			assert.Equal(t, c.affected(id, nil), changed == "selected.yaml")
			calls := sourceCaptureCount(&captured)
			assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(1))
			assert.Equal(t, c.affected(id, nil), changed == "selected.yaml")
			assert.Equal(t, sourceCaptureCount(&captured), calls)
			assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(1))
			if sourceCaptureCount(&captured) > 3 {
				t.Fatalf(
					"qualified one changed path %d times for %d recorded inputs; want at most 3 artifact reads",
					sourceCaptureCount(&captured),
					len(inputs),
				)
			}
		})
	}
}

func TestAdmission_ReverseInputForIndexedReader(t *testing.T) {
	t.Parallel()
	for _, changedKey := range []string{"selected", "unrelated"} {
		t.Run(changedKey, func(t *testing.T) {
			t.Parallel()
			reader := &manifest.HelmRelease{
				Name: "reader", Namespace: "apps",
				ValuesFrom: []helmv2.ValuesReference{{Kind: "ConfigMap", Name: "settings", ValuesKey: "selected"}},
			}
			before := &manifest.ConfigMap{
				Name: "settings", Namespace: reader.Namespace,
				Data: map[string]any{"selected": "data: before", "unrelated": "data: before"},
			}
			after := &manifest.ConfigMap{
				Name: before.Name, Namespace: before.Namespace,
				Data: map[string]any{"selected": "data: before", "unrelated": "data: before"},
			}
			after.Data[changedKey] = "data: after"
			input := after.Named()
			stores := [2]*store.Store{store.New(), store.New()}
			for side, obj := range [2]*manifest.ConfigMap{before, after} {
				stores[side].SetArtifact(
					manifest.BootstrapSourceID,
					&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: t.TempDir(), FS: fstest.MapFS{}},
				)
				stores[side].AddObject(obj)
			}
			changes, err := change.Qualify(
				t.Context(), detectChanges(t, []string{"settings.yaml"}, []string{"settings.yaml"}),
				change.SourcePair{Stores: stores}, manifest.WorkScope{},
			)
			assert.NoError(t, err)
			id := reader.Named()
			declaration := store.Child{Obj: reader}
			prepared := map[manifest.NamedResource]store.Child{id: declaration}
			changes, err = changes.WithDeclarations(t.Context(), change.DeclarationInputs{
				Stores: stores, Prepared: [2]map[manifest.NamedResource]store.Child{prepared, prepared},
			})
			assert.NoError(t, err)
			facts := &solverFacts{
				store: stores[1], changes: changes,
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					id: {Declaration: reader},
				}},
				context: SourceContext{SideKnown: true, Side: manifest.TreeHead},
				sources: map[manifest.NamedResource]SourceFacts{manifest.BootstrapSourceID: {Paired: true}},
			}
			closure := solverClosure(t, stores[1], facts)
			closure.context = facts.context
			closure.proposal.Request.Changes = changes
			closure.states[input] = admissionState{role: admissionRole{dirty: true}}
			assert.Equal(t, stores[1].GetObject(id), nil)
			assert.Equal(t, closure.reverseInputRole(id, admissionRole{}, true).dirty, changedKey == "selected")
		})
	}
}

func TestAdmission_OrderingKeepsDeclarationStable(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
	owner := &manifest.Kustomization{
		Name: "owner", Namespace: target.Namespace,
		DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}},
	}
	st.AddObject(target)
	st.AddObject(owner)
	const path = "edited.yaml"
	root := t.TempDir()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	changes := detectChanges(t, []string{path}, []string{path})
	facts := &solverFacts{store: st, changes: changes, context: SourceContext{Root: root}}
	c := solverClosure(t, st, facts)
	c.context = facts.context
	c.proposal.Request.Changes = changes
	var reads store.ReadSet
	reads.Init(st, owner.Named(), nil)
	t.Cleanup(reads.Close)
	var work manifest.WorkRecorder
	reads.SetWork(&work)
	c.objects = &reads
	c.states[target.Named()] = admissionState{role: admissionRole{dirty: true}}
	stable := true
	c.checkDeclarationReferences(owner.Named(), &stable)
	assert.Equal(t, stable, true)
	noInputs := &manifest.DiscoveryRecord{}
	c.checkDeclarationFiles(manifest.BootstrapSourceID, &stable, noInputs, noInputs)
	assert.Equal(t, stable, true)
	assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(0))
	c.checkDeclarationFiles(
		manifest.BootstrapSourceID,
		&stable,
		&manifest.DiscoveryRecord{Inputs: []string{path}},
		noInputs,
	)
	assert.Equal(t, stable, false)
	assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(1))
}

func TestAdmission_DeclarationStabilityUsesRemappedProduction(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := (&manifest.Kustomization{Name: "owner", Namespace: "apps"}).Named()
	authored := (&manifest.ConfigMap{Name: "authored", Namespace: "raw"}).Named()
	effective := (&manifest.ConfigMap{Name: "effective", Namespace: "apps"}).Named()
	root := t.TempDir()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	changes := detectChanges(t, nil, []string{"unrelated.yaml"})
	facts := &solverFacts{
		store: st, changes: changes, context: SourceContext{Root: root},
		index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			authored: {Authored: authored, File: "package/authored.yaml"},
			owner:    {Authored: owner, File: "owner.yaml"},
		}},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{
			authored: {
				Declaration: OccurrenceID{Source: root, File: "package/authored.yaml"},
				Identity:    authored, Authored: authored, CatalogKnown: true, RequiresBuild: true,
			},
		},
	}
	c := solverClosure(t, st, facts)
	c.context = facts.context
	c.proposal.Request.Changes = changes
	c.states[effective] = admissionState{
		productionsKnown: true,
		productions: []manifest.Production{{
			Owner: owner, Declaration: authored, Authored: authored, Source: manifest.BootstrapSourceID,
			File: "package/authored.yaml",
		}},
	}
	assert.Equal(t, c.compareDeclarationFile(effective, owner, &manifest.DiscoveryRecord{}, false),
		declarationStability{stable: true, known: true})

	unclaimed := &manifest.DiscoveryRecord{Parent: owner}
	assert.Equal(t, c.compareDeclarationFile(authored, owner, unclaimed, false), declarationStability{})

	other := owner
	other.Name = "foreign"
	facts.index.Records[other] = manifest.DiscoveryRecord{Authored: other, File: "foreign.yaml"}
	located := facts.occurrences[authored]
	located.RequiresBuild = false
	facts.occurrences[authored] = located
	assert.Equal(t, c.compareDeclarationFile(authored, owner, unclaimed, false),
		declarationStability{stable: true, known: true})
	assert.Equal(t, c.compareDeclarationFile(authored, other, unclaimed, false), declarationStability{})

	hidden := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	second := owner
	second.Name = "second"
	for _, tc := range []struct {
		name   string
		owners []manifest.NamedResource
		want   manifest.NamedResource
	}{
		{"first visible", []manifest.NamedResource{hidden, owner, second}, owner},
		{"last hidden", []manifest.NamedResource{hidden, manifest.BootstrapSourceID}, manifest.BootstrapSourceID},
		{"fallback", []manifest.NamedResource{{}, effective}, second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			productions := make([]manifest.Production, 0, len(tc.owners))
			for _, id := range tc.owners {
				productions = append(productions, manifest.Production{Owner: id})
			}
			closure := admissionClosure{states: map[manifest.NamedResource]admissionState{
				effective: {productionsKnown: true, productions: productions},
			}}
			assert.Equal(t, closure.declarationProductionOwner(effective, second), tc.want)
		})
	}
}

func sourceCaptureCount(reads *store.ReadSet) int {
	count := 0
	for id := range reads.Inputs() {
		if id.KindInfo().SourceArtifact {
			count++
		}
	}
	return count
}

func TestAdmission_ContentPathsSettleBeforeReach(t *testing.T) {
	t.Parallel()
	for _, result := range []string{"unchanged", "changed", "unavailable"} {
		t.Run(result, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
			)
			changes := detectChanges(t, []string{"changed/file.yaml"}, []string{"changed/file.yaml"})
			facts := &solverFacts{store: st, changes: changes}
			facts.context.Root = root
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
			facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{id: {
				ContentRoots: map[manifest.NamedResource][]string{
					manifest.BootstrapSourceID: {"one/../first", "two/../second"},
				},
			}}
			var captured store.ReadSet
			captured.Init(st, id, nil)
			defer captured.Retire()
			c := solverClosure(t, st, facts)
			c.context = facts.context
			c.objects = &captured
			c.proposal.reads = &captured
			c.proposal.Request.Changes = changes
			c.proposal.preparation = &Preparation{}
			assert.Equal(t, c.affected(id, nil), false)
			assert.Equal(t, c.states[id].affectedKnown, false)
			assert.Equal(t, len(c.proposal.demands), 2)
			acquired := make([]Acquisition, 0, len(c.proposal.demands))
			for i, demand := range c.proposal.demands {
				resolved := "unrelated"
				if i == 1 && result == "changed" {
					resolved = "changed"
				}
				acquired = append(
					acquired,
					Acquisition{Demand: demand, Root: resolved, Available: result != "unavailable", Reads: &captured},
				)
			}
			c.proposal.preparation.acceptAcquired(acquired)
			c.proposal.demands = nil
			assert.Equal(t, c.affected(id, nil), result != "unchanged")
			assert.Equal(t, c.states[id].affectedKnown, true)
			assert.Equal(t, len(c.proposal.demands), 0)
		})
	}
}

func TestAdmission_DirectoryReadersRespectContentRoots(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, file  string
		readRoots   int
		readerEdges uint64
		changed     bool
		mixed       bool
		materialize bool
		pinned      bool
	}{
		{name: "declared subtree", file: "apps/owner/new.yaml", readRoots: 1, changed: true},
		{name: "observed external input", file: "shared/input.yaml", readerEdges: 3, changed: true},
		{name: "pinned unpaired input", file: "shared/input.yaml", readerEdges: 2, pinned: true},
		{name: "indexed producer", file: "shared/input.yaml", readerEdges: 3, changed: true, materialize: true},
		{name: "unobserved external descendant", file: "shared/nested/unrelated.yaml", readRoots: 1},
		{
			name: "distinct direct and ancestor readers", file: "shared/nested/unrelated.yaml",
			readRoots: 1, readerEdges: 3, mixed: true,
		},
		{name: "exact ancestor reader is excluded", file: "apps/owner/new.yaml", readRoots: 1, mixed: true, changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
			)
			var owner manifest.BaseManifest = &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps/owner"}
			var want []Demand
			if tc.materialize {
				owner = &manifest.RawObject{
					APIVersion: manifest.SourceExtensionsDomain + "/v1beta1",
					Kind:       manifest.KindArtifactGenerator, Name: "owner", Namespace: "apps",
				}
				want = []Demand{{Identity: owner.Named(), Materialize: true}}
			}
			st.AddObject(owner)
			reader := manifest.FileReader{
				Reader:      owner.Named(),
				Source:      manifest.BootstrapSourceID,
				Materialize: tc.materialize,
			}
			facts := &solverFacts{
				store: st, changes: detectChanges(t, []string{tc.file}, []string{tc.file}),
				context: SourceContext{Root: root},
				index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{owner.Named(): {Pinned: tc.pinned}},
					Readers: map[string][]manifest.FileReader{
						"apps/owner": {reader}, "shared": {reader}, "shared/input.yaml": {reader},
					},
				},
			}
			facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{owner.Named(): {
				Identity: owner.Named(), Authored: owner.Named(), CatalogKnown: true,
				Source: manifest.BootstrapSourceID, Roots: []string{"apps/owner"},
			}}
			direct := (&manifest.Kustomization{Name: "direct", Namespace: "apps"}).Named()
			exact := (&manifest.Kustomization{Name: "exact", Namespace: "apps"}).Named()
			if tc.mixed {
				facts.index.Readers[tc.file] = []manifest.FileReader{{Reader: direct, Source: reader.Source}}
				facts.index.Records[direct] = manifest.DiscoveryRecord{
					ContentRoots: map[manifest.NamedResource][]string{reader.Source: {"elsewhere"}},
				}
			}
			if tc.mixed && tc.changed {
				facts.index.Readers["apps/owner"] = append(facts.index.Readers["apps/owner"], manifest.FileReader{
					Reader: exact, Source: reader.Source, Exact: true,
				})
				facts.index.Records[exact] = manifest.DiscoveryRecord{
					ContentRoots: map[manifest.NamedResource][]string{reader.Source: {"apps/owner"}},
				}
			}
			c := solverClosure(t, st, facts)
			counted := &countedAdmissionFacts{census: facts, rootPaths: map[manifest.NamedResource]int{}}
			c.facts = counted
			c.context = facts.context
			c.proposal.Request.Changes = facts.changes
			c.proposal.Request.construction = tc.materialize
			c.proposal.preparation = new(Preparation)
			t.Cleanup(c.proposal.preparation.Retire)
			source := c.boundSource(reader.Source)
			assert.Equal(t, source.Paired, false)
			assert.Equal(t, c.fileChanged(source, tc.file).changed, true)
			assert.Equal(t, c.changedClaim(&reader, tc.file, false), tc.changed)
			assert.Equal(t, counted.rootPaths[owner.Named()], tc.readRoots)
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			t.Cleanup(reads.Close)
			var work manifest.WorkRecorder
			reads.SetWork(&work)
			c.objects = &reads
			c.seedChangedReaders(tc.file)
			assert.Equal(t, c.states[owner.Named()].seed, tc.changed && !tc.materialize)
			assert.Diff(t, c.proposal.demands, want)
			if tc.readerEdges != 0 {
				assert.Equal(t, work.Scope().Admission().Counts.ReaderEdges, tc.readerEdges)
			}
			if tc.mixed {
				assert.Equal(t, c.states[direct].seed, true)
				assert.Equal(t, c.states[exact].seed, false)
			}
		})
	}
}

func TestAdmission_SourceUsesPreparedOwner(t *testing.T) {
	t.Parallel()
	child := &manifest.HelmRelease{Name: "demo", Namespace: "apps"}
	id := child.Named()
	prepared := (&manifest.Kustomization{Name: "prepared", Namespace: "apps"}).Named()
	incoming := (&manifest.Kustomization{Name: "incoming", Namespace: "apps"}).Named()
	source := (&manifest.GitRepository{Name: "package", Namespace: "apps"}).Named()
	other := manifest.Production{Owner: incoming, Source: manifest.BootstrapSourceID}
	facts := &solverFacts{index: manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			id: {
				Builder: "app/kustomization.yaml", BuilderOwner: prepared,
				Productions: []manifest.Production{other, {Owner: prepared, Source: source}},
			},
		},
	}}
	c := admissionClosure{
		facts: facts,
		states: map[manifest.NamedResource]admissionState{
			id: {productionsKnown: true, productions: []manifest.Production{other}},
		},
	}
	assert.Equal(t, c.source(id), source)
	state := c.states[id]
	state.incoming = 1
	c.states[id] = state
	c.owner, c.generation = incoming, []store.Child{{Obj: child}}
	assert.Equal(t, c.source(id), other.Source)
}
