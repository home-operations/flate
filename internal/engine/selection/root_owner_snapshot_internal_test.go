package selection

import (
	"fmt"
	"maps"
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type countedRootOwner struct {
	*manifest.Kustomization

	paths int
}

func (o *countedRootOwner) SourcePath() (manifest.NamedResource, string, bool) {
	o.paths++
	return o.Kustomization.SourcePath()
}

func TestAdmission_CoverageReusesOwnerCoordinates(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
	)
	owner := &countedRootOwner{Kustomization: &manifest.Kustomization{
		Name: "owner", Namespace: "apps", Path: ".",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.BootstrapSourceID.Kind,
			Namespace: manifest.BootstrapSourceID.Namespace,
			Name:      manifest.BootstrapSourceID.Name,
		},
	}}
	facts := &solverFacts{
		store:       st,
		index:       manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{},
	}
	facts.context.Root = root
	inputs := make([]manifest.NamedResource, 0, 3)
	for i := range 3 {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: fmt.Sprintf("input-%d", i)}
		inputs = append(inputs, id)
		facts.index.Records[id] = manifest.DiscoveryRecord{Path: filepath.Join(root, id.Name+".yaml")}
		facts.occurrences[id] = OccurrenceFacts{
			Identity: id, Authored: id, CatalogKnown: true,
			Declaration: OccurrenceID{Source: root, File: id.Name + ".yaml"},
		}
	}
	c := solverClosure(t, st, facts)
	counted := &countedAdmissionFacts{
		census: facts, occurrences: map[manifest.NamedResource]int{},
		roots: map[manifest.NamedResource]int{}, sources: map[manifest.NamedResource]int{},
	}
	c.facts = counted
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Close()
	c.objects, c.proposal.reads = &reads, &reads
	concreteOwner := rootOwnerFact(owner.Kustomization, c.rootReader())
	unrelated := facts.index.Records[inputs[0]]
	assert.Equal(t, c.bootstrapInputRequired(&concreteOwner, inputs[0], &unrelated, targetedClaimScope), false)
	assert.Equal(t, len(c.ownerClaims), 0)

	coordinates := rootOwnerFact(owner, c.rootReader())
	missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "missing"}
	assert.Equal(t, c.coversDeclaration(&coordinates, missing, targetedClaimScope), false)
	assert.Equal(t, len(c.ownerClaims), 0)
	assert.Equal(t, c.materializerTargetsMatch(
		owner.Named(),
		&OccurrenceFacts{Catalog: owner, Targets: []manifest.ProducerTarget{{NamedResource: inputs[0]}}},
		&materializerDemand{target: inputs[0]},
	), true)
	assert.Equal(t, len(c.ownerClaims), 0)
	assert.Equal(
		t,
		c.bootstrapInputRequired(&coordinates, inputs[0], new(facts.index.Records[inputs[0]]), targetedClaimScope),
		false,
	)
	assert.Equal(t, len(c.ownerClaims), 0)
	for _, id := range inputs {
		assert.Equal(t, c.coversDeclaration(&coordinates, id, targetedClaimScope), true)
		assert.Equal(t, counted.occurrences[id], 1)
	}
	assert.Equal(t, len(c.ownerClaims), 1)
	assert.Equal(t, owner.paths, 1)
	owner.Path = "../outside"
	coordinates = rootOwnerFact(owner, c.rootReader())
	assert.Equal(t, coordinates.Path, "")
	for _, id := range inputs {
		assert.Equal(t, c.coversDeclaration(&coordinates, id, targetedClaimScope), false)
	}
	assert.Equal(t, owner.paths, 2)
	owner.paths = 0
	parented := inputs[0]
	record := facts.index.Records[parented]
	record.Parent = owner.Named()
	facts.index.Records[parented] = record
	facts.index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
	c.ownersKnown = true
	assert.Equal(t, c.bootstrapPrerequisite(parented), false)
	assert.Equal(t, owner.paths, 0)
}

func TestAdmission_OwnerClaimsReuse(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
	)
	owner := &manifest.Kustomization{
		Name: "owner", Namespace: "apps", Path: "app",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "input"},
			{Kind: manifest.KindConfigMap, Name: "physical"},
			{Kind: manifest.KindConfigMap, Name: "outside"},
			{Kind: manifest.KindConfigMap, Name: "omitted"},
		}},
	}
	factory := (&manifest.ResourceSet{Name: "factory", Namespace: "apps"}).Named()
	st.AddObject(owner)
	facts := &solverFacts{
		store: st, index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			owner.Named(): {Productions: []manifest.Production{{Owner: factory}}},
		}},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{},
		roots:       map[manifest.NamedResource]RootEvidence{},
	}
	facts.context.Root = root
	declaration := func(name, file string) manifest.NamedResource {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}
		record := manifest.DiscoveryRecord{Path: filepath.Join(root, file), File: file, Authored: id}
		occurrence := OccurrenceFacts{
			Identity: id, Authored: id, CatalogKnown: true,
			Declaration: OccurrenceID{Source: root, File: file},
		}
		facts.index.Records[id], facts.occurrences[id] = record, occurrence
		facts.roots[id] = RootEvidence{
			Identity: id, Occurrence: occurrence, Record: RootRecordFrom(&record),
			Scope: RootScopeFacts{NamespaceEligible: true, Initial: true},
		}
		return id
	}
	input := declaration("input", "app/input.yaml")
	physical := declaration("physical", "app/physical.yaml")
	unrelated := declaration("unrelated", "app/unrelated.yaml")
	outside := declaration("outside", "../outside.yaml")
	generator := declaration("generator", "app/kustomization.yaml")
	omitted := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "omitted"}
	facts.index.Records[omitted] = manifest.DiscoveryRecord{
		Productions: []manifest.Production{{Owner: generator, Source: manifest.BootstrapSourceID}},
	}
	st.RecordEmitted(generator, &store.Emission{Complete: true}, nil)
	physicalRoot := facts.roots[physical]
	physicalRoot.Scope.Owners = []RootOwnerFacts{{Identity: owner.Named()}}
	facts.roots[physical] = physicalRoot

	c := solverClosure(t, st, facts)
	c.context.Root = root
	assert.Equal(t, c.bootstrapPrerequisite(unrelated), false)
	assert.Equal(t, len(c.ownerCandidates), 1)
	assert.Equal(t, c.bootstrapPrerequisite(outside), false)
	assert.Equal(t, c.bootstrapPrerequisite(generator), false)
	assert.Equal(t, c.ownerCandidates[0].claims, nil)
	assert.Equal(t, len(c.ownerClaims), 0)

	assert.Equal(t, c.bootstrapPrerequisite(physical), true)
	assert.Equal(t, c.ownerCandidates[0].claims, nil)
	assert.Equal(t, len(c.ownerClaims), 1)
	first := c.ownerClaims[c.ownerCandidates[0].claimKey(root)]
	assert.Equal(t, first != nil, true)
	assert.Equal(t, c.bootstrapPrerequisite(physical), true)
	live := c.rootSupport(input)
	assert.Equal(t, Root(live).Owner, owner.Named())
	assert.Equal(t, c.ownerCandidates[0].claims == first, true)
	assert.Equal(t, c.bootstrapPrerequisite(input), true)
	assert.Equal(t, c.bootstrapPrerequisite(input), true)
	assert.Equal(t, c.ownerCandidates[0].claims == first, true)
	assert.Equal(t, len(c.ownerClaims), 1)

	invalid := RootCoveringOwner{Object: owner, Identity: owner.Named()}
	borrowed := c.rootOwnerClaims(&invalid, root, targetedClaimScope)
	assert.Equal(t, borrowed == &facts.index, true)
	assert.Equal(t, c.ownerClaims[invalid.claimKey(root)] == borrowed, true)
	assert.Equal(t, c.rootOwnerClaims(&invalid, root, targetedClaimScope) == borrowed, true)

	replacement := owner.Clone()
	replacement.Path = "other"
	st.AddObject(replacement)
	updated := *facts
	updated.index.Records = maps.Clone(facts.index.Records)
	facts = &updated
	c.facts = facts
	added := declaration("added", "other/added.yaml")
	assert.NoError(t, c.refreshPrerequisites(t.Context(), nil, &PrepareRequest{}))
	assert.Equal(t, c.bootstrapPrerequisite(input), false)
	refreshed := c.ownerCandidates[0].claims
	assert.Equal(t, refreshed != first, true)
	_, previous := first.Records[added]
	_, current := refreshed.Records[added]
	assert.Equal(t, previous, false)
	assert.Equal(t, current, true)
	_, borrowedBefore := borrowed.Records[added]
	assert.Equal(t, borrowedBefore, false)
	assert.Equal(t, c.ownerCandidates[0].Object, manifest.BaseManifest(replacement))
	assert.Equal(t, Root(live).Owner, owner.Named())
	assert.Diff(t, live.Scope.Claims["app/"], []manifest.NamedResource{owner.Named()})
	assert.Diff(t, refreshed.Claims["other/"], []manifest.NamedResource{owner.Named()})
}

func TestAdmission_BootstrapClaimScopes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		subdir string
	}{
		{name: "same root"},
		{name: "different roots", subdir: "checkout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			contextRoot := t.TempDir()
			root := filepath.Join(contextRoot, tc.subdir)
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			owner := &manifest.Kustomization{
				Name: "owner", Namespace: "apps", Path: "app",
				DependsOn: []kustomizev1.DependencyReference{{Name: "subject"}},
			}
			subject := &manifest.Kustomization{Name: "subject", Namespace: "apps", Path: "app"}
			factory := (&manifest.ResourceSet{Name: "factory", Namespace: "apps"}).Named()
			productions := []manifest.Production{{Owner: factory}}
			id := subject.Named()
			record := manifest.DiscoveryRecord{
				Path: filepath.Join(root, "app", "subject.yaml"),
				File: "app/subject.yaml", Authored: id, Productions: productions,
			}
			occurrence := OccurrenceFacts{
				Identity: id, Authored: id, CatalogKnown: true,
				Declaration: OccurrenceID{Source: root, File: record.File},
			}
			facts := &solverFacts{
				store: st, index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner.Named(): {Productions: productions}, id: record,
				}},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{id: occurrence},
				roots: map[manifest.NamedResource]RootEvidence{id: {
					Identity: id, Occurrence: occurrence, Record: RootRecordFrom(&record),
					Scope: RootScopeFacts{NamespaceEligible: true, Initial: true},
				}},
			}
			facts.context.Root = contextRoot
			st.AddObject(owner)
			st.AddObject(subject)
			c := solverClosure(t, st, facts)
			c.context.Root = contextRoot

			coordinates := rootOwnerFact(owner, c.rootReader())
			assert.Equal(t, c.coversDeclaration(&coordinates, id, targetedClaimScope), true)
			targeted := c.ownerClaims[coordinates.claimKey(contextRoot)]
			targetedPrefix := filepath.ToSlash(filepath.Join(tc.subdir, "app")) + "/"
			assert.Diff(t, targeted.Claims[targetedPrefix], []manifest.NamedResource{owner.Named()})
			assert.Equal(t, c.bootstrapPrerequisite(id), false)
			full := c.ownerCandidates[0].claims
			assert.Equal(t, full != targeted, true)
			assert.Diff(t, full.Claims["app/"], []manifest.NamedResource{owner.Named(), id})
			assert.Equal(t, c.rootEvidence(id).Independent, true)
			assert.Equal(t, c.ownerCandidates[0].claims == full, true)
			assert.Equal(t, c.coversDeclaration(&coordinates, id, targetedClaimScope), true)
			assert.Equal(t, c.ownerClaims[coordinates.claimKey(contextRoot)] == targeted, true)
		})
	}
}

func TestAdmission_EmptyRootOwnerInventoryShared(t *testing.T) {
	t.Parallel()
	session := newSolverSession(t, store.New(), nil, Request{Changed: true})
	facts := session.facts.(*solverFacts)
	facts.roots = map[manifest.NamedResource]RootEvidence{}
	facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{}
	closure := solverClosure(t, session.store, facts)
	reader := &admissionArtifactReads{admissionReader: session.store}
	closure.objects = reader
	roots := make([]manifest.NamedResource, 0, 3)
	for i := range 3 {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: fmt.Sprintf("root-%d", i)}
		roots = append(roots, id)
		record := manifest.DiscoveryRecord{Path: filepath.Join(t.TempDir(), "object.yaml")}
		facts.index.Records[id] = record
		facts.roots[id] = RootEvidence{
			Identity: id,
			Captured: true,
			Record:   RootRecordFrom(&record),
			Scope:    RootScopeFacts{NamespaceEligible: true, Initial: true},
		}
	}
	expected := 0
	for info := range manifest.Kinds() {
		if info.Structural {
			expected++
		}
	}
	for _, id := range roots {
		closure.rootSupport(id)
	}
	assert.Equal(t, closure.ownersKnown, true)
	assert.Equal(t, len(closure.ownerCandidates), 0)
	assert.Equal(t, reader.lists, expected)
	assert.NoError(t, closure.refreshPrerequisites(t.Context(), nil, &PrepareRequest{}))
	for _, id := range roots {
		closure.rootSupport(id)
	}
	assert.Equal(t, closure.ownersKnown, true)
	assert.Equal(t, len(closure.ownerCandidates), 0)
	assert.Equal(t, reader.lists, 2*expected)
}

func TestAdmission_AuthoredInputOutsideBootstrapInventory(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                        string
		inside, needed, nonSource, unloaded         bool
		bootstrap, inactive, indexOnly, transformed bool
		noRootOccurrence, want                      bool
	}{
		{name: "needed outside inventory", needed: true, want: true},
		{name: "located outside inventory without root occurrence", needed: true, noRootOccurrence: true, want: true},
		{name: "transformed source outside inventory", needed: true, transformed: true},
		{name: "unrequested outside inventory"},
		{name: "needed inside inventory", inside: true, needed: true},
		{name: "non-source", nonSource: true, needed: true},
		{name: "unloaded source", unloaded: true, needed: true},
		{name: "bootstrap source", bootstrap: true, needed: true},
		{name: "inactive source", inactive: true, needed: true},
		{name: "index-only source", indexOnly: true, needed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			dir := t.TempDir()
			input := &store.SourceArtifact{LocalPath: dir, Provider: store.ProviderInput, FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			var object manifest.BaseManifest = &manifest.HelmRepository{Name: "repository", Namespace: "apps"}
			if tc.nonSource {
				object = &manifest.ConfigMap{Name: "values", Namespace: "apps"}
			}
			id := object.Named()
			owner := (&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "apps"}).Named()
			file := filepath.Join(dir, "apps", "repository.yaml")
			record := manifest.DiscoveryRecord{Path: file, Parent: owner, Authored: id}
			if tc.transformed {
				record.Productions = []manifest.Production{{
					Source: manifest.BootstrapSourceID, Owner: owner, Authored: id,
					Declaration: owner, Path: file, File: "apps/repository.yaml",
				}}
			}
			occurrence := OccurrenceFacts{
				Identity: id, Authored: id, Catalog: object, CatalogKnown: true, Mounted: input,
				Declaration: OccurrenceID{Source: dir, File: "apps/repository.yaml"},
			}
			eligibility := RootEligible
			if tc.inactive {
				eligibility = RootInactive
			}
			occurrences := map[manifest.NamedResource]OccurrenceFacts{id: occurrence}
			if tc.indexOnly {
				occurrence = OccurrenceFacts{}
				delete(occurrences, id)
			}
			if tc.noRootOccurrence {
				occurrence = OccurrenceFacts{}
			}
			facts := &solverFacts{
				store:   st,
				context: SourceContext{Root: dir},
				index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: record},
				},
				occurrences: occurrences,
				roots: map[manifest.NamedResource]RootEvidence{id: {
					Identity: id, Record: RootRecordFrom(&record), Occurrence: occurrence,
					Scope: RootScopeFacts{
						Path: file, Eligibility: eligibility, NamespaceEligible: true,
						Loaded:    !tc.unloaded && !tc.indexOnly,
						Bootstrap: tc.bootstrap, EntryKustomize: true, OutsideInventory: !tc.inside,
						Owners: []RootOwnerFacts{{Identity: owner}},
					},
				}},
			}
			c := solverClosure(t, st, facts)
			c.states[id] = admissionState{role: admissionRole{needed: tc.needed}}
			evidence := declarationEvidence{owner: owner}
			c.classifyAuthoredDeclaration(id, &evidence)
			assert.Equal(t, evidence.authored, tc.want)
			assert.Equal(t, evidence.owner == (manifest.NamedResource{}), tc.want)
			if tc.transformed {
				c.states[id] = admissionState{role: admissionRole{needed: true, ordinary: true}}
				declaration := c.declarationEvidence(id)
				assert.Equal(t, declaration.owner, owner)
				assert.Equal(t, c.hydrateDeclaration(t.Context(), id, nil, &declaration), nil)
			}
		})
	}
}
