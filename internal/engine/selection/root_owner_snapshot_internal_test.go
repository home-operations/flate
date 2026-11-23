package selection

import (
	"fmt"
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
	assert.Equal(t, c.bootstrapInputRequired(&concreteOwner, inputs[0], &unrelated), false)
	assert.Equal(t, len(c.ownerClaims), 0)

	coordinates := rootOwnerFact(owner, c.rootReader())
	missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "missing"}
	assert.Equal(t, c.coversDeclaration(&coordinates, missing), false)
	assert.Equal(t, len(c.ownerClaims), 0)
	assert.Equal(t, c.materializerTargetsMatch(
		owner.Named(),
		&OccurrenceFacts{Catalog: owner, Targets: []manifest.ProducerTarget{{NamedResource: inputs[0]}}},
		&materializerDemand{target: inputs[0]},
	), true)
	assert.Equal(t, len(c.ownerClaims), 0)
	for _, id := range inputs {
		assert.Equal(t, c.coversDeclaration(&coordinates, id), true)
		assert.Equal(t, counted.occurrences[id], 1)
	}
	assert.Equal(t, len(c.ownerClaims), 1)
	assert.Equal(t, owner.paths, 1)
	owner.Path = "../outside"
	coordinates = rootOwnerFact(owner, c.rootReader())
	assert.Equal(t, coordinates.Path, "")
	for _, id := range inputs {
		assert.Equal(t, c.coversDeclaration(&coordinates, id), false)
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
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.Path = "."
	facts := &solverFacts{
		store: st, index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{},
	}
	facts.context.Root = root
	c := solverClosure(t, st, facts)
	c.context.Root = root
	coordinates := rootOwnerFact(owner, c.rootReader())
	first := c.rootOwnerClaims(&coordinates)
	repeated := c.rootOwnerClaims(&coordinates)
	assert.Equal(t, repeated == first, true)
	added := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "added"}
	facts.index.Records[added] = manifest.DiscoveryRecord{Path: filepath.Join(root, "added.yaml")}
	assert.NoError(t, c.refreshPrerequisites(t.Context(), nil, &PrepareRequest{}))
	refreshed := c.rootOwnerClaims(&coordinates)
	assert.Equal(t, refreshed == first, false)
	_, previous := first.Records[added]
	_, current := refreshed.Records[added]
	assert.Equal(t, previous, false)
	assert.Equal(t, current, true)
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
