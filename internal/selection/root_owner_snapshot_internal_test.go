package selection

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
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
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	owner := &countedRootOwner{Kustomization: &manifest.Kustomization{
		Name: "owner", Namespace: "apps", Path: ".",
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
	coordinates := c.rootOwnerFacts(owner)
	for _, id := range inputs {
		assert.Equal(t, c.coversDeclaration(&coordinates, id), true)
		assert.Equal(t, counted.occurrences[id], 1)
	}
	assert.Equal(t, owner.paths, 1)
	owner.Path = "../outside"
	coordinates = c.rootOwnerFacts(owner)
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
