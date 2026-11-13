package selection

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func capturedClosure(
	t *testing.T,
	st *store.Store,
	index *manifest.DiscoveryIndex,
	emitter manifest.NamedResource,
) admissionClosure {
	t.Helper()
	c := solverClosure(t, st, &solverFacts{store: st, index: *index})
	publishDecision(
		t,
		c.session,
		&Admission{
			Needed:   []manifest.NamedResource{emitter},
			Selected: []manifest.NamedResource{emitter},
			Dirty:    []manifest.NamedResource{emitter},
			Ordinary: []manifest.NamedResource{emitter},
		},
		0,
	)
	return c
}

func (c *admissionClosure) evaluateCaptured(
	t *testing.T,
	emitter manifest.NamedResource,
	child manifest.BaseManifest,
	objects admissionReader,
) {
	t.Helper()
	c.objects = objects
	c.facts.(*solverFacts).reader = objects
	guard, _ := objects.(*store.ReadSet)
	_, err := evaluateSolver(
		t,
		c,
		&PrepareRequest{Owner: emitter, Generation: []store.Child{{Obj: child}}, Reads: guard},
	)
	assert.NoError(t, err)
}

func capturedRoles(c *admissionClosure) map[manifest.NamedResource][4]bool {
	roles := map[manifest.NamedResource][4]bool{}
	for _, bucket := range c.session.Snapshot().buckets {
		for id := range bucket {
			role := c.role(id)
			roles[id] = [4]bool{role.needed, role.selected, role.dirty, role.ordinary}
		}
	}
	for id := range c.states {
		if role := c.role(id); role.needed {
			roles[id] = [4]bool{role.needed, role.selected, role.dirty, role.ordinary}
		}
	}
	return roles
}

func TestAddEmitted_CapturedUnpublishedDeclaration(t *testing.T) {
	t.Parallel()
	emitter := testutil.KustomizationID("apps", "emitter")
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.GitRepository{Name: "input", Namespace: "apps"}
	child := &manifest.Kustomization{
		Name:      "child",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: target.Name},
	}
	proof := manifest.Production{
		Owner:    producer.Named(),
		Authored: target.Named(),
		Path:     "/repo/input.yaml",
		File:     "input.yaml",
		Source:   manifest.BootstrapSourceID,
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter:        {File: "emitter.yaml"},
		target.Named(): {Productions: []manifest.Production{proof}},
	}}
	st := store.New()
	st.AddObjects([]manifest.BaseManifest{producer, target})
	st.SetDiscoveryIndex(index)
	c := capturedClosure(t, st, index, emitter)
	var reads store.ReadSet
	reads.Init(st, emitter, nil)
	defer reads.Close()
	assert.Equal(t, reads.GetObject(target.Named()), manifest.BaseManifest(target))
	assert.Equal(t, reads.GetDeclaration(target.Named()), nil)
	reads.Productions(target.Named(), index)
	assert.Equal(t, reads.Valid(), true)
	st.RecordEmitted(
		producer.Named(),
		&store.Emission{
			Children: []store.Child{{Obj: target, Productions: []manifest.Production{proof}}},
			Complete: true,
		},
		nil,
	)
	assert.Equal(t, st.GetDeclaration(target.Named()), manifest.BaseManifest(target))
	c.evaluateCaptured(t, emitter, child, &reads)
	assert.Diff(
		t,
		capturedRoles(&c),
		map[manifest.NamedResource][4]bool{
			emitter:          {true, true, true, true},
			child.Named():    {true, true, true, true},
			target.Named():   {true, false, false, true},
			producer.Named(): {true, false, false, true},
		},
	)
	assert.Equal(t, reads.Valid(), false)

	fresh := capturedClosure(t, st, index, emitter)
	fresh.evaluateCaptured(t, emitter, child, st)
	assert.Diff(
		t,
		capturedRoles(&fresh),
		map[manifest.NamedResource][4]bool{
			emitter:        {true, true, true, true},
			child.Named():  {true, true, true, true},
			target.Named(): {true, false, false, true},
		},
	)
}

func TestAddEmitted_CapturedTransitiveReferences(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	dependency := &manifest.GitRepository{Name: "dependency", Namespace: "apps"}
	dependency.SecretRef = &manifest.LocalObjectReference{Name: "captured"}
	st := store.New()
	st.AddObject(dependency)
	c := capturedClosure(
		t,
		st,
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}},
		},
		parent,
	)
	var reads store.ReadSet
	reads.Init(st, parent, nil)
	defer reads.Retire()
	reads.GetObject(dependency.Named())
	changed := *dependency
	changed.SecretRef = &manifest.LocalObjectReference{Name: "newer"}
	st.AddObject(&changed)
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	child.SourceRef.Kind, child.SourceRef.Name = manifest.KindGitRepository, dependency.Name
	c.evaluateCaptured(t, parent, child, &reads)
	assert.Equal(
		t,
		c.role(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "captured"}).needed,
		true,
	)
	assert.Equal(
		t,
		c.role(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "newer"}).needed,
		false,
	)
	assert.Equal(t, reads.Valid(), false)
}

func TestAddEmitted_CapturedAbsentOrderingTarget(t *testing.T) {
	t.Parallel()
	emitter := testutil.KustomizationID("apps", "emitter")
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
	child := &manifest.Kustomization{
		Name:      "child",
		Namespace: "apps",
		DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}},
	}
	st := store.New()
	st.AddObject(producer)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		emitter:        {File: "emitter.yaml"},
		target.Named(): {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	c := capturedClosure(t, st, index, emitter)
	var reads store.ReadSet
	reads.Init(st, emitter, nil)
	defer reads.Close()
	assert.Equal(t, reads.GetObject(target.Named()), nil)
	assert.Equal(t, reads.Valid(), true)
	st.AddObject(target)
	assert.Equal(t, reads.GetObject(target.Named()), nil)
	c.evaluateCaptured(t, emitter, child, &reads)
	assert.Diff(
		t,
		capturedRoles(&c),
		map[manifest.NamedResource][4]bool{
			emitter:          {true, true, true, true},
			child.Named():    {true, true, true, true},
			target.Named():   {true, false, false, true},
			producer.Named(): {true, false, false, true},
		},
	)
	assert.Equal(t, reads.Valid(), false)

	fresh := capturedClosure(t, st, index, emitter)
	var current store.ReadSet
	current.Init(st, emitter, nil)
	defer current.Close()
	fresh.evaluateCaptured(t, emitter, child, &current)
	assert.Diff(
		t,
		capturedRoles(&fresh),
		map[manifest.NamedResource][4]bool{emitter: {true, true, true, true}, child.Named(): {true, true, true, true}},
	)
	assert.Equal(t, current.Valid(), true)
}
