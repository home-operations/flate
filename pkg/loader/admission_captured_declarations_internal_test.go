package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"},
	)
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
	f.AddEmitted(emitter, new(store.Child{Obj: child}), &reads)
	assert.Diff(
		t,
		f.keep,
		map[manifest.NamedResource]role{
			emitter:          primary,
			child.Named():    primary,
			target.Named():   kept,
			producer.Named(): kept,
		},
	)
	assert.Equal(t, reads.Valid(), false)

	fresh := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"emitter.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	fresh.AddEmitted(emitter, new(store.Child{Obj: child}), st)
	assert.Diff(
		t,
		fresh.keep,
		map[manifest.NamedResource]role{emitter: primary, child.Named(): primary, target.Named(): kept},
	)
}

func TestAddEmitted_DoesNotReadIncomingDeclaration(t *testing.T) {
	t.Parallel()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.GitRepository{Name: "input", Namespace: "apps"}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
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
		consumer.Named(): {File: "consumer.yaml"},
		target.Named():   {Productions: []manifest.Production{proof}},
	}}
	st := store.New()
	st.AddObjects([]manifest.BaseManifest{producer, consumer})
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	assert.Equal(t, f.keep[target.Named()], kept)
	var reads store.ReadSet
	reads.Init(st, producer.Named(), nil)
	defer reads.Close()
	child := store.Child{Obj: target, Productions: []manifest.Production{proof}}
	f.AddEmitted(producer.Named(), new(child), &reads)
	assert.Equal(t, reads.Valid(), true)
	st.RecordEmitted(producer.Named(), &store.Emission{Children: []store.Child{child}, Complete: true}, nil)
	assert.Equal(t, reads.Valid(), true)
}

func (c *countedProductions) fixtureStore() *store.Store { return c.Store }
func (p *pausedIncoming) fixtureStore() *store.Store     { return p.Store }
