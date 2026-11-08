package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

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
	assert.Equal(t, f.role(target.Named()), kept)
	var reads store.ReadSet
	reads.Init(st, producer.Named(), nil)
	defer reads.Close()
	child := store.Child{Obj: target, Productions: []manifest.Production{proof}}
	f.AddEmitted(producer.Named(), new(child), &reads)
	assert.Equal(t, reads.Valid(), true)
	st.RecordEmitted(producer.Named(), &store.Emission{Children: []store.Child{child}, Complete: true}, nil)
	assert.Equal(t, reads.Valid(), true)
}
