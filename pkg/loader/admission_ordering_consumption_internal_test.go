package loader

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAddEmitted_RetainedInputsRemainUnconsumed(t *testing.T) {
	t.Parallel()
	st := store.New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	target := &manifest.ConfigMap{Name: "values", Namespace: "apps"}
	consumer := &manifest.HelmRelease{
		Name:       "consumer",
		Namespace:  "apps",
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: target.Name}},
	}
	st.AddObjects([]manifest.BaseManifest{producer, consumer})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		target.Named():   {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"consumer.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	assert.Equal(t, f.Reconcile(producer.Named()), true)
	var reads store.ReadSet
	reads.Init(st, consumer.Named(), nil)
	defer reads.Close()
	f.AddEmitted(manifest.NamedResource{}, new(store.Child{Obj: consumer}), &reads)
	_, _, next := st.GetEmission(producer.Named())
	defer next.Finish()
	next.Children = []store.Child{{Obj: target}}
	next.Complete = true
	st.RecordEmitted(producer.Named(), &next, nil)
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, reads.GetObject(target.Named()), manifest.BaseManifest(target))
}

func TestFilter_ProducedOrderingTarget(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"kustomization", "helmrelease", "emitted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			ancestor := testutil.KustomizationID("apps", "ancestor")
			declaration := testutil.KustomizationID("apps", "raw")
			target := testutil.KustomizationID("apps", "generated-raw")
			var consumer manifest.BaseManifest = &manifest.Kustomization{Name: "consumer", Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}}}
			if mode == "helmrelease" {
				target = testutil.HelmReleaseID("apps", "generated-raw")
				consumer = &manifest.HelmRelease{
					Name:      "consumer",
					Namespace: "apps",
					DependsOn: []helmv2.DependencyReference{{Name: target.Name}},
				}
			}
			sibling := testutil.KustomizationID("apps", "sibling")
			emitter := testutil.KustomizationID("apps", "emitter")
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Named(): {File: "consumer.yaml"},
				emitter:          {File: "emitter.yaml"},
				producer.Named(): {File: "producer.yaml", Parent: ancestor},
				target: {
					Productions: []manifest.Production{
						{Owner: producer.Named(), Declaration: declaration, Authored: declaration},
					},
				},
				sibling: {Productions: []manifest.Production{{Owner: producer.Named()}}},
			}}
			st := store.New()
			st.AddObject(producer)
			changed := "consumer.yaml"
			if mode == "emitted" {
				st.AddObject(&manifest.Kustomization{Name: emitter.Name, Namespace: emitter.Namespace})
				changed = "emitter.yaml"
			} else {
				st.AddObject(consumer)
			}
			f := newAdmission(
				t,
				admissionConfig{changes: change.NewSet([]string{changed}), index: index, inventory: st, root: "/repo"},
			)
			want := map[manifest.NamedResource]role{
				consumer.Named(): primary,
				target:           kept,
				producer.Named(): kept,
				declaration:      kept,
				ancestor:         kept,
			}
			if mode == "emitted" {
				assert.Equal(t, f.Reconcile(consumer.Named()), false)
				var reads store.ReadSet
				reads.Init(st, emitter, nil)
				defer reads.Close()
				added := f.AddEmitted(emitter, new(store.Child{Obj: consumer}), &reads)
				assert.Equal(t, slices.IsSortedFunc(added, manifest.NamedResource.Compare), true)
				assert.Equal(t, len(added), len(want))
				assert.Equal(t, reads.Valid(), true)
				want[emitter] = primary
			}
			assert.Diff(t, f.roles(), want)
			assert.Equal(t, f.Reconcile(sibling), false)
			assert.Equal(t, st.GetObject(target), nil)
		})
	}
}

func TestAddEmitted_OrderingDoesNotSelectConsumer(t *testing.T) {
	t.Parallel()
	target := &manifest.Kustomization{Name: "target", Namespace: "apps"}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		DependsOn: []kustomizev1.DependencyReference{{Name: target.Name}},
	}
	st := store.New()
	st.AddObject(target)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target.Named(): {File: "target.yaml"}},
	}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"target.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	f.AddEmitted(testutil.KustomizationID("apps", "unchanged"), new(store.Child{Obj: consumer}), st)
	assert.Diff(t, f.roles(), map[manifest.NamedResource]role{target.Named(): primary})
}
