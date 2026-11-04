package loader

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_AvailableInputDoesNotDemandMaterializers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		available bool
	}{{name: "missing"}, {name: "available", available: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &manifest.ConfigMap{Name: "settings"}
			consumer := &manifest.HelmRelease{
				Name:      "consumer",
				Namespace: "apps",
				ValuesFrom: []manifest.ValuesReference{{
					Kind: manifest.KindConfigMap,
					Name: input.Name,
				}},
			}
			st.AddObject(consumer)
			l := New(st)
			l.Options.Filter = change.NewFilter()
			production := manifest.Production{Authored: input.Named(), Path: "settings.yaml", File: "settings.yaml"}
			record := manifest.DiscoveryRecord{Productions: []manifest.Production{production}}
			if tc.available {
				record.ReferenceData = []manifest.ReferenceData{{Production: production, OfflineData: input}}
			}
			l.Index.Records[input.Named()] = record
			c := admissionClosure{loader: l, objects: st, states: map[manifest.NamedResource]admissionState{}}
			c.add(consumer.Named(), admissionRole{needed: true})
			demands := 0
			assert.NoError(t, c.walk(t.Context(), func(demand materializerDemand, owner manifest.NamedResource) bool {
				demands++
				assert.Equal(t, demand.target, input.WithNamespace(consumer.Namespace).Named())
				assert.Equal(t, owner, consumer.Named())
				assert.Equal(t, demand.required, true)
				return false
			}))
			want := 1
			if tc.available {
				want = 0
			}
			assert.Equal(t, demands, want)
		})
	}
}

func TestAdmission_MaterializersSkipAvailableData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		available bool
	}{{name: "missing"}, {name: "available", available: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &manifest.ConfigMap{Name: "settings", Namespace: "apps"}
			if tc.available {
				st.AddDeclaration(store.Declaration{Obj: input, Origin: manifest.SuppliedDeclaration})
			}
			consumer := (&manifest.HelmRelease{Name: "consumer", Namespace: "apps"}).Named()
			producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
			l := New(st)
			l.Options.Filter = change.NewFilter()
			l.locations = map[manifest.NamedResource]Occurrence{producer.Named(): compactOccurrence(producer,
				manifest.ProjectProducer(producer))}
			c := admissionClosure{loader: l, objects: st, states: map[manifest.NamedResource]admissionState{}}
			pending := c.materializers(
				&materializerDemand{target: input.Named(), required: true},
				consumer,
				c.materializerCandidates(false),
			)
			assert.Equal(t, pending, !tc.available)
			assert.Equal(t, c.role(producer.Named()).needed, !tc.available)
		})
	}
}
