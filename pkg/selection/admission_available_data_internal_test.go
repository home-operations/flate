package selection

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_AvailableInputDoesNotDemandMaterializers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		available    bool
		missingKey   bool
		allowMissing bool
	}{{name: "missing"}, {name: "allowed missing", allowMissing: true}, {name: "available", available: true}, {name: "missing requested key", available: true, missingKey: true}, {name: "missing requested key with permission", available: true, missingKey: true, allowMissing: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &manifest.ConfigMap{Name: "settings", Data: map[string]any{"values.yaml": "{}"}}
			if tc.missingKey {
				input.Data = map[string]any{"sibling": "readable"}
			}
			consumer := &manifest.HelmRelease{
				Name:      "consumer",
				Namespace: "apps",
				ValuesFrom: []manifest.ValuesReference{{
					Kind: manifest.KindConfigMap,
					Name: input.Name,
				}},
			}
			st.AddObject(consumer)
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}

			production := manifest.Production{Authored: input.Named(), Path: "settings.yaml", File: "settings.yaml"}
			record := manifest.DiscoveryRecord{Productions: []manifest.Production{production}}
			if tc.available {
				record.ReferenceData = []manifest.ReferenceData{{Production: production, OfflineData: input}}
			}
			facts.index.Records[input.Named()] = record
			c := solverClosure(t, st, facts)
			c.missing = &solverMissingPolicy{allowed: tc.allowMissing}
			c.add(consumer.Named(), admissionRole{needed: true})
			demands := 0
			assert.NoError(t, c.walk(t.Context(), func(demand materializerDemand, owner manifest.NamedResource) bool {
				demands++
				assert.Equal(t, demand.target, input.WithNamespace(consumer.Namespace).Named())
				assert.Equal(t, owner, consumer.Named())
				assert.Equal(t, demand.required, !tc.allowMissing || tc.missingKey)
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
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}

			policy := producer.EmissionPolicy()
			occurrence := OccurrenceFacts{
				Policy: &policy, Identity: producer.Named(), Authored: producer.Named(), Object: producer,
				Catalog: producer, Located: true, CatalogKnown: true, Uncertain: true, NeedsCandidates: true,
			}
			facts.index.Records[producer.Named()] = manifest.DiscoveryRecord{}
			facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{producer.Named(): occurrence}
			facts.producers = map[manifest.BaseManifest]ProducerFacts{
				producer: {Declaration: producer, Occurrence: occurrence},
			}
			facts.roots = map[manifest.NamedResource]RootEvidence{producer.Named(): {
				Identity: producer.Named(), Scope: RootScopeFacts{Candidate: true, NamespaceEligible: true},
			}}
			c := solverClosure(t, st, facts)
			pending := c.materializers(
				materializerDemand{target: input.Named(), required: true},
				consumer,
			)
			assert.Equal(t, pending, !tc.available)
			assert.Equal(t, c.role(producer.Named()).needed, !tc.available)
		})
	}
}

func TestAdmission_ResolvedInputDoesNotCaptureProducerSettlement(t *testing.T) {
	t.Parallel()
	for _, indexed := range []bool{false, true} {
		name := "present"
		if indexed {
			name = "indexed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			st.AddObject(producer)
			input := &manifest.ConfigMap{Name: "settings", Namespace: "apps"}
			children := []store.Child{{Obj: input}}
			st.RecordEmitted(producer.Named(), &store.Emission{Children: children}, nil)
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}
			if indexed {
				facts.index.Records[input.Named()] = manifest.DiscoveryRecord{
					Productions: []manifest.Production{{Owner: producer.Named()}},
				}
			}
			consumer := (&manifest.HelmRelease{Name: "consumer", Namespace: "apps"}).Named()
			var reads store.ReadSet
			reads.Init(st, consumer, nil)
			defer reads.Close()
			c := solverClosure(t, st, facts)
			c.objects = &reads
			c.materializeReferenceInput(consumer, new(manifest.Reference{
				NamedResource: input.Named(), Key: "values.yaml", Use: "valuesFrom",
			}), false, func(materializerDemand, manifest.NamedResource) bool {
				t.Fatal("resolved input demanded a materializer")
				return false
			})
			st.RecordEmitted(producer.Named(), &store.Emission{Children: children, Complete: true}, nil)
			assert.Equal(t, reads.Valid(), true)
		})
	}
}
