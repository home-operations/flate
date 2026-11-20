package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCollectManifests_GeneratedOverlap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		static bool
		scoped bool
	}{
		{name: "three producers"},
		{name: "static authority", static: true},
		{name: "excluded winner", scoped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			generator := &manifest.RawObject{
				APIVersion: manifest.FluxOperatorDomain + "/v1",
				Kind:       "FluxInstance",
				Name:       "instance",
				Namespace:  "flux-system",
			}
			generated := &manifest.Kustomization{Name: "sync", Namespace: "flux-system"}
			st.AddObject(generator)
			st.RecordEmitted(generator.Named(), &store.Emission{
				Children: []store.Child{{Obj: generated, Generated: true, VisibleGeneration: true}},
				Complete: true,
			}, nil)

			var authored manifest.BaseManifest = &manifest.Kustomization{Name: "authored", Namespace: "apps"}
			root := testutil.KustomizationID("", "")
			if tc.scoped {
				authored = manifest.NewEntryKustomization(manifest.BootstrapSourceID, "apps", manifest.NamedResource{})
				root.Namespace = "apps"
			}
			st.AddObject(authored)
			producers := []manifest.BaseManifest{authored, generated}
			if !tc.scoped {
				other := &manifest.Kustomization{Name: "z-authored", Namespace: "flux-system"}
				st.AddObject(other)
				producers = append(producers, other)
			}

			shared := &manifest.ConfigMap{Name: "shared", Namespace: "apps"}
			unrelated := testutil.ObjectDocument(&manifest.ConfigMap{Name: "unrelated", Namespace: "apps"})
			documents := make(map[manifest.NamedResource][]map[string]any)
			ownerKeys := generated.EmissionPolicy().OwnerKeys
			for _, producer := range producers {
				id := producer.Named()
				doc := map[string]any{
					"apiVersion": "v1",
					"kind":       manifest.KindConfigMap,
					"metadata": map[string]any{
						"name": shared.Name, "namespace": shared.Namespace,
						"labels": map[string]any{ownerKeys[0]: "same-owner", ownerKeys[1]: "apps"},
					},
					"data": map[string]any{"value": id.Name},
				}
				docs := []map[string]any{doc}
				if producer == authored {
					docs = append(docs, unrelated)
				}
				parsed := parseChildrenForTest(id, docs, manifest.ParseDocOptions{})
				documents[id] = parsed.Documents
				st.RecordEmitted(id, captureEmission(st, id, &store.Emission{
					Children:  parsed.Children,
					Manifests: testutil.DocumentOccurrences(parsed.Documents),
					Complete:  true,
				}), nil)
			}

			winner := producers[len(producers)-1].Named()
			want := map[manifest.NamedResource][]map[string]any{
				authored.Named(): {unrelated},
				winner:           documents[winner],
			}
			winnerLabel := winner.String()
			overlappingProducers := len(producers)
			switch {
			case tc.scoped:
				want = map[manifest.NamedResource][]map[string]any{authored.Named(): documents[authored.Named()]}
			case tc.static:
				st.AddObject(shared)
				assert.Equal(t, st.GetObject(shared.Named()), manifest.BaseManifest(shared))
				winner = manifest.NamedResource{}
				winnerLabel = "static object"
				want = documents
				overlappingProducers = 0
			}
			assert.Equal(t, st.EmissionOwner(shared.Named()), winner)
			assessment := st.AssessProducers()
			assert.Equal(t, len(assessment.GeneratedOverlaps), overlappingProducers)
			assert.FatalIf(t, len(assessment.Warnings) != 1, "warnings: %v", assessment.Warnings)
			warning := assessment.Warnings[0]
			assert.Equal(t, warning.Category, manifest.WarnConflictingProducers)
			assert.Equal(t, warning.Resource, shared.Named())
			assert.Equal(t, warning.Count, 1)
			assert.Equal(t, warning.Message, "conflicting producers; using "+winnerLabel)

			o := &Orchestrator{store: st}
			_, emissions := o.collectCompletedManifests(assessment.GeneratedOverlaps)
			projected, errs := selection.Project(
				[]selection.Snapshot{{Store: st, Complete: true, Emissions: emissions}},
				nil,
				selection.Request{Roots: []manifest.NamedResource{root}},
			)
			assert.NoError(t, errs.Selected)
			assert.NoError(t, errs.Remaining)
			assert.Diff(t, outputByParent(projected[0].Manifests), want)
		})
	}
}
