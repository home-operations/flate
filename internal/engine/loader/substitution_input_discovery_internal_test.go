package loader

import (
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSubstitutionResolution_InputDiscovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, inputName     string
		encrypted, optional bool
		unresolved          bool
	}{
		{name: "absent", inputName: "vars"},
		{name: "optional absent", inputName: "vars", optional: true},
		{name: "malformed unnamed"},
		{name: "encrypted", inputName: "vars", encrypted: true},
		{name: "absent after unresolved name", inputName: "vars", unresolved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			owner.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
				{Kind: manifest.KindSecret, Name: tc.inputName, Optional: tc.optional},
			}}
			reader := &substitutionDataReader{
				inputs: map[manifest.NamedResource]store.ReferenceFacts{}, lookups: map[manifest.NamedResource]int{},
			}
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: owner.Namespace, Name: tc.inputName}
			var readOrder, wantOrder []manifest.NamedResource
			record := manifest.DiscoveryRecord{Declaration: owner}
			if tc.unresolved {
				ancestor := &manifest.Kustomization{Name: "ancestor", Namespace: owner.Namespace}
				ancestor.PostBuild = &kustomizev1.PostBuild{
					Substitute: map[string]string{"KNOWN_SETTINGS": "known-settings"},
					SubstituteFrom: []kustomizev1.SubstituteReference{
						{Kind: manifest.KindSecret, Name: "parent-settings"},
					},
				}
				l.Index.Records[ancestor.Named()] = manifest.DiscoveryRecord{Declaration: ancestor}
				record.Parent = ancestor.Named()
				literal := kustomizev1.SubstituteReference{Kind: manifest.KindConfigMap, Name: "literal-settings"}
				owner.PostBuild.SubstituteFrom = slices.Insert(owner.PostBuild.SubstituteFrom, 0,
					literal,
					kustomizev1.SubstituteReference{Kind: manifest.KindConfigMap, Name: "${SETTINGS}"},
					literal,
					kustomizev1.SubstituteReference{Kind: manifest.KindConfigMap, Name: "${KNOWN_SETTINGS}"},
				)
				source := manifest.NamedResource{
					Kind: manifest.KindSecret, Namespace: owner.Namespace, Name: "parent-settings",
				}
				reader.inputs[source] = store.ReferenceFacts{Generated: true}
				first := manifest.NamedResource{Kind: literal.Kind, Namespace: owner.Namespace, Name: literal.Name}
				resolved := manifest.NamedResource{
					Kind: manifest.KindConfigMap, Namespace: owner.Namespace, Name: "known-settings",
				}
				for _, inputID := range []manifest.NamedResource{first, resolved} {
					input := &substitutionData{
						Data: &manifest.ConfigMap{
							Name:      inputID.Name,
							Namespace: inputID.Namespace,
							Data:      map[string]any{"OTHER": inputID.Name},
						},
						onRead: func() { readOrder = append(readOrder, inputID) },
					}
					reader.inputs[inputID] = store.ReferenceFacts{Data: input, Original: input, Declared: true}
				}
				wantOrder = []manifest.NamedResource{first, first, resolved}
			}
			l.Index.Records[owner.Named()] = record
			if tc.encrypted {
				data := &manifest.Secret{
					Name:       id.Name,
					Namespace:  id.Namespace,
					Data:       nil,
					StringData: map[string]any{"VALUE": "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"},
				}
				reader.inputs[id] = store.ReferenceFacts{Data: data, Original: data, Declared: true}
			}
			resolution := substitutionResolution{
				loader:   l,
				provider: values.NewStoreProvider(reader, manifest.NamedResource{}),
			}
			_, known := resolution.resolveValue(t.Context(), owner.Named(), "${VALUE}")
			assert.Equal(t, known, false)
			assert.Equal(t, resolution.needsInputs, !tc.encrypted)
			assert.Equal(t, reader.lookups[id], 1)
			if tc.unresolved {
				assert.Diff(t, readOrder, wantOrder)
				unresolved := manifest.NamedResource{
					Kind: manifest.KindConfigMap, Namespace: owner.Namespace, Name: "${SETTINGS}",
				}
				assert.Equal(t, reader.lookups[unresolved], 0)
				assert.Equal(t, resolution.catalogs[owner.Named()].PostBuild.SubstituteFrom[1].Name, unresolved.Name)
			}
		})
	}
}
