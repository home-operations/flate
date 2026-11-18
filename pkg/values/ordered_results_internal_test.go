package values

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestExpandPostBuildSubstituteReference_UnreadablePrecedence(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
		SubstituteFrom: []kustomizev1.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "first"},
			{Kind: manifest.KindSecret, Name: "later"},
			{Kind: manifest.KindSecret, Name: "generated"},
		},
		Substitute: map[string]string{"INLINE": "fixed"},
	}}
	first := &manifest.ConfigMap{
		Name:      "first",
		Namespace: "apps",
		Data:      map[string]any{"VALUE": "earlier", "INLINE": "earlier"},
	}
	later := &manifest.Secret{
		Name:       "later",
		Namespace:  "apps",
		Data:       nil,
		StringData: map[string]any{"VALUE": cipher, "INLINE": cipher, "READABLE": "retained"},
	}
	canonical := later.Snapshot()
	var inputs []Result
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		facts := store.ReferenceFacts{Generated: true, Keys: manifest.KeySet{Names: []string{"PRODUCED"}}}
		switch ref.Name {
		case "first":
			facts = store.ReferenceFacts{Data: first, Original: first}
		case "later":
			facts = store.ReferenceFacts{Data: later, Original: later}
		}
		inputs = append(inputs, Resolve(&ResolveRequest{Reference: ref, Facts: facts}))
	}
	unreadable := UnreadableSubstitutions(ks, inputs)
	assert.Equal(t, len(unreadable), 2)
	assert.Equal(t, unreadable["VALUE"].NamedResource, later.Named())
	assert.Equal(t, unreadable["VALUE"].Position, 1)
	assert.Equal(t, unreadable["VALUE"].Key, "VALUE")
	assert.Equal(t, unreadable["PRODUCED"].Position, 2)
	assert.NoError(t, ExpandPostBuildSubstituteReference(ks, inputs))
	assert.Diff(
		t,
		ks.PostBuild.Substitute,
		map[string]string{"VALUE": "", "INLINE": "fixed", "READABLE": "retained", "PRODUCED": ""},
	)
	assert.Diff(t, later.Snapshot(), canonical)
	assert.Equal(t, len(inputs[1].warnings), 0)
	assert.Equal(t, len(inputs[1].diagnostics), 0)
}

func TestExpandValueReferences_OrderedUnreadableOmissions(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{
		{Kind: manifest.KindConfigMap, Name: "first"},
		{Kind: manifest.KindSecret, Name: "encrypted"},
		{Kind: manifest.KindConfigMap, Name: "later", ValuesKey: "value", TargetPath: "image.tag"},
		{Kind: manifest.KindSecret, Name: "generated"},
	}, Values: map[string]any{"inline": "kept"}}
	var inputs []Result
	for ref := range manifest.References(hr, manifest.ReferenceRelations) {
		if ref.Use != "valuesFrom" {
			continue
		}
		facts := store.ReferenceFacts{Generated: true}
		switch ref.Name {
		case "first":
			facts.Data = &manifest.ConfigMap{
				Name:      ref.NamedResource.Name,
				Namespace: ref.NamedResource.Namespace,
				Data:      map[string]any{"values.yaml": "image:\n  tag: first\n"},
			}
			facts.Original = facts.Data
		case "encrypted":
			facts.Data = &manifest.Secret{
				Name:       ref.NamedResource.Name,
				Namespace:  ref.NamedResource.Namespace,
				Data:       nil,
				StringData: map[string]any{"values.yaml": cipher},
			}
			facts.Original = facts.Data
		case "later":
			facts.Data = &manifest.ConfigMap{
				Name:      ref.NamedResource.Name,
				Namespace: ref.NamedResource.Namespace,
				Data:      map[string]any{"value": "later"},
			}
			facts.Original = facts.Data
		}
		inputs = append(inputs, Resolve(&ResolveRequest{Reference: ref, Facts: facts}))
	}
	assert.NoError(t, ExpandValueReferences(hr, inputs, &Cache{}))
	assert.Diff(t, hr.Values, map[string]any{"image": map[string]any{"tag": "later"}, "inline": "kept"})
	assert.Equal(t, len(hr.ValuesFrom), 2)
	assert.Equal(t, hr.ValuesFrom[0].Name, "first")
	assert.Equal(t, hr.ValuesFrom[1].Name, "later")
	assert.Diff(
		t,
		OmittedIDs(inputs),
		[]manifest.NamedResource{
			{Kind: manifest.KindSecret, Namespace: "apps", Name: "encrypted"},
			{Kind: manifest.KindSecret, Namespace: "apps", Name: "generated"},
		},
	)
	for i := range inputs {
		assert.Equal(t, len(inputs[i].diagnostics), 0)
	}
}

func TestExpandValueReferences_RejectsUnsettledAssessment(t *testing.T) {
	t.Parallel()
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "zero", true: "deferred"}[deferred], func(t *testing.T) {
			t.Parallel()
			hr := &manifest.HelmRelease{
				Name:       "consumer",
				Namespace:  "apps",
				ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "input"}},
				Values:     map[string]any{"inline": "unchanged"},
			}
			before := hr.Clone()
			var result Result
			if deferred {
				for ref := range manifest.References(hr, manifest.ReferenceRelations) {
					if ref.Use == "valuesFrom" {
						result = Resolve(
							&ResolveRequest{
								Reference: ref,
								Facts:     store.ReferenceFacts{Generated: true},
								Settlement: Settlement{
									Pending: []manifest.NamedResource{
										{Kind: manifest.KindKustomization, Name: "producer"},
									},
								},
							},
						)
					}
				}
			}
			assert.Equal(t, ExpandValueReferences(hr, []Result{result}, nil) != nil, true)
			assert.Equal(t, manifest.Equal(hr, before), true)
		})
	}
}

func TestExpansion_ReturnsCapturedFailure(t *testing.T) {
	t.Parallel()
	failure := manifest.NewError(manifest.ErrInvalidValuesReference, nil, "captured material failure")
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
		SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindSecret, Name: "input"}},
		Substitute:     map[string]string{"INLINE": "unchanged"},
	}}
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{
		{Kind: manifest.KindSecret, Name: "input"},
	}, Values: map[string]any{"inline": "unchanged"}}
	beforeKS, beforeHR := ks.Clone(), hr.Clone()
	var substitution, helm []Result
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		substitution = append(substitution, Resolve(&ResolveRequest{Reference: ref, Failure: failure}))
	}
	for ref := range manifest.References(hr, manifest.ReferenceRelations) {
		if ref.Use == "valuesFrom" {
			helm = append(helm, Resolve(&ResolveRequest{Reference: ref, Failure: failure}))
		}
	}
	assert.Equal(t, ExpandPostBuildSubstituteReference(ks, substitution), error(failure))
	assert.Equal(t, ExpandValueReferences(hr, helm, nil), error(failure))
	assert.Equal(t, manifest.Equal(ks, beforeKS), true)
	assert.Equal(t, manifest.Equal(hr, beforeHR), true)
}
