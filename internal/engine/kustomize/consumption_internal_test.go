package kustomize

import (
	"errors"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestShape_AllDocumentFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		firstKey   string
		firstValue string
		unreadable bool
		following  []string
	}{
		{
			name: "mixed failures", firstKey: "${TOKEN}", firstValue: "value", unreadable: true,
			following: []string{"${READABLE}", "${GOOD}"},
		},
		{
			name: "readable failures", firstKey: "value", firstValue: "${",
			following: []string{"${READABLE}", "${GOOD}"},
		},
		{
			name: "unreadable only", firstKey: "${TOKEN}", firstValue: "value", unreadable: true,
			following: []string{"${GOOD}"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "owner", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
				Substitute:     map[string]string{"READABLE": "[", "GOOD": "substituted"},
				SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindSecret, Name: "input"}},
			}}
			secret := &manifest.Secret{
				Name: "input", Namespace: "apps",
				StringData: map[string]any{"TOKEN": "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"},
			}
			var inputs []values.Result
			for ref := range manifest.References(ks, manifest.ReferenceRelations) {
				inputs = append(inputs, values.Resolve(&values.ResolveRequest{
					Reference: ref, Facts: store.ReferenceFacts{Declared: true, Data: secret},
				}))
			}
			apply := func(following []string) ([]manifest.DocOccurrence, error) {
				t.Helper()
				prepared, err := Prepare(ks, inputs)
				assert.NoError(t, err)
				docs := make([]manifest.DocOccurrence, 1, 1+len(following))
				docs[0] = manifest.DocOccurrence{Ordinal: 7, Document: map[string]any{
					"data": map[string]any{tc.firstKey: tc.firstValue},
				}}
				for i, expression := range following {
					docs = append(docs, manifest.DocOccurrence{Ordinal: 11 + 4*i, Document: map[string]any{
						"data": map[string]any{"value": expression},
					}})
				}
				base.PrepareDocuments(prepared, docs)
				shape := PrepareShape(&RenderRequest{
					Spec: prepared, SubstitutionActive: values.ResolvedSubstitutionActive(inputs),
					UnreadableSubstitutions: values.UnreadableSubstitutions(ks, inputs),
				}, docs)
				return docs, shape.Apply(t.Context(), ShapeOptions{})
			}
			_, firstErr := apply(nil)
			assert.FatalIf(t, firstErr == nil, "first document must fail substitution")
			var runs [2][]string
			for run := range runs {
				docs, err := apply(tc.following)
				assert.FatalIf(t, err == nil, "render must fail substitution")
				assert.Equal(t, err, docs[0].Err)
				assert.Equal(t, errors.Is(err, manifest.ErrInvalidSubstituteReference), true)
				assert.Equal(t, err.Error(), firstErr.Error())
				runs[run] = make([]string, len(docs)-1)
				for i, doc := range docs[:len(docs)-1] {
					assert.FatalIf(t, doc.Err == nil, "document %d must fail substitution", i)
					assert.Equal(t, errors.Is(doc.Err, manifest.ErrInvalidSubstituteReference), true)
					runs[run][i] = doc.Err.Error()
					assert.Equal(t, doc.Ordinal, 7+4*i)
					assert.Equal(t, len(doc.ConsumedUnreadable) != 0, i == 0 && tc.unreadable)
				}
				last := docs[len(docs)-1]
				assert.Equal(t, last.Ordinal, 7+4*(len(docs)-1))
				assert.NoError(t, last.Err)
				assert.Equal(t, len(last.ConsumedUnreadable), 0)
				assert.Diff(t, last.Document, map[string]any{
					"data": map[string]any{"value": "substituted"},
					"metadata": map[string]any{"labels": map[string]any{
						"kustomize.toolkit.fluxcd.io/name":      "owner",
						"kustomize.toolkit.fluxcd.io/namespace": "apps",
					}},
				})
			}
			assert.Diff(t, runs[0], runs[1])
		})
	}
}

func TestShape_ConsumedUnreadable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		expression string
		inline     map[string]string
		disabled   bool
		strict     bool
		produced   bool
		consumed   bool
		failed     bool
	}{
		{name: "known empty", expression: "${TOKEN}", consumed: true},
		{name: "strict known empty", expression: "${TOKEN}", strict: true, consumed: true},
		{name: "strict known produced empty", expression: "${TOKEN}", strict: true, produced: true, consumed: true},
		{name: "default", expression: "${TOKEN:-fallback}", consumed: true},
		{name: "escaped", expression: "$${TOKEN}"},
		{name: "unused", expression: "plain"},
		{name: "disabled", expression: "${TOKEN}", disabled: true},
		{name: "inline override", expression: "${TOKEN}", inline: map[string]string{"TOKEN": "readable"}},
		{name: "strict undefined", expression: "${TOKEN}${UNKNOWN}", strict: true, failed: true},
		{name: "unproved syntax", expression: "${TOKEN}${", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "owner", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
				Substitute:     tc.inline,
				SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindSecret, Name: "input"}},
			}}
			secret := &manifest.Secret{
				Name:       "input",
				Namespace:  "apps",
				StringData: map[string]any{"TOKEN": "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"},
			}
			var inputs []values.Result
			for ref := range manifest.References(ks, manifest.ReferenceRelations) {
				facts := store.ReferenceFacts{Declared: true, Data: secret}
				if tc.produced {
					facts = store.ReferenceFacts{
						Generated: true,
						Keys:      manifest.KeySet{Names: []string{"TOKEN"}, Complete: true},
					}
				}
				inputs = append(inputs, values.Resolve(&values.ResolveRequest{Reference: ref, Facts: facts}))
			}
			unreadable := values.UnreadableSubstitutions(ks, inputs)
			prepared, err := Prepare(ks, inputs)
			assert.NoError(t, err)
			metadata := map[string]any{"name": "output"}
			if tc.disabled {
				metadata["annotations"] = map[string]any{manifest.SubstituteAnnotationKey: "disabled"}
			}
			docs := []manifest.DocOccurrence{{Ordinal: 7, Document: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   metadata,
				"data":       map[string]any{"value": tc.expression},
			}}}
			shape := PrepareShape(
				&RenderRequest{
					Spec:                    prepared,
					SubstitutionActive:      values.ResolvedSubstitutionActive(inputs),
					UnreadableSubstitutions: unreadable,
				},
				docs,
			)
			err = shape.Apply(t.Context(), ShapeOptions{Strict: tc.strict})
			assert.Equal(t, err != nil, tc.failed)
			assert.Equal(t, len(docs[0].ConsumedUnreadable) != 0, tc.consumed)
			assert.Equal(t, docs[0].Ordinal, 7)
			if tc.consumed && len(docs[0].ConsumedUnreadable) != 0 {
				assert.Equal(t, docs[0].ConsumedUnreadable[0].NamedResource, secret.Named())
				assert.Equal(t, docs[0].ConsumedUnreadable[0].Key, "TOKEN")
			}
		})
	}
}

func TestShape_UnknownNamesActivate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		strict     bool
		expression string
		want       any
		failed     bool
	}{
		{name: "empty", expression: "${UNKNOWN}"},
		{name: "default", expression: "${UNKNOWN:-fallback}", want: "fallback"},
		{name: "strict used", expression: "${UNKNOWN}", strict: true, failed: true},
		{name: "strict default", expression: "${UNKNOWN:-fallback}", strict: true, want: "fallback"},
		{name: "strict unused", expression: "plain", strict: true, want: "plain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "owner", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
				SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindSecret, Name: "generated"}},
			}}
			var inputs []values.Result
			for ref := range manifest.References(ks, manifest.ReferenceRelations) {
				inputs = append(
					inputs,
					values.Resolve(
						&values.ResolveRequest{Reference: ref, Facts: store.ReferenceFacts{Generated: true}},
					),
				)
			}
			prepared, err := Prepare(ks, inputs)
			assert.NoError(t, err)
			assert.Equal(t, len(prepared.PostBuild.Substitute), 0)
			assert.Equal(t, values.ResolvedSubstitutionActive(inputs), true)
			docs := []manifest.DocOccurrence{{Document: map[string]any{"value": tc.expression}}}
			shape := PrepareShape(
				&RenderRequest{Spec: prepared, SubstitutionActive: values.ResolvedSubstitutionActive(inputs)},
				docs,
			)
			err = shape.Apply(t.Context(), ShapeOptions{Strict: tc.strict})
			assert.Equal(t, err != nil, tc.failed)
			if err == nil {
				assert.Diff(t, docs[0].Document["value"], tc.want)
			}
			assert.Equal(t, len(docs[0].ConsumedUnreadable), 0)
		})
	}
}
