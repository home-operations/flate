package selection

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestResult_WarningsFor(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "config"}
	foreign := id
	foreign.Group = "example.test"
	otherNamespace, otherName, otherKind := id, id, id
	otherNamespace.Namespace, otherName.Name, otherKind.Kind = "other", "other", manifest.KindSecret
	object := manifest.Warning{
		Resource: id, Subject: manifest.DiagnosticFor(id, nil),
		Category: manifest.WarnSOPSUnreadable, Message: "encrypted keys", Detail: []string{"a", "b"}, Count: 1,
	}
	consumer := manifest.Warning{
		Resource: manifest.NamedResource{Kind: manifest.KindKustomization, Name: "consumer"},
		Related:  []manifest.NamedResource{id}, Category: manifest.WarnUnresolvedSubstitution,
		Message: "encrypted input", Detail: []string{"a", "b"}, Count: 2,
	}
	foreignWarning := object
	foreignWarning.Resource = foreign
	warnings := []manifest.Warning{consumer, object, foreignWarning, consumer}
	for _, tc := range []struct {
		name   string
		output []manifest.NamedResource
		want   []manifest.Warning
	}{
		{name: "empty", want: []manifest.Warning{consumer, consumer}},
		{name: "core identity", output: []manifest.NamedResource{id}, want: []manifest.Warning{consumer, object, consumer}},
		{name: "API group", output: []manifest.NamedResource{foreign}, want: []manifest.Warning{consumer, foreignWarning, consumer}},
		{name: "namespace", output: []manifest.NamedResource{otherNamespace}, want: []manifest.Warning{consumer, consumer}},
		{name: "name", output: []manifest.NamedResource{otherName}, want: []manifest.Warning{consumer, consumer}},
		{name: "kind", output: []manifest.NamedResource{otherKind}, want: []manifest.Warning{consumer, consumer}},
		{name: "all", output: []manifest.NamedResource{id, foreign}, want: warnings},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backing := slices.Clone(warnings)
			result := &Result{Warnings: backing}
			got := result.WarningsFor(tc.output)
			assert.Diff(t, got, tc.want)
			assert.Diff(t, result.Warnings, warnings)
			if len(got) == len(warnings) {
				assert.ErrorIf(t, &got[0] != &backing[0], "unfiltered warnings must retain the borrowed slice")
			} else {
				got[0].Message = "changed copy"
				assert.Diff(t, result.Warnings, warnings)
			}
		})
	}
}

func TestResult_WarningsForPreservesStoredEvidence(t *testing.T) {
	t.Parallel()
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "encrypted"}
	s.AddWarning(manifest.Warning{Resource: id, Category: manifest.WarnSOPSUnreadable, Detail: []string{"token"}})
	before := s.Warnings()
	result := &Result{Warnings: before}
	assert.Equal(t, len(result.WarningsFor(nil)), 0)
	assert.Diff(t, s.Warnings(), before)
	assert.Diff(t, result.Warnings, before)
}
