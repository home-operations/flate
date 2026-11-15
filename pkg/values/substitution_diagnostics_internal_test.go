package values

import (
	"errors"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestExpandPostBuildSubstituteReference_CapturedFailure(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      "FluxInstance",
		Namespace: "flux-system",
		Name:      "sync",
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", owner).WithNamespace("sources").(*manifest.Kustomization)
	entry.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
		{Kind: "Unsupported", Name: "ignored"},
		{Kind: manifest.KindConfigMap, Name: "missing"},
		{Kind: manifest.KindConfigMap, Name: "optional", Optional: true},
		{Kind: manifest.KindSecret, Name: "unreadable"},
	}, Substitute: map[string]string{"INLINE": "retained"}}
	before := entry.Clone()
	inputs := resolveInputsForTest(entry, NewStoreProvider(store.New()), testPermissions(nil))
	err := ExpandPostBuildSubstituteReference(entry, inputs)
	assert.Equal(t, err, inputs[0].Err())
	assert.Equal(t, errors.Is(err, manifest.ErrObjectNotFound), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	assert.Equal(t, manifest.Equal(entry, before), true)
	assert.Equal(
		t,
		inputs[0].ID(),
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "sources", Name: "missing"},
	)
}

func TestResolve_DiagnosticsAcceptedOnce(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	st.AddObject(owner)
	ref := manifest.Reference{Kind: manifest.KindSecret, Namespace: "apps", Name: "missing", Use: substitutionUse}
	result := Resolve(&ResolveRequest{Reference: ref, Permission: testPermissions{manifest.KindSecret}})
	assert.Equal(t, result.Effect(), Omitted)
	var reads store.ReadSet
	reads.Init(st, owner.Named(), nil)
	defer reads.Retire()
	result.ReportWarnings(&reads)
	result.ReportWarnings(&reads)
	assert.Equal(t, len(st.Warnings()), 0)
	reads.Accept(store.AssessmentReconciled)
	warnings := st.Warnings()
	assert.Equal(t, len(warnings), 1)
	if len(warnings) == 1 {
		assert.Equal(t, warnings[0].Resource, manifest.NamedResource{})
		assert.Equal(t, warnings[0].Subject, manifest.DiagnosticIdentity{})
		assert.Equal(t, warnings[0].Count, 1)
		assert.Equal(t, warnings[0].Category, manifest.WarnUnresolvedSubstitution)
	}
}
