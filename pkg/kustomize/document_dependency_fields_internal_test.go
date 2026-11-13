package kustomize

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

//nolint:paralleltest,tparallel // Subtests finish clone checks before the parent reuses the shared original.
func TestDocumentDependencies_PreservesFields(t *testing.T) {
	t.Parallel()
	docs, err := testutil.SplitDocs([]byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: apps}
spec:
  path: ./app
  dependsOn:
  - {name: plain, namespace: shared, readyExpr: 'dep.metadata.name == "plain"'}
  - {name: '${PREFIX}config', readyExpr: 'dep.metadata.name == "prod-config"'}
  - {name: '${PREFIX}tail', namespace: '${NS}'}
`))
	assert.NoError(t, err)
	parsed, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	assert.NoError(t, err)
	original := parsed.Object.(*manifest.Kustomization)
	before := original.Clone()
	data, err := values.SubstitutionDocument(manifest.DocOccurrence{Document: docs[0]}, false)
	assert.NoError(t, err)
	assert.FatalIf(t, data != nil || parsed.Object != original, "%s", "no-op preparation replaced object")
	other := &manifest.RawObject{APIVersion: "v1", Kind: manifest.KindConfigMap, Name: "config"}
	otherDoc := testutil.ObjectDocument(other)
	literal, err := values.SubstitutionDocument(manifest.DocOccurrence{Document: otherDoc}, true)
	assert.NoError(t, err)
	assert.FatalIf(t, literal != nil || other.Kind != manifest.KindConfigMap, "%s", "literal object replaced")
	for _, excluded := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "excluded"}[excluded], func(t *testing.T) {
			child := original.Clone()
			if excluded {
				child.Annotations = map[string]string{"kustomize.toolkit.fluxcd.io/substitute": "disabled"}
			}
			parent := &manifest.Kustomization{
				PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"PREFIX": "prod-", "NS": "infra"}},
			}
			got := shapeDependency(t, parent, child).Object.(*manifest.Kustomization)
			want := child.Clone()
			if !excluded {
				want.DependsOn[1].Name = "prod-config"
				want.DependsOn[2].Name = "prod-tail"
				want.DependsOn[2].Namespace = "infra"
			}
			assert.FatalIf(t, got == child, "%s", "private document preparation reused borrowed object")
			diff := cmp.Diff(
				want,
				got,
				cmp.AllowUnexported(manifest.Kustomization{}),
				cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
			)
			assert.FatalIf(t, diff != "", "transformed fields (-want +got):\n%s", diff)
			diff = cmp.Diff(
				before,
				original,
				cmp.AllowUnexported(manifest.Kustomization{}),
				cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
			)
			assert.FatalIf(t, diff != "", "authored object mutated (-want +got):\n%s", diff)
			for ref := range manifest.References(got, manifest.ReferenceRelations) {
				assert.FatalIf(
					t,
					ref.Use == "dependsOn" && ref.Position == 1 &&
						(ref.Namespace != "apps" || got.DependsOn[1].Namespace != ""),
					"%s",
					"omitted namespace lost inheritance",
				)
			}
		})
	}
	parent := &manifest.Kustomization{
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"PREFIX": "${PREFIX}", "NS": "infra"}},
	}
	got := shapeDependency(t, parent, original).Object.(*manifest.Kustomization)
	assert.FatalIf(t, got == original, "%s", "namespace-only change did not clone")
	assert.Equal(t, got.DependsOn[1].Name, original.DependsOn[1].Name)
	assert.Equal(t, got.DependsOn[2].Namespace, "infra")
}
