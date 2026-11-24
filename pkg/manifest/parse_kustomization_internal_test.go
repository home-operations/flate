package manifest

import (
	"errors"
	"slices"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil/assert"
)

//nolint:tparallel // testing.AllocsPerRun measures process-wide allocation counters.
func TestReferenceCapacity_KustomizationFieldOrder(t *testing.T) {
	k := &Kustomization{Name: "owner", Namespace: "apps", Path: "./apps"}
	k.TargetNamespace = "target"
	k.DependsOn = []kustomizev1.DependencyReference{{Name: "prerequisite"}}
	k.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
		{Kind: KindConfigMap, Name: "vars"},
		{Kind: "unsupported", Name: "invalid"},
		{Kind: KindSecret},
	}}
	assert.Equal(t, k.ReferenceCapacity(), ReferenceLimits{References: 6, DataInputs: 3})
	assert.Equal(t, testing.AllocsPerRun(10, func() { k.ReferenceCapacity() }), float64(0))
	relations := slices.Collect(References(k, ReferenceRelations))
	all := slices.Collect(References(k, ReferenceAll))
	assert.Equal(t, len(all), 5)
	assert.Equal(t, all[0].Use, "")
	assert.Equal(t, all[0].NamedResource, k.EffectiveSourceID())
	assert.Equal(t, len(relations), 4)
	assert.Equal(t, relations[0].Use, "targetNamespace")
	assert.Equal(t, relations[1].Use, dependsOnKey)
	assert.Equal(t, relations[1].Namespace, "apps")
	assert.Equal(t, relations[1].NamespaceDefault, true)
	assert.Equal(t, relations[2].Use, substituteFromKey)
	assert.Equal(t, relations[2].Namespace, "apps")
	assert.Equal(t, relations[2].NamespaceDefault, true)
	assert.Equal(t, relations[3].Position, 2)
	assert.Equal(t, relations[3].Name, "")
	for _, tc := range []struct {
		view ReferenceView
		refs []Reference
	}{{ReferenceAll, all}, {ReferenceRelations, relations}} {
		for stop := range tc.refs {
			count := 0
			References(k, tc.view)(func(ref Reference) bool {
				assert.FatalIf(t, count > stop, "reference iterator continued after position %d", stop)
				assert.Equal(t, ref.NamedResource, tc.refs[count].NamedResource)
				assert.Equal(t, ref.Use, tc.refs[count].Use)
				assert.Equal(t, ref.Position, tc.refs[count].Position)
				count++
				return count <= stop
			})
			assert.Equal(t, count, stop+1)
		}
	}

	t.Run("partial_source", func(t *testing.T) {
		t.Parallel()
		allowUnexported := cmp.AllowUnexported(Reference{})
		partial := *k
		partial.SourceRef.Name = "repo"
		assert.Equal(t, partial.ReferenceCapacity(), k.ReferenceCapacity())
		assert.Equal(t, cmp.Diff(all, slices.Collect(References(&partial, ReferenceAll)), allowUnexported), "")
		gotRelations := slices.Collect(References(&partial, ReferenceRelations))
		assert.Equal(t, cmp.Diff(relations, gotRelations, allowUnexported), "")

		partial.Path = ""
		authored := Reference{
			NamedResource:    partial.SourceID(),
			sourceRoles:      bindsSource | readsSourceTree,
			Use:              sourceKey,
			Gate:             GateInputs | GateArtifacts,
			Artifact:         true,
			ArtifactStages:   GateInputs | GateArtifacts,
			NamespaceDefault: true,
		}
		want := slices.Concat([]Reference{authored}, relations)
		assert.Equal(t, partial.ReferenceCapacity(), k.ReferenceCapacity())
		assert.Equal(t, cmp.Diff(want, slices.Collect(References(&partial, ReferenceAll)), allowUnexported), "")
		gotRelations = slices.Collect(References(&partial, ReferenceRelations))
		assert.Equal(t, cmp.Diff(want, gotRelations, allowUnexported), "")
	})
}

func TestParseKustomization_DependsOnReadyExpr(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: src}
  dependsOn:
    - name: infra
      readyExpr: |
        object.status.conditions.exists(c, c.type == "InfraInitialized" && c.status == "True")
  interval: 5m
`)
	k, err := parseKustomization(doc)
	assert.FatalIf(t, err != nil, "parseKustomization: %v", err)
	deps := refsForUse(t, k, "dependsOn")
	assert.FatalIf(t, len(deps) != 1, "DependsOn len = %d", len(deps))
	if deps[0].Name != "infra" {
		t.Errorf("name: %q", deps[0].Name)
	}
	if !strings.Contains(*deps[0].ReadyExpr, "InfraInitialized") {
		t.Errorf("ReadyExpr lost: %q", *deps[0].ReadyExpr)
	}
}

func TestParseKustomization_InvalidGeneratorFields(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"patches", "images", "components", "buildMetadata"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			_, err := parseKustomization(
				mustYAML(
					t,
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: apps}\nspec: {"+field+": not-a-list}\n",
				),
			)
			assert.FatalIf(
				t,
				!errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux),
				"invalid typed field error = %v, want input error",
				err,
			)
			assert.FatalIf(
				t,
				!strings.Contains(err.Error(), "cannot unmarshal string"),
				"missing decoder diagnostic: %v",
				err,
			)
		})
	}
}

func TestParseKustomization_DependsOn(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: cluster
  dependsOn:
    - name: infra
    - name: extras
      namespace: other
  postBuild:
    substitute:
      DOMAIN: example.com
    substituteFrom:
      - kind: ConfigMap
        name: cluster-vars
        optional: true
`)
	k, err := parseKustomization(doc)
	assert.FatalIf(t, err != nil, "parseKustomization: %v", err)
	deps := refsForUse(t, k, "dependsOn")
	assert.FatalIf(t, len(deps) != 2, "DependsOn len = %d, want 2", len(deps))
	{
		got := deps[0].NamespacedName()
		assert.ErrorIf(t, got != "flux-system/infra", "DependsOn[0] = %q, want flux-system/infra", got)
	}
	{
		got := deps[1].NamespacedName()
		assert.ErrorIf(t, got != "other/extras", "DependsOn[1] = %q, want other/extras", got)
	}
	if len(k.PostBuild.SubstituteFrom) != 1 || !k.PostBuild.SubstituteFrom[0].Optional {
		t.Errorf("substituteFrom = %+v", k.PostBuild.SubstituteFrom)
	}
	if k.PostBuild.Substitute["DOMAIN"] != "example.com" {
		t.Errorf("substitute = %+v", k.PostBuild.Substitute)
	}
}
