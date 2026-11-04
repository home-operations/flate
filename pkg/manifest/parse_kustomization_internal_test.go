package manifest

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

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
	{
		assert.FatalIf(t, err != nil, "parseKustomization: %v", err)
	}
	deps := refsForUse(t, k, "dependsOn")
	{
		assert.FatalIf(t, len(deps) != 1, "DependsOn len = %d", len(deps))
	}
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
			{
				assert.FatalIf(
					t,
					!errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux),
					"invalid typed field error = %v, want input error",
					err,
				)
			}
			{
				assert.FatalIf(
					t,
					!strings.Contains(err.Error(), "cannot unmarshal string"),
					"missing decoder diagnostic: %v",
					err,
				)
			}
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
	{
		assert.FatalIf(t, err != nil, "parseKustomization: %v", err)
	}
	deps := refsForUse(t, k, "dependsOn")
	{
		assert.FatalIf(t, len(deps) != 2, "DependsOn len = %d, want 2", len(deps))
	}
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
