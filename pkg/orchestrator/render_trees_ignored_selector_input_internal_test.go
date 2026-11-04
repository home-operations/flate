package orchestrator

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_IgnoredSelectorInput(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	consumer := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "consumer",
	}
	nonmatching := consumer
	nonmatching.Name = "nonmatching"
	for side, root := range roots {
		for _, name := range []string{"consumer", "nonmatching"} {
			role := "selected"
			if name == "nonmatching" {
				role = "other"
			}
			testutil.WriteFile(
				t,
				root,
				"flux/"+name+".yaml",
				testutil.KustomizationYAML(name+"-owner", "apps", "  path: ./"+name+"\n"),
			)
			testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: [set.yaml]\n")
			testutil.WriteFile(t, root, name+"/set.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: apps}
spec:
  inputsFrom:
    - kind: ResourceSetInputProvider
      selector: {matchLabels: {role: %s}}
  resourcesTemplate: |
    << if inputs >>
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: %s-output, namespace: apps}
    data: {value: '<< inputs.value >>'}
    << end >>
`, name, role, name))
		}
		testutil.WriteFile(
			t,
			root,
			"flux/input.yaml",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: input, namespace: apps, labels: {role: selected}}\nspec: {type: Static, defaultValues: {value: ready}}\n",
		)
		ignore := ""
		if side == 1 {
			ignore = "input.yaml\n"
		}
		testutil.WriteFile(t, root, "flux/.krmignore", ignore)
	}
	var fullResults [2]*Result
	for side, root := range roots {
		full, err := RenderTrees(
			t.Context(),
			[]Tree{
				{
					ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					RepoRoot: root,
					Path:     filepath.Join(root, "flux"),
				},
			},
			Config{Concurrency: 2},
		)
		assert.NoError(t, err)
		fullResults[side] = full[0].Result
		assert.Equal(t, configMapNames(full[0].Result)["consumer-output"], side == 0)
		assert.Equal(t, configMapNames(full[0].Result)["nonmatching-output"], false)
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			testRenderTreesIgnoredSelectorInputCase2(
				t,
				testRenderTreesIgnoredSelectorInputCase2Inputs{
					roots:       &roots,
					consumer:    &consumer,
					nonmatching: &nonmatching,
					fullResults: &fullResults,
					reverse:     &reverse,
				},
			)
		},
		)
	}
}

type testRenderTreesIgnoredSelectorInputCaseInputs struct {
	roots       *[2]string
	consumer    *manifest.NamedResource
	nonmatching *manifest.NamedResource
	fullResults *[2]*Result
	reverse     *bool
}

func testRenderTreesIgnoredSelectorInputCase(
	t *testing.T,
	testCaseInput testRenderTreesIgnoredSelectorInputCaseInputs,
) {
	t.Helper()

	pair := (*testCaseInput.roots)
	if *testCaseInput.reverse {
		pair[0], pair[1] = pair[1], pair[0]
	}
	var trees []Tree
	if len(pair) > 0 {
		trees = make([]Tree, 0, len(pair))
	}
	for side, root := range pair {
		trees = append(
			trees,
			Tree{
				ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
				RepoRoot: root,
				Path:     filepath.Join(root, "flux"),
			},
		)
	}
	runs, err := RenderTrees(t.Context(), trees, Config{Concurrency: 2})
	assert.NoError(t, err)
	for side, run := range runs {
		needed, selected, _, _ := run.filter.Membership((*testCaseInput.consumer))
		t.Logf(
			"selector visibility: side=%d needed=%t selected=%t output=%t",
			run.ID.Side,
			needed,
			selected,
			configMapNames(run.Result)["consumer-output"],
		)
		visible := side == 0 != (*testCaseInput.reverse)
		assert.Equal(t, needed, true)
		assert.Equal(t, selected, true)
		assert.Equal(t, configMapNames(run.Result)["consumer-output"], visible)
		owner := testutil.KustomizationID("apps", "consumer-owner")
		fullSide := side
		if *testCaseInput.reverse {
			fullSide = 1 - side
		}
		oracle := outputByParent(testCaseInput.fullResults[fullSide].Manifests)[owner]
		actual := outputByParent(run.Result.Manifests)[owner]
		assert.Diff(t, actual, oracle)
		otherNeeded, otherSelected, otherDirty, _ := run.filter.Membership((*testCaseInput.nonmatching))
		t.Logf(
			"nonmatching declaration support: needed=%t selected=%t dirty=%t",
			otherNeeded,
			otherSelected,
			otherDirty,
		)
		assert.Equal(t, otherSelected, false)
		assert.Equal(t, otherDirty, false)
		assert.Equal(t, configMapNames(run.Result)["nonmatching-output"], false)
	}
}

type testRenderTreesIgnoredSelectorInputCase2Inputs struct {
	roots       *[2]string
	consumer    *manifest.NamedResource
	nonmatching *manifest.NamedResource
	fullResults *[2]*Result
	reverse     *bool
}

func testRenderTreesIgnoredSelectorInputCase2(
	t *testing.T,
	testCaseInput testRenderTreesIgnoredSelectorInputCase2Inputs,
) {
	t.Helper()

	testRenderTreesIgnoredSelectorInputCase(
		t,
		testRenderTreesIgnoredSelectorInputCaseInputs(testCaseInput),
	)
}
