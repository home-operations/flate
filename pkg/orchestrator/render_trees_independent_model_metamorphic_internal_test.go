package orchestrator

import (
	"bytes"
	"maps"
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestRenderTrees_IndependentModel_Metamorphic(t *testing.T) {
	t.Parallel()
	fixture := decodeRenderModelCase([]byte{1, 0})
	first, err := checkRenderModel(t, &fixture)
	if err != nil {
		failRenderModel(t, []byte{1, 0}, &fixture, err)
	}
	second, err := checkRenderModel(t, &fixture)
	if err != nil {
		failRenderModel(t, []byte{1, 0}, &fixture, err)
	}
	{
		assert.FatalIf(t, !bytes.Equal(first, second), "two successful runs differ:\n%s\n%s", first, second)
	}
	for index, name := range []string{"disjoint-growth", "source-bijection", "reverse-edit", "equivalent-placement"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testRenderTreesIndependentModelMetamorphicCase(
				t,
				testRenderTreesIndependentModelMetamorphicCaseInputs{
					fixture: &fixture,
					first:   &first,
					index:   &index,
					name:    &name,
				},
			)
		},
		)
	}
}

func FuzzRenderTrees_IndependentModel(f *testing.F) {
	for _, tc := range renderModelInputs() {
		f.Add(tc.input)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		fixture := decodeRenderModelCase(input)
		if _, err := checkRenderModel(t, &fixture); err != nil {
			failRenderModel(t, input, &fixture, err)
		}
	})
}

type testRenderTreesIndependentModelMetamorphicCaseInputs struct {
	fixture *renderModelCase
	first   *[]byte
	index   *int
	name    *string
}

func testRenderTreesIndependentModelMetamorphicCase(
	t *testing.T,
	testCaseInput testRenderTreesIndependentModelMetamorphicCaseInputs,
) {
	t.Helper()

	encoded := []byte{255, byte((*testCaseInput.index))}
	control := decodeRenderModelCase(encoded)
	switch *testCaseInput.name {
	case "disjoint-growth":
		want, got := compareModelWorlds(testCaseInput.fixture.before, testCaseInput.fixture.after), compareModelWorlds(
			control.before,
			control.after,
		)
		{
			assert.FatalIf(
				t,
				!reflect.DeepEqual(want, got),
				"disjoint growth changed expected membership: want=%+v got=%+v",
				want,
				got,
			)
		}
	case "reverse-edit":
		want, got := compareModelWorlds(testCaseInput.fixture.before, testCaseInput.fixture.after), compareModelWorlds(
			control.before,
			control.after,
		)
		{
			assert.FatalIf(t, !maps.Equal(want.before.affected, got.after.affected) ||
				!maps.Equal(
					want.after.affected,
					got.before.affected,
				), "%v", "reversing an edit changed its symmetric affected membership")
		}
	}
	output, err := checkRenderModel(t, &control)
	if err != nil {
		failRenderModel(t, encoded, &control, err)
	}
	{
		assert.FatalIf(
			t, (*testCaseInput.name) !=
				"reverse-edit" && !bytes.Equal((*testCaseInput.first), output), "%s changed equivalent selected output:\n%s\n%s", (*testCaseInput.name), (*testCaseInput.first), output,
		)
	}
}
