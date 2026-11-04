package manifest

import (
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestStripResourceFields(t *testing.T) {
	t.Parallel()
	newDoc := func() map[string]any {
		return map[string]any{
			"apiVersion": "volsync.backube/v1alpha1",
			"kind":       "ReplicationSource",
			"metadata":   map[string]any{"name": "app-config"},
			"spec": map[string]any{
				"sourcePVC": "app-config",
				"trigger":   map[string]any{"schedule": "0 * * * *"},
				"restic": map[string]any{
					"unlock":     "20260606112230",
					"repository": "app-restic-secret",
					"retain":     map[string]any{"daily": 7},
				},
			},
		}
	}

	t.Run("deletes nested leaf, leaves siblings + parents", func(t *testing.T) {
		t.Parallel()
		testStripResourceFieldsDeletesNestedLeafLeavesSiblingsParents(
			t,
			&newDoc,
		)
	},

	// Parent map is kept (leaf-only delete) so both diff sides match.

	)

	t.Run("no-op when a segment is absent", func(t *testing.T) {
		t.Parallel()
		testStripResourceFieldsNoOpWhenASegmentIsAbsent(
			t,
			&newDoc,
		)
	},
	)

	t.Run("no-op when a segment is not a map", func(t *testing.T) {
		t.Parallel()
		testStripResourceFieldsNoOpWhenASegmentIsNotAMap(

			// spec.sourcePVC is a string; descending through it must not panic
			// or mutate.
			t, &newDoc)
	},
	)

	t.Run("multiple paths + empty path ignored", func(t *testing.T) {
		t.Parallel()
		testStripResourceFieldsMultiplePathsEmptyPathIgnored(
			t,
			&newDoc,
		)
	},
	)

	t.Run("top-level leaf", func(t *testing.T) {
		t.Parallel()
		testStripResourceFieldsTopLevelLeaf(t, &newDoc)
	},
	)
}

func testStripResourceFieldsDeletesNestedLeafLeavesSiblingsParents(
	t *testing.T,
	testCaseInput *func() map[string]any,
) {
	t.Helper()

	d := (*testCaseInput)()
	StripResourceFields(d, []string{"spec.restic.unlock"})

	restic := d["spec"].(map[string]any)["restic"].(map[string]any)
	{
		_, ok := restic["unlock"]
		assert.FatalIf(t, ok, "%s", "spec.restic.unlock not deleted")
	}
	if restic["repository"] != "app-restic-secret" {
		t.Errorf("sibling spec.restic.repository was disturbed: %v", restic["repository"])
	}
	{
		_, ok := restic["retain"]
		assert.ErrorIf(t, !ok, "%s", "sibling spec.restic.retain was disturbed")
	}

	{
		_, ok := d["spec"].(map[string]any)["restic"]
		assert.ErrorIf(t, !ok, "%s", "parent spec.restic should not be pruned")
	}
}

func testStripResourceFieldsNoOpWhenASegmentIsAbsent(
	t *testing.T,
	testCaseInput *func() map[string]any,
) {
	t.Helper()

	d := (*testCaseInput)()
	before := DeepCopyMap(d)
	StripResourceFields(d, []string{"spec.absent.field", "metadata.annotations.x"})
	assert.ErrorIf(t, !reflect.DeepEqual(d, before), "%s", "absent-path strip mutated the doc")
}

func testStripResourceFieldsNoOpWhenASegmentIsNotAMap(
	t *testing.T,
	testCaseInput *func() map[string]any,
) {
	t.Helper()

	d := (*testCaseInput)()
	before := DeepCopyMap(d)

	StripResourceFields(d, []string{"spec.sourcePVC.deeper"})
	assert.ErrorIf(t, !reflect.DeepEqual(d, before), "%s", "non-map segment strip mutated the doc")
}

func testStripResourceFieldsMultiplePathsEmptyPathIgnored(
	t *testing.T,
	testCaseInput *func() map[string]any,
) {
	t.Helper()

	d := (*testCaseInput)()
	StripResourceFields(d, []string{"", "spec.restic.unlock", "spec.trigger.schedule"})
	restic := d["spec"].(map[string]any)["restic"].(map[string]any)
	{
		_, ok := restic["unlock"]
		assert.ErrorIf(t, ok, "%s", "spec.restic.unlock not deleted")
	}
	trig := d["spec"].(map[string]any)["trigger"].(map[string]any)
	{
		_, ok := trig["schedule"]
		assert.ErrorIf(t, ok, "%s", "spec.trigger.schedule not deleted")
	}
}

func testStripResourceFieldsTopLevelLeaf(t *testing.T, testCaseInput *func() map[string]any,
) {
	t.Helper()

	d := (*testCaseInput)()
	StripResourceFields(d, []string{"kind"})
	{
		_, ok := d["kind"]
		assert.ErrorIf(t, ok, "%s", "top-level kind not deleted")
	}
}
