package manifest

import (
	"reflect"
	"testing"
)

func TestStripResourceFields(t *testing.T) {
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
		d := newDoc()
		StripResourceFields(d, []string{"spec.restic.unlock"})

		restic := d["spec"].(map[string]any)["restic"].(map[string]any)
		if _, ok := restic["unlock"]; ok {
			t.Fatal("spec.restic.unlock not deleted")
		}
		if restic["repository"] != "app-restic-secret" {
			t.Errorf("sibling spec.restic.repository was disturbed: %v", restic["repository"])
		}
		if _, ok := restic["retain"]; !ok {
			t.Error("sibling spec.restic.retain was disturbed")
		}
		// Parent map is kept (leaf-only delete) so both diff sides match.
		if _, ok := d["spec"].(map[string]any)["restic"]; !ok {
			t.Error("parent spec.restic should not be pruned")
		}
	})

	t.Run("no-op when a segment is absent", func(t *testing.T) {
		d := newDoc()
		before := DeepCopyMap(d)
		StripResourceFields(d, []string{"spec.absent.field", "metadata.annotations.x"})
		if !reflect.DeepEqual(d, before) {
			t.Error("absent-path strip mutated the doc")
		}
	})

	t.Run("no-op when a segment is not a map", func(t *testing.T) {
		d := newDoc()
		before := DeepCopyMap(d)
		// spec.sourcePVC is a string; descending through it must not panic
		// or mutate.
		StripResourceFields(d, []string{"spec.sourcePVC.deeper"})
		if !reflect.DeepEqual(d, before) {
			t.Error("non-map segment strip mutated the doc")
		}
	})

	t.Run("multiple paths + empty path ignored", func(t *testing.T) {
		d := newDoc()
		StripResourceFields(d, []string{"", "spec.restic.unlock", "spec.trigger.schedule"})
		restic := d["spec"].(map[string]any)["restic"].(map[string]any)
		if _, ok := restic["unlock"]; ok {
			t.Error("spec.restic.unlock not deleted")
		}
		trig := d["spec"].(map[string]any)["trigger"].(map[string]any)
		if _, ok := trig["schedule"]; ok {
			t.Error("spec.trigger.schedule not deleted")
		}
	})

	t.Run("top-level leaf", func(t *testing.T) {
		d := newDoc()
		StripResourceFields(d, []string{"kind"})
		if _, ok := d["kind"]; ok {
			t.Error("top-level kind not deleted")
		}
	})
}
