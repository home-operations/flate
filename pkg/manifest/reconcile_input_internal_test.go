package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestRawObject_ReconcileInput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		uid, reconcile any
		wantUID        string
		disabled       bool
	}{
		{"missing", nil, nil, "", false},
		{"uid", "current", nil, "current", false},
		{"non-string", 42, true, "", false},
		{"disabled", "current", "DiSaBlEd", "current", true},
		{"enabled", "current", "enabled", "current", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{
				"metadata": map[string]any{
					"uid": tc.uid,
					"annotations": map[string]any{
						"source.extensions.fluxcd.io/reconcile": tc.reconcile,
						"ignored":                               "anything",
					},
				},
			}
			obj := &RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator"}
			uid, disabled, consumes := obj.ReconcileInput(doc)
			{
				assert.FatalIf(
					t,
					uid != tc.wantUID || disabled != tc.disabled || !consumes,
					"ReconcileInput = %q/%t/%t",
					uid,
					disabled,
					consumes,
				)
			}
			obj.Kind = "Other"
			{
				uid, disabled, consumes := obj.ReconcileInput(doc)
				assert.FatalIf(
					t,
					uid != "" || disabled || consumes,
					"unrelated kind consumed metadata: %q/%t/%t",
					uid,
					disabled,
					consumes,
				)
			}
		})
	}
}
