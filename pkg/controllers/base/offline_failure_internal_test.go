package base

import (
	"context"
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRenderFailure_DocumentEvidence(t *testing.T) {
	t.Parallel()
	failure := manifest.NewError(manifest.ErrInput, nil, "invalid rendered document")
	ref := manifest.Reference{
		Kind:      manifest.KindSecret,
		Namespace: "apps",
		Name:      "input",
		Use:       "substituteFrom",
		Key:       "TOKEN",
		Position:  1,
	}
	for _, tc := range []struct {
		name     string
		docs     []manifest.DocOccurrence
		err      error
		canceled bool
		skipped  bool
	}{
		{name: "failed document", docs: []manifest.DocOccurrence{{Ordinal: 4, Err: failure, ConsumedUnreadable: []manifest.Reference{ref}}}, skipped: true},
		{name: "healthy sibling", docs: []manifest.DocOccurrence{{Ordinal: 3, ConsumedUnreadable: []manifest.Reference{ref}}, {Ordinal: 4, Err: failure}}},
		{name: "mixed failures", docs: []manifest.DocOccurrence{{Ordinal: 3, Err: failure, ConsumedUnreadable: []manifest.Reference{ref}}, {Ordinal: 4, Err: failure}}},
		{name: "strict error", docs: []manifest.DocOccurrence{{Ordinal: 4, Err: failure}}, err: failure},
		{name: "cancel error", docs: []manifest.DocOccurrence{{Err: context.Canceled, ConsumedUnreadable: []manifest.Reference{ref}}}},
		{name: "deadline error", docs: []manifest.DocOccurrence{{Err: context.DeadlineExceeded, ConsumedUnreadable: []manifest.Reference{ref}}}},
		{name: "canceled context", docs: []manifest.DocOccurrence{{Err: failure, ConsumedUnreadable: []manifest.Reference{ref}}}, canceled: true},
		{name: "successful unreadable", docs: []manifest.DocOccurrence{{ConsumedUnreadable: []manifest.Reference{ref}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			if tc.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			run := &Execution{}
			err := run.RenderFailure(ctx, tc.err, tc.docs, nil)
			assert.Equal(t, errors.Is(err, manifest.ErrSourceSkipped), tc.skipped)
			if tc.skipped {
				assert.Equal(t, errors.Is(err, failure), true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(
					t,
					err.Error(),
					"cannot render offline: unreadable inputs substituteFrom Secret apps/input key \"TOKEN\" (reference 2): document 5: invalid rendered document",
				)
			}
		})
	}
}

func TestRenderFailure_WithdrawsAcceptedOutput(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	st.AddObject(owner)
	tasks := task.NewBounded(2)
	c := New(st, tasks)
	run := gateExecution(c, owner.Named())
	defer run.Close()
	run.Reads.GetObject(owner.Named())
	child := &manifest.ConfigMap{Name: "stale", Namespace: "apps"}
	output := store.Emission{
		Children: []store.Child{{Obj: child}},
		Complete: true,
		Render:   manifest.NewRenderReceipt("old", nil),
	}
	assert.NoError(t, run.Emit(t.Context(), owner.Named(), &output, nil))
	assert.Equal(t, st.GetObject(child.Named()) != nil, true)
	failure := manifest.NewError(manifest.ErrInput, nil, "invalid output")
	err := run.RenderFailure(
		t.Context(),
		failure,
		nil,
		[]manifest.Reference{
			{Kind: manifest.KindSecret, Namespace: "apps", Name: "input", Use: "valuesFrom", Key: "values.yaml"},
		},
	)
	run.finishFailure(t.Context(), owner.Named(), err)
	assert.Equal(t, st.GetObject(child.Named()) == nil, true)
	_, _, after := st.GetEmission(owner.Named())
	defer after.Finish()
	assert.Equal(t, len(after.Manifests), 0)
	assert.Equal(t, after.Render == nil, true)
	assert.Equal(t, after.Fingerprint, "")
	status, _ := st.GetStatus(owner.Named())
	assert.Equal(t, status.Outcome(), store.OutcomeSkipped)
}
