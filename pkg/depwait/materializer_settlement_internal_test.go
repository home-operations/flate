package depwait

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestAvailable_MaterializerTargetSettlement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status store.Status
		want   schedule.Kind
	}{
		{"pending alternative", store.StatusPending, schedule.Blocked},
		{"terminal alternatives", store.StatusFailed, schedule.DependencyFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := testutil.KustomizationID("apps", "consumer")
			producers := []manifest.NamedResource{
				testutil.KustomizationID("apps", "first"),
				testutil.KustomizationID("apps", "second"),
			}
			for i, id := range producers {
				st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
				status := store.StatusFailed
				if i == 1 {
					status = tc.status
				}
				st.UpdateStatus(id, store.StatusInfo{Status: status, Message: "materializer failed"})
			}
			var reads store.ReadSet
			reads.Init(st, parent, nil)
			defer reads.Close()
			waiter := Waiter{Store: &reads, Parent: parent, Missing: NewMissingPolicy(nil)}
			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
			got := waiter.Available(t.Context(), DataDependency{
				NamedResource: target, Use: "substituteFrom",
				Input: store.ReferenceFacts{Materializers: []manifest.NamedResource{producers[1], producers[0]}},
			}, manifest.DrainNone)
			assert.Equal(t, got.Kind, tc.want)
			if tc.want == schedule.Blocked {
				assert.Diff(t, got.Blocked, producers[1:])
				got = waiter.Available(t.Context(), DataDependency{
					NamedResource: target, Use: "substituteFrom",
					Input: store.ReferenceFacts{Materializers: []manifest.NamedResource{producers[1], producers[0]}},
				}, manifest.DrainForce)
				assert.Equal(t, got.Kind, schedule.DependencyFailed)
				assert.Diff(t, got.Failed, []manifest.NamedResource{target})
				assert.Diff(t, got.Reason.Causes, []manifest.DiagnosticReason{
					{Subject: manifest.DiagnosticFor(producers[0], nil), Message: "materializer failed"},
					{Subject: manifest.DiagnosticFor(producers[1], nil), Message: "not ready"},
				})
				return
			}
			captured, ok := errors.AsType[*manifest.DependencyFailedError](got.Result.Err())
			assert.Equal(t, ok, true)
			assert.Diff(t, got.Failed, []manifest.NamedResource{target})
			assert.Equal(t, len(captured.Reasons[target].Causes), 2)
			for i, id := range producers {
				assert.Equal(t, captured.Reasons[target].Causes[i].Subject.ID, id)
				assert.Equal(t, captured.Reasons[target].Causes[i].Message, "materializer failed")
			}
		})
	}
}

func TestAvailable_CapturedProducerCauses(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := testutil.HelmReleaseID("apps", "consumer")
	producer := testutil.KustomizationID("apps", "producer")
	cause := testutil.KustomizationID("apps", "cause")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	st.UpdateFailure(producer, &manifest.DependencyFailedError{
		Parent: producer, Failed: []manifest.NamedResource{cause},
		Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{cause: {
			Subject: manifest.DiagnosticFor(cause, nil), Message: "render failed",
		}},
	})
	var reads store.ReadSet
	reads.Init(st, parent, nil)
	defer reads.Close()
	waiter := Waiter{Store: &reads, Parent: parent, Missing: NewMissingPolicy(nil)}
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	got := waiter.Available(t.Context(), DataDependency{
		NamedResource: target, Use: "valuesFrom", Key: "uri",
		Input: store.ReferenceFacts{Materializers: []manifest.NamedResource{producer}},
	}, manifest.DrainForce)
	assert.Equal(t, got.Kind, schedule.DependencyFailed)
	assert.Equal(t, got.Result.NeedsInput(), true)
	assert.Equal(t, got.Result.Effect(), values.Failed)
	_, retained := errors.AsType[*manifest.DependencyFailedError](got.Result.Err())
	assert.Equal(t, retained, false)
	assert.Diff(t, got.Failed, []manifest.NamedResource{target})
	assert.Diff(t, got.Reason, manifest.DiagnosticReason{
		Subject: manifest.DiagnosticFor(target, nil), Message: "not found; supply a readable document",
		Causes: []manifest.DiagnosticReason{{
			Subject: manifest.DiagnosticFor(producer, nil),
			Causes: []manifest.DiagnosticReason{{
				Subject: manifest.DiagnosticFor(cause, nil), Message: "render failed",
			}},
		}},
	})
}

func TestAvailable_UnavailableMaterialRetainsInputFailure(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := testutil.HelmReleaseID("apps", "consumer")
	producer := testutil.KustomizationID("apps", "producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	st.UpdateStatus(producer, store.StatusInfo{Status: store.StatusFailed, Message: "render failed"})
	var reads store.ReadSet
	reads.Init(st, parent, nil)
	defer reads.Close()
	waiter := Waiter{Store: &reads, Parent: parent, Missing: NewMissingPolicy(nil)}
	data := &manifest.Secret{Name: "input", Namespace: "apps", StringData: map[string]any{"other": "value"}}
	got := waiter.Available(t.Context(), DataDependency{
		NamedResource: data.Named(), Use: "valuesFrom", Key: "uri",
		Input: store.ReferenceFacts{Data: data, Materializers: []manifest.NamedResource{producer}},
	}, manifest.DrainForce)
	assert.Equal(t, got.Kind, schedule.DependencyFailed)
	assert.Equal(t, got.Result.NeedsInput(), false)
	assert.Equal(t, errors.Is(got.Result.Err(), manifest.ErrInvalidValuesReference), true)
	assert.Equal(t, errors.Is(got.Result.Err(), manifest.ErrObjectNotFound), false)
	assert.Diff(t, got.Failed, []manifest.NamedResource{data.Named()})
	assert.Diff(t, got.Reason, manifest.DiagnosticReason{
		Subject: manifest.DiagnosticFor(data.Named(), nil), Message: "key \"uri\" not found",
		Causes: []manifest.DiagnosticReason{{Subject: manifest.DiagnosticFor(producer, nil), Message: "render failed"}},
	})
}
