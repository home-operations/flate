package depwait

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
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
				Input: store.ReferenceFacts{Materializers: producers},
			}, manifest.DrainNone)
			assert.Equal(t, got.Kind, tc.want)
			if tc.want == schedule.Blocked {
				assert.Diff(t, got.Blocked, producers[1:])
				return
			}
			captured, ok := errors.AsType[*manifest.DependencyFailedError](got.Result.Err())
			assert.Equal(t, ok, true)
			assert.Diff(t, got.Failed, []manifest.NamedResource{target})
			assert.Equal(t, len(captured.Reasons[target].Causes), 2)
			for i, id := range producers {
				assert.Equal(t, captured.Reasons[target].Causes[i].Subject.ID, id)
			}
		})
	}
}
