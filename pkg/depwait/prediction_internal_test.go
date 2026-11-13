package depwait

import (
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPrediction_DetachedReadiness(t *testing.T) {
	t.Parallel()
	for _, blocked := range []bool{false, true} {
		name := "failed parent"
		if blocked {
			name = "blocked parent"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := &manifest.ResourceSet{Name: "child", Namespace: "apps"}
			missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "missing"}
			st.AddObject(parent)
			failure := errors.New("parent failed")
			if blocked {
				failure = &manifest.DependencyFailedError{
					Parent: parent.Named(),
					Failed: []manifest.NamedResource{missing},
				}
			}
			st.UpdateFailure(parent.Named(), failure)
			before := st.FailedResources()
			var reads store.ReadSet
			reads.Init(st, child.Named(), nil)
			defer reads.Retire()
			waiter := Waiter{Store: &reads, Parent: child.Named(), Missing: NewMissingPolicy(nil)}
			references := []manifest.Reference{{NamedResource: missing, Gate: manifest.GateInputs, Use: "dependsOn"}}
			err := waiter.Prediction(t.Context(), &reads, slices.Values(references))
			assert.Equal(t, err != nil, true)
			status := store.FailureStatus(err)
			assert.Equal(t, status.Outcome(), store.OutcomeBlocked)
			assert.Diff(t, status.BlockedBy(), []manifest.NamedResource{missing})
			assert.Equal(t, len(st.FailedResources()), len(before))
			_, exists := st.GetStatus(child.Named())
			assert.Equal(t, exists, false)
			assert.Equal(t, st.GetDeclaration(child.Named()) == nil, true)
			assert.Equal(t, st.HasInventory(child.Named()), false)
			assert.Equal(t, len(st.Warnings()), 0)
		})
	}
}
