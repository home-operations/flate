package depwait

import (
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrediction_DetachedReadiness(t *testing.T) {
	t.Parallel()
	t.Run("checkpoints", testPredictionCheckpointPrecedence)
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
			references := []manifest.Reference{
				{NamedResource: missing, Gate: manifest.GateInputs, Use: "dependsOn"},
				{NamedResource: parent.Named(), Use: "materializer", Optional: true},
			}
			err := waiter.Prediction(t.Context(), &reads, slices.Values(references), nil)
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

func testPredictionCheckpointPrecedence(t *testing.T) {
	t.Parallel()
	child := testutil.HelmReleaseID("apps", "child")
	known := testutil.HelmReleaseID("apps", "indexed-only")
	unknown := testutil.HelmReleaseID("apps", "unknown")
	chart := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "chart",
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		known: {File: "indexed.yaml", Path: "/source/indexed.yaml"},
	}}
	cases := []struct {
		name string

		changed, optional bool
		ordering          manifest.NamedResource
		chartGate         manifest.GateStage
		blocked           manifest.NamedResource
	}{
		{name: "full keeps indexed target", ordering: known, blocked: known},
		{name: "changed excludes indexed target", changed: true, ordering: known},
		{name: "changed keeps unknown target", changed: true, ordering: unknown, blocked: unknown},
		{
			name:      "chart declaration precedes data",
			chartGate: manifest.GateInputs | manifest.GateArtifacts,
			blocked:   chart,
		},
		{name: "data precedes late chart content", chartGate: manifest.GateArtifacts},
		{
			name:      "optional key permits late chart check",
			chartGate: manifest.GateArtifacts,
			optional:  true,
			blocked:   chart,
		},
	}
	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &manifest.ConfigMap{Name: "values", Namespace: "apps", Data: map[string]any{"other": "key: value"}}
			st.AddObject(input)
			session, err := selection.New(st, nil, selection.Request{Changed: tc.changed})
			assert.NoError(t, err)
			var reads store.ReadSet
			reads.Init(st, child, nil)
			defer reads.Retire()
			waiter := Waiter{
				Store: &reads, Parent: child, Missing: NewMissingPolicy(nil),
				Decision: session.Snapshot(), Indexed: index, Existence: errorPromotion{},
			}
			references := []manifest.Reference{{
				NamedResource: input.Named(), Use: "valuesFrom", Key: "values.yaml", Optional: tc.optional,
			}}
			if tc.ordering != (manifest.NamedResource{}) {
				references = append(references, manifest.Reference{
					NamedResource: tc.ordering, Use: "dependsOn", Ordering: true, Gate: manifest.GateInputs,
				})
			}
			if tc.chartGate != 0 {
				references = append(references, manifest.Reference{
					NamedResource: chart, Use: "chart", Artifact: true, ChartSource: true,
					Gate: tc.chartGate, ArtifactStages: manifest.GateArtifacts,
				})
			}
			err = waiter.Prediction(t.Context(), &reads, slices.Values(references), index)
			assert.FatalIf(t, err == nil, "expected a detached assessment failure")
			status := store.FailureStatus(err)
			if tc.blocked == (manifest.NamedResource{}) {
				assert.Equal(t, status.Outcome(), store.OutcomeFailed)
				assert.Equal(t, status.Message, `valuesFrom ConfigMap apps/values key "values.yaml" not found`)
			} else {
				assert.Equal(t, status.Outcome(), store.OutcomeBlocked)
				assert.Diff(t, status.BlockedBy(), []manifest.NamedResource{tc.blocked})
			}
			assert.Equal(t, len(st.FailedResources()), 0)
			assert.Equal(t, len(st.Warnings()), 0)
		})
	}
}
