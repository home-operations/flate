package selection

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAddEmitted_KeptChildUsesIncomingReferences(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"ancestor_only", "primary", "kept_emitter"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			parent := testutil.KustomizationID("apps", "parent")
			child := &manifest.HelmRelease{
				Name:      "app",
				Namespace: "apps",
				ChartRef: &helmv2.CrossNamespaceSourceReference{
					Kind: manifest.KindOCIRepository, Name: "new-chart", Namespace: "apps",
				},
			}
			child.Chart.Source = child.ChartSource()
			stale := child.Clone()
			stale.Chart.Source.Name = "old-chart"
			stale.ChartRef = &helmv2.CrossNamespaceSourceReference{
				Kind:      manifest.KindOCIRepository,
				Name:      "old-chart",
				Namespace: "apps",
			}
			competitor := &manifest.Kustomization{Name: "competitor", Namespace: "apps"}
			st := store.New()
			st.AddObjects([]manifest.BaseManifest{stale, competitor})
			facts := &solverFacts{
				store: st,
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					parent: {File: "parent.yaml"}, child.Named(): {
						Productions: []manifest.Production{
							{Owner: competitor.Named(), Source: manifest.BootstrapSourceID},
						},
					},
				}},
			}
			c := solverClosure(t, st, facts)
			session := c.session
			initial := &Admission{
				Needed:   []manifest.NamedResource{parent, child.Named()},
				Ordinary: []manifest.NamedResource{parent, child.Named()},
			}
			if mode != "kept_emitter" {
				initial.Selected, initial.Dirty = []manifest.NamedResource{parent}, []manifest.NamedResource{parent}
			}
			if mode == "primary" {
				initial.Selected = append(initial.Selected, child.Named())
				initial.Dirty = append(initial.Dirty, child.Named())
				initial.Needed, initial.Ordinary = append(
					initial.Needed,
					competitor.Named(),
				), append(
					initial.Ordinary,
					competitor.Named(),
				)
			}
			publishDecision(t, session, initial, 0)
			assert.Equal(t, session.Snapshot().Reconcile(child.Named()), true)
			assert.Equal(t, session.Snapshot().Reconcile(child.ChartSource()), false)
			proposal, err := prepareSolver(
				t.Context(),
				t,
				session,
				PrepareRequest{Owner: parent, Generation: []store.Child{{Obj: child}}},
			)
			assert.NoError(t, err)
			added := commitNewIdentities(t, session, proposal)
			want := []manifest.NamedResource{child.ChartSource()}
			if mode != "primary" {
				want = append(want, competitor.Named())
				slices.SortFunc(want, manifest.NamedResource.Compare)
			}
			assert.Diff(t, added, want)
			assert.Equal(
				t,
				session.Snapshot().Reconcile(competitor.Named()) && !session.Snapshot().Output(competitor.Named()),
				true,
			)
			assert.Equal(t, session.Snapshot().Reconcile(stale.ChartSource()), false)
			assert.Equal(t, session.Snapshot().Reconcile(child.ChartSource()), true)
			sourceClosure := solverClosure(t, st, facts)
			sourceClosure.generation = []store.Child{{Obj: child}}
			sourceClosure.states[child.Named()] = admissionState{incoming: 1}
			assert.Diff(
				t,
				slices.Collect(sourceClosure.sources(child.Named())),
				[]manifest.NamedResource{child.ChartSource()},
			)
			assert.Equal(
				t,
				sourceClosure.incomingContentSource(child, &manifest.DiscoveryRecord{}),
				child.ChartSource(),
			)
			sentinel := testutil.KustomizationID("apps", "sentinel")
			incoming := &manifest.Kustomization{Name: sentinel.Name, Namespace: sentinel.Namespace}
			proposal, err = prepareSolver(
				t.Context(),
				t,
				session,
				PrepareRequest{Owner: child.Named(), Generation: []store.Child{{Obj: incoming}}},
			)
			assert.NoError(t, err)
			var sentinels []manifest.NamedResource
			if mode != "kept_emitter" {
				sentinels = []manifest.NamedResource{sentinel}
			}
			assert.Diff(t, commitNewIdentities(t, session, proposal), sentinels)
			assert.Equal(t, session.Snapshot().Reconcile(sentinel), mode != "kept_emitter")
		})
	}
}

func commitNewIdentities(t *testing.T, session *Session, proposal *Admission) []manifest.NamedResource {
	t.Helper()
	before := session.Snapshot()
	_, committed, err := session.commitSolver(t.Context(), t, proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	var added []manifest.NamedResource
	for _, bucket := range session.Snapshot().buckets {
		for id := range bucket {
			if !before.Reconcile(id) {
				added = append(added, id)
			}
		}
	}
	slices.SortFunc(added, manifest.NamedResource.Compare)
	return added
}
