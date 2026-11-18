package selection

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
			for _, object := range []manifest.BaseManifest{stale, competitor} {
				st.AddObject(object)
			}
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
			oldRepository := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmRepository,
				Namespace: "apps",
				Name:      "old-repository",
			}
			facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{
				child.Named(): {
					Identity:     child.Named(),
					Object:       stale,
					Catalog:      stale,
					CatalogKnown: true,
					Refs: []manifest.Reference{
						{NamedResource: oldRepository, Content: true},
					},
					Reverse: []manifest.NamedResource{oldRepository},
				},
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
			assert.Equal(t, session.Snapshot().Reconcile(oldRepository), false)
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

func TestReferences_IncomingRetiresProjectedInputs(t *testing.T) {
	t.Parallel()
	old := &manifest.HelmRelease{Name: "release", Namespace: "apps"}
	replacement := old.Clone()
	replacement.Values = map[string]any{"changed": true}
	repository := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmRepository,
		Namespace: "apps",
		Name:      "old",
	}
	ref := manifest.Reference{NamedResource: repository, Content: true}
	facts := &solverFacts{
		index: manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{old.Named(): {Declaration: old}},
		},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{
			old.Named(): {
				Identity:     old.Named(),
				Object:       old,
				Catalog:      old,
				CatalogKnown: true,
				Refs:         []manifest.Reference{ref},
				Reverse:      []manifest.NamedResource{repository},
			},
		},
	}
	closure := solverClosure(t, store.New(), facts)
	assert.Equal(t, closure.reverse(old.Named(), old, &ref), true)
	closure.generation = []store.Child{{Obj: replacement}}
	closure.states[old.Named()] = admissionState{incoming: 1}
	assert.Equal(
		t,
		slices.ContainsFunc(
			slices.Collect(closure.references(old.Named(), replacement)),
			func(r manifest.Reference) bool { return r.NamedResource == repository },
		),
		false,
	)
	assert.Equal(t, closure.reverse(old.Named(), replacement, &ref), false)
	currentRepository := repository
	currentRepository.Name = "current"
	current := manifest.Reference{NamedResource: currentRepository, Content: true}
	facts.occurrences[old.Named()] = OccurrenceFacts{
		Identity: old.Named(), Object: replacement, Catalog: replacement, CatalogKnown: true,
		Refs: []manifest.Reference{current}, Reverse: []manifest.NamedResource{currentRepository},
	}
	facts.index.Records[old.Named()] = manifest.DiscoveryRecord{Declaration: replacement}
	closure = solverClosure(t, store.New(), facts)
	assert.Equal(
		t,
		slices.ContainsFunc(
			slices.Collect(closure.references(old.Named(), replacement)),
			func(r manifest.Reference) bool { return r.NamedResource == currentRepository },
		),
		true,
	)
	assert.Equal(t, closure.reverse(old.Named(), replacement, &current), true)
	assert.Equal(t, closure.reverse(old.Named(), replacement, &ref), false)
}

func TestReferences_EarlyStop(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"current", "projected", "indexed", "historical"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			object := &manifest.HelmRelease{Name: "release", Namespace: "apps"}
			first := manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "first"}
			second := first
			second.Name = "second"
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}
			var current manifest.BaseManifest = object
			want := 4
			switch mode {
			case "current":
				object.ValuesFrom = []helmv2.ValuesReference{
					{Kind: first.Kind, Name: first.Name}, {Kind: second.Kind, Name: second.Name},
				}
			case "projected":
				facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{object.Named(): {
					Identity:     object.Named(),
					Object:       object,
					CatalogKnown: true,
					Refs: []manifest.Reference{
						{NamedResource: first, Content: true},
						{NamedResource: second, Content: true},
					},
				}}
			case "indexed":
				current = nil
				facts.index.Records[object.Named()] = manifest.DiscoveryRecord{
					Refs: []manifest.NamedResource{first, second},
				}
			case "historical":
				want = 2
			}
			closure := solverClosure(t, store.New(), facts)
			owner := testutil.KustomizationID("apps", "owner")
			closure.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner: {object.Named(): {Refs: []manifest.NamedResource{first, second}}},
			}
			assert.Equal(t, len(slices.Collect(closure.references(object.Named(), current))), want)
			calls := 0
			closure.references(object.Named(), current)(func(manifest.Reference) bool {
				calls++
				return false
			})
			assert.Equal(t, calls, 1)
		})
	}
}
