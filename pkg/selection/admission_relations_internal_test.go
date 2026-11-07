package selection

import (
	"context"
	"fmt"
	"sync"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestFilter_SharedInputRelationLookupBounded(t *testing.T) {
	t.Parallel()
	st := store.New()
	facts := &solverFacts{
		store:   st,
		changes: change.NewSet([]string{"apps/app-7/ks.yaml"}),
		index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
	}
	facts.context.Root = "/repo"
	for i := range 100 {
		object := &manifest.Kustomization{
			Name:      fmt.Sprintf("app-%d", i),
			Namespace: "flux-system",
			Path:      fmt.Sprintf("./apps/app-%d", i),
		}
		object.SourceRef.Kind, object.SourceRef.Name, object.SourceRef.Namespace = manifest.KindGitRepository, "flux-system", "flux-system"
		st.AddObject(object)
		facts.index.Records[object.Named()] = manifest.DiscoveryRecord{File: fmt.Sprintf("apps/app-%d/ks.yaml", i)}
	}
	counted := &countedProductions{Store: st, id: manifest.BootstrapSourceID}
	c := solverClosure(t, st, facts)
	c.objects = counted
	_, err := evaluateSolver(t, &c, &PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, c.role(testutil.KustomizationID("flux-system", "app-7")).needed, true)
	assert.Equal(t, c.role(testutil.KustomizationID("flux-system", "app-8")).needed, false)
	assert.Equal(t, counted.stored, 1)
}

func TestAddEmitted_PrimaryAdmissionReadsRelationOnce(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"primary_parent", "primary_child", "primary_input"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			parent := testutil.KustomizationID("apps", "parent")
			child := &manifest.Kustomization{
				Name:      "child",
				Namespace: "apps",
				Path:      "unrelated",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository,
					Name: "incoming",
				},
			}
			st := &countedProductions{Store: store.New(), id: child.Named()}
			facts := &solverFacts{
				store:   st.Store,
				changes: change.NewSet([]string{"parent.yaml"}),
				index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}},
				},
			}
			c := solverClosure(t, st.Store, facts)
			selected := parent
			switch mode {
			case "primary_child":
				selected = child.Named()
				parent = testutil.KustomizationID("apps", "dependency")
			case "primary_input":
				selected = child.SourceID()
				parent = testutil.KustomizationID("apps", "dependency")
			}
			publishDecision(
				t,
				c.session,
				new(
					Admission{
						Needed:   []manifest.NamedResource{selected},
						Selected: []manifest.NamedResource{selected},
						Dirty:    []manifest.NamedResource{selected},
						Ordinary: []manifest.NamedResource{selected},
					},
				),
				0,
			)
			c.objects = st
			c.generation = []store.Child{{Obj: child, Productions: []manifest.Production{{Owner: parent}}}}
			_, err := evaluateSolver(t, &c, &PrepareRequest{})
			assert.NoError(t, err)
			assert.Equal(t, c.role(child.Named()).needed, true)
			assert.Equal(t, c.role(child.SourceID()).needed, true)
			assert.Equal(t, st.reads, 1)
		})
	}
}

func TestAddEmitted_PromotionDuringEvaluation(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := &manifest.ConfigMap{Name: "target", Namespace: "apps"}
	primary := &manifest.Kustomization{
		Name:      "primary",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "settings"},
				{Kind: manifest.KindConfigMap, Name: target.Name},
			},
		},
	}
	dependency := testutil.KustomizationID("apps", "dependency")
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	child := &manifest.Kustomization{
		Name:      "late",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: target.Name}},
		},
	}
	st.AddObject(primary)
	objects := &pausedIncoming{Store: st, id: child.Named(), read: make(chan struct{}), release: make(chan struct{})}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		primary.Named(): {File: "primary.yaml"}, settings: {Productions: []manifest.Production{{Owner: dependency}}},
	}}
	facts := &solverFacts{store: st, changes: change.NewSet([]string{"primary.yaml"}), index: *index}
	facts.context.Root = "/repo"
	c := solverClosure(t, st, facts)
	session := c.session
	publishDecision(
		t,
		session,
		new(
			Admission{
				Needed:   []manifest.NamedResource{primary.Named(), dependency, target.Named(), settings},
				Selected: []manifest.NamedResource{primary.Named()},
				Dirty:    []manifest.NamedResource{primary.Named()},
				Ordinary: []manifest.NamedResource{primary.Named(), dependency, target.Named(), settings},
			},
		),
		0,
	)
	assert.Equal(t, session.Snapshot().Reconcile(dependency), true)
	assert.Equal(t, session.Snapshot().Reconcile(target.Named()) && !session.Snapshot().Output(target.Named()), true)
	slots := task.NewBounded(2)
	slots.Go(
		t.Context(),
		"admit",
		func(ctx context.Context) {
			c.objects = objects
			c.generation = []store.Child{{Obj: child, Productions: []manifest.Production{{Owner: dependency}}}}
			proposal, err := evaluateSolver(t, &c, &PrepareRequest{})
			assert.NoError(t, err)
			_, committed, err := session.commitSolver(ctx, t, proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
		},
	)
	slots.Go(t.Context(), "promote", func(context.Context) {
		<-objects.read
		publishDecision(
			t,
			session,
			new(
				Admission{
					Needed:   []manifest.NamedResource{target.Named()},
					Selected: []manifest.NamedResource{target.Named()},
					Dirty:    []manifest.NamedResource{target.Named()},
					Ordinary: []manifest.NamedResource{target.Named()},
				},
			),
			session.Snapshot().version(),
		)
		close(objects.release)
	})
	slots.BlockTillDone()
	assert.Equal(t, slots.Failures(), 0)
	assert.Equal(t, session.Snapshot().Reconcile(child.Named()), true)
	assert.Equal(t, session.Snapshot().roles(target.Named()), session.Snapshot().roles(primary.Named()))
	assert.Equal(t, session.Snapshot().Reconcile(target.Named()), true)
}

type countedProductions struct {
	*store.Store

	id            manifest.NamedResource
	reads, stored int
}

func (c *countedProductions) Productions(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) []manifest.Production {
	if id == c.id {
		c.stored++
	}
	return c.Store.Productions(id, index)
}

//nolint:gocritic // The reader contract transfers an immutable child snapshot by value.
func (c *countedProductions) IncomingProductions(
	child store.Child,
	index *manifest.DiscoveryIndex,
) []manifest.Production {
	if child.Obj.Named() == c.id {
		c.reads++
	}
	return c.Store.IncomingProductions(child, index)
}

type pausedIncoming struct {
	*store.Store

	id            manifest.NamedResource
	once          sync.Once
	read, release chan struct{}
}

//nolint:gocritic // The reader contract transfers an immutable child snapshot by value.
func (p *pausedIncoming) IncomingProductions(child store.Child, index *manifest.DiscoveryIndex) []manifest.Production {
	if child.Obj.Named() == p.id {
		p.once.Do(func() { close(p.read); <-p.release })
	}
	return p.Store.IncomingProductions(child, index)
}
