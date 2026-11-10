package selection

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ProducerFrontierRequiresCurrentRelation(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"pending", "declaration", "built", "renamed producer", "withdrawn", "renamed consumer", "available"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			raw := &manifest.Kustomization{Name: "factory", Namespace: "apps", Path: "raw"}
			current := raw.Clone()
			current.Path = "transformed"
			consumer := (&manifest.HelmRelease{Name: "application", Namespace: "apps"}).Named()
			production := manifest.Production{Owner: raw.Named(), Source: manifest.BootstrapSourceID}
			facts := &solverFacts{store: st, index: manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					consumer: {Productions: []manifest.Production{production}},
				},
			}, occurrences: map[manifest.NamedResource]OccurrenceFacts{raw.Named(): {Object: raw, Catalog: raw}}}
			session, err := New(st, facts, Request{Changed: true})
			assert.NoError(t, err)
			preparation := &Preparation{closure: admissionClosure{session: session, facts: facts}}
			defer preparation.Retire()
			previous := &Admission{preparation: preparation}
			closure := admissionClosure{session: session, facts: facts, proposal: previous, objects: st}
			closure.requestProducer(consumer, &production)
			assert.Equal(t, len(slices.Collect(previous.Demands())), 0)
			accepted := previous.result()
			previous.Retire()

			want := false
			switch route {
			case "declaration":
				st.AddObject(current)
				want = true
			case "built":
				facts.index.Records[raw.Named()] = manifest.DiscoveryRecord{Builder: "native"}
				facts.roots = map[manifest.NamedResource]RootEvidence{raw.Named(): {
					BuiltCurrent: true, BuiltChild: store.Child{Obj: current},
				}}
				want = true
			case "renamed producer":
				current.Name = "transformed-factory"
				st.AddObject(current)
			case "withdrawn":
				st.AddObject(current)
				facts.index.Records[consumer] = manifest.DiscoveryRecord{}
			case "renamed consumer":
				st.AddObject(current)
				replacement := consumer
				replacement.Name = "transformed-application"
				facts.index.Records[replacement] = facts.index.Records[consumer]
				delete(facts.index.Records, consumer)
			case "available":
				st.AddObject(current)
				st.AddObject(&manifest.HelmRelease{Name: consumer.Name, Namespace: consumer.Namespace})
			}
			request := PrepareRequest{Preparation: &preparation}
			proposal, err := session.RefreshProducers(t.Context(), &accepted, request)
			assert.NoError(t, err)
			demands := slices.Collect(proposal.Demands())
			assert.Equal(t, len(demands) != 0, want)
			if want {
				assert.Equal(t, len(demands), 1)
				assert.Equal(t, demands[0].Identity, current.Named())
				assert.Equal(t, demands[0].Witness == current, true)
				assert.Equal(t, demands[0].Reads.Valid(), true)
			}
			frontierResult := proposal.result()
			snapshot := session.Snapshot()
			_, committed, err := session.publishProposal(t.Context(), proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, false)
			assert.Equal(t, session.Snapshot() == snapshot, true)
			next, err := session.RefreshProducers(t.Context(), &frontierResult, request)
			assert.NoError(t, err)
			next.Retire()
			if route == "declaration" {
				assert.Equal(t, demands[0].Reads.Valid(), true)
				replacement := current.Clone()
				replacement.Path = "replaced"
				st.AddObject(replacement)
				assert.Equal(t, demands[0].Reads.Valid(), false)
			}
		})
	}
}

func TestAdmission_ProducerFrontierLineage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, beforePath, beforeFile, afterPath, afterFile string
		source, authored, declaration                      bool
		want                                               bool
	}{
		{name: "resolved path", beforeFile: "child.yaml", afterPath: "/source/child.yaml", afterFile: "child.yaml", want: true},
		{name: "diff coordinate", beforePath: "/source/child.yaml", beforeFile: "../fetched/child.yaml", afterPath: "/source/child.yaml", afterFile: "child.yaml", want: true},
		{name: "rebound path", beforePath: "/source/child.yaml", beforeFile: "child.yaml", afterPath: "/replacement/child.yaml", afterFile: "child.yaml"},
		{name: "foreign source", source: true},
		{name: "foreign authored", authored: true},
		{name: "nonlocal", beforePath: "/source/child.yaml", beforeFile: "child.yaml", afterPath: "/source/child.yaml", afterFile: "../outside/child.yaml", declaration: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "factory", Namespace: "apps"}
			st.AddObject(owner)
			consumer := (&manifest.HelmRelease{Name: "application", Namespace: "apps"}).Named()
			production := manifest.Production{
				Owner: owner.Named(), Source: manifest.BootstrapSourceID, Path: tc.beforePath, File: tc.beforeFile,
			}
			if tc.declaration {
				production.Declaration = consumer
			}
			current := production
			current.Path, current.File = tc.afterPath, tc.afterFile
			if tc.source {
				current.Source.Name = "replacement"
			}
			if tc.authored {
				current.Authored = owner.Named()
			}
			facts := &solverFacts{store: st, index: manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					consumer: {Productions: []manifest.Production{current}},
				},
			}}
			session, err := New(st, facts, Request{Changed: true})
			assert.NoError(t, err)
			preparation := &Preparation{closure: admissionClosure{session: session, facts: facts}}
			defer preparation.Retire()
			previous := &Admission{preparation: preparation}
			closure := admissionClosure{session: session, facts: facts, proposal: previous, objects: st}
			closure.requestProducer(consumer, &production)
			accepted := previous.result()
			previous.Retire()
			proposal, err := session.RefreshProducers(t.Context(), &accepted, PrepareRequest{Preparation: &preparation})
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, len(slices.Collect(proposal.Demands())) != 0, tc.want)
		})
	}
}

func TestAdmission_ProducerFrontierPreparation(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"foreign session", "foreign logical owner", "nil previous"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			facts := &solverFacts{store: st}
			session, err := New(st, facts, Request{Changed: true})
			assert.NoError(t, err)
			preparation := &Preparation{closure: admissionClosure{session: session, facts: facts}}
			defer preparation.Retire()
			previous := &Admission{preparation: preparation}
			closure := admissionClosure{session: session, facts: facts, proposal: previous, objects: st}
			owner := (&manifest.Kustomization{Name: "factory", Namespace: "apps"}).Named()
			consumer := (&manifest.HelmRelease{Name: "application", Namespace: "apps"}).Named()
			closure.requestProducer(consumer, &manifest.Production{Owner: owner})
			accepted := previous.result()
			previous.Retire()
			request := PrepareRequest{Preparation: &preparation}
			acceptedResult := &accepted
			caller := session
			switch route {
			case "foreign session":
				foreign, foreignErr := New(st, facts, Request{Changed: true})
				assert.NoError(t, foreignErr)
				caller = foreign
			case "foreign logical owner":
				foreign := &Preparation{closure: admissionClosure{session: session, facts: facts}}
				defer foreign.Retire()
				request.Preparation = &foreign
			case "nil previous":
				acceptedResult = nil
			}
			proposal, err := caller.RefreshProducers(t.Context(), acceptedResult, request)
			assert.Equal(t, err != nil, true)
			assert.Equal(t, proposal == nil, true)
		})
	}
}

func TestAdmission_ProducerFrontierEmptyStages(t *testing.T) {
	t.Parallel()
	session := newSolverSession(t, store.New(), nil, Request{Changed: true})
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	request := PrepareRequest{Preparation: &preparation}
	initial, err := session.prepareTest(t.Context(), request)
	assert.NoError(t, err)
	initialResult := initial.result()
	initial.Retire()
	assert.Equal(t, len(initial.producers), 0)
	late := &Admission{preparation: preparation}
	closure := admissionClosure{session: session, facts: session.facts, proposal: late}
	owner := (&manifest.Kustomization{Name: "factory", Namespace: "apps"}).Named()
	consumer := (&manifest.HelmRelease{Name: "application", Namespace: "apps"}).Named()
	closure.requestProducer(consumer, &manifest.Production{Owner: owner})
	late.Retire()
	assert.Equal(t, preparation.producerOwner != nil, true)
	frontier, err := session.RefreshProducers(t.Context(), &initialResult, request)
	assert.NoError(t, err)
	assert.Equal(t, len(slices.Collect(frontier.Demands())), 0)
	frontier.Retire()

	current, err := session.prepareTest(t.Context(), request)
	assert.NoError(t, err)
	assert.Equal(t, len(current.producers), 0)
	assert.Equal(t, current.producerOwner == preparation.producerOwner, true)
	currentResult := current.result()
	current.Retire()
	frontier, err = session.RefreshProducers(t.Context(), &currentResult, request)
	assert.NoError(t, err)
	assert.Equal(t, len(slices.Collect(frontier.Demands())), 0)
	frontier.Retire()
}

type frontierDerivationFacts struct {
	*solverFacts

	afterDerivation func()
}

func (f *frontierDerivationFacts) SourceFacts(id manifest.NamedResource, reads *store.ReadSet) SourceFacts {
	facts := f.solverFacts.SourceFacts(id, reads)
	if facts.Derived && f.afterDerivation != nil {
		f.afterDerivation()
	}
	return facts
}

func TestAdmission_ProducerFrontierSourceDerivation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		warm, canceled bool
	}{
		{name: "cold"}, {name: "warm", warm: true}, {name: "canceled", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			st := store.New()
			changes := change.NewSet(nil)
			facts := &frontierDerivationFacts{solverFacts: &solverFacts{store: st, changes: changes}}
			session, err := New(st, facts, Request{Changed: true})
			assert.NoError(t, err)
			if tc.warm {
				var reads store.ReadSet
				reads.Init(st, manifest.NamedResource{}, nil)
				facts.SourceFacts(manifest.BootstrapSourceID, &reads)
				reads.Close()
			}
			if tc.canceled {
				facts.afterDerivation = cancel
			}
			var work manifest.WorkRecorder
			st.SetWorkTree(manifest.TreeID{}, work.Scope())
			preparation := &Preparation{closure: admissionClosure{session: session, facts: facts}}
			defer preparation.Retire()
			previous := &Admission{preparation: preparation}
			closure := admissionClosure{session: session, facts: facts, proposal: previous}
			owner := (&manifest.Kustomization{Name: "factory", Namespace: "apps"}).Named()
			consumer := (&manifest.HelmRelease{Name: "application", Namespace: "apps"}).Named()
			closure.requestProducer(consumer, &manifest.Production{Owner: owner})
			if tc.canceled {
				owner.Name = "next-factory"
				closure.requestProducer(consumer, &manifest.Production{Owner: owner})
			}
			accepted := previous.result()
			previous.Retire()
			snapshot := session.Snapshot()
			proposal, err := session.RefreshProducers(ctx, &accepted, PrepareRequest{
				Preparation: &preparation, Changes: changes,
			})
			want := manifest.WorkCounts{}
			if tc.canceled {
				assert.Equal(t, err, context.Canceled)
				assert.Equal(t, proposal == nil, true)
				want.Attempts, want.Failures, want.OwnerPrepares = 1, 1, 1
			} else {
				assert.NoError(t, err)
				defer proposal.Retire()
				assert.Equal(t, len(slices.Collect(proposal.Demands())), 0)
				if !tc.warm {
					want.Attempts, want.Completed, want.OwnerPrepares = 1, 1, 1
				}
			}
			assert.Equal(t, session.Snapshot() == snapshot, true)
			assert.Diff(t, work.Scope().Admission().Counts, want)
		})
	}
}
