package selection

import (
	"context"
	"slices"
	"testing"
	"testing/fstest"

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

//nolint:gocognit // Each lineage mutation MUST exercise the same retained obligation and capture.
func TestAdmission_ProducerFrontierLineage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, beforePath, beforeFile, afterPath, afterFile string
		source, authored, declaration                      bool
		runnable, sourceTarget, replaceDeclaration         bool
		want                                               bool
	}{
		{name: "resolved path", beforeFile: "child.yaml", afterPath: "/source/child.yaml", afterFile: "child.yaml", want: true},
		{name: "diff coordinate", beforePath: "/source/child.yaml", beforeFile: "../fetched/child.yaml", afterPath: "/source/child.yaml", afterFile: "child.yaml", want: true},
		{name: "rebound path", beforePath: "/source/child.yaml", beforeFile: "child.yaml", afterPath: "/replacement/child.yaml", afterFile: "child.yaml"},
		{name: "foreign source", source: true},
		{name: "foreign source with runnable declaration", source: true, runnable: true},
		{name: "foreign authored with runnable declaration", authored: true, runnable: true},
		{name: "replaced declaration", runnable: true, replaceDeclaration: true},
		{name: "raw source file", sourceTarget: true, beforePath: "/source/artifact.yaml", afterPath: "/source/artifact.yaml", beforeFile: "artifact.yaml", afterFile: "artifact.yaml"},
		{name: "nonlocal runnable declaration", runnable: true, beforePath: "/source/generator.yaml", afterPath: "/source/generator.yaml", beforeFile: "generator.yaml", afterFile: "../outside/generator.yaml"},
		{name: "foreign authored", authored: true},
		{name: "nonlocal", beforePath: "/source/child.yaml", beforeFile: "child.yaml", afterPath: "/source/child.yaml", afterFile: "../outside/child.yaml", declaration: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "factory", Namespace: "apps"}
			st.AddObject(owner)
			consumer := (&manifest.HelmRelease{Name: "application", Namespace: "apps"}).Named()
			if tc.sourceTarget {
				consumer = (&manifest.ExternalArtifact{Name: "application", Namespace: "apps"}).Named()
			}
			generator := &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1",
				Kind:       "ArtifactGenerator",
				Name:       "generator",
				Namespace:  "apps",
			}
			if tc.runnable {
				st.AddObject(generator)
			}
			production := manifest.Production{
				Owner: owner.Named(), Source: manifest.BootstrapSourceID, Path: tc.beforePath, File: tc.beforeFile,
			}
			if tc.declaration {
				production.Declaration = consumer
			}
			if tc.runnable {
				production.Declaration = generator.Named()
			}
			current := production
			current.Path, current.File = tc.afterPath, tc.afterFile
			if tc.replaceDeclaration {
				current.Declaration.Name = "replacement"
			}
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
			if tc.runnable {
				assert.Equal(t, slices.Contains(proposal.Needed, generator.Named()), false)
				assert.Equal(t, slices.Contains(slices.Collect(proposal.reads.Inputs()), generator.Named()), false)
			}
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

func TestAdmission_ProjectedProductionLifetime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, namespace  string
		output, complete bool
		nonmatching      bool
		foreignNamespace bool
		want             bool
	}{
		{name: "indexed", want: true},
		{name: "partial", output: true, want: true},
		{name: "partial exact namespace", namespace: "apps", output: true, want: true},
		{name: "complete", output: true, complete: true, want: true},
		{name: "complete nonmatching", output: true, complete: true, nonmatching: true},
		{name: "foreign namespace", foreignNamespace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", TargetNamespace: "apps"}
			if tc.foreignNamespace {
				producer.TargetNamespace = "other"
			}
			st.AddObject(producer)
			authored := &manifest.OCIRepository{Name: "shared", Namespace: tc.namespace}
			target := authored.WithNamespace("apps").Named()
			production := manifest.Production{
				Owner: producer.Named(), Source: manifest.BootstrapSourceID,
				Authored: authored.Named(), File: "source.yaml",
			}
			occurrence := applicationOccurrence(authored, t.TempDir())
			occurrence.Identity = target
			facts := &solverFacts{
				store: st,
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					producer.Named(): {Path: "producer.yaml", File: "producer.yaml"},
					authored.Named(): {
						Path: "source.yaml", File: "source.yaml", Authored: authored.Named(),
						Productions: []manifest.Production{production},
					},
				}},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{target: occurrence},
			}
			if tc.output {
				var children []store.Child
				if tc.complete && !tc.nonmatching {
					children = []store.Child{{Obj: authored.WithNamespace(target.Namespace)}}
				}
				st.RecordEmitted(producer.Named(), &store.Emission{Children: children, Complete: tc.complete}, nil)
				st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
			}
			closure := solverClosure(t, st, facts)
			assert.Equal(t, slices.ContainsFunc(closure.productions(target), func(p manifest.Production) bool {
				return p.Owner == producer.Named()
			}), tc.want)
		})
	}
}

//nolint:gocognit // Source readiness and producer lifetime MUST share one retained obligation across the state table.
func TestAdmission_ProducerFrontierGeneratedSource(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"index-only", "unpublished declaration", "completed parent", "partial parent", "declaration without content", "metadata without files", "available", "failed generator", "withdrawn", "replacement"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "factory"}
			generator := &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1",
				Kind:       "ArtifactGenerator",
				Name:       "generator",
				Namespace:  "apps",
			}
			target := (&manifest.ExternalArtifact{Name: "bundle", Namespace: "apps"}).Named()
			st.AddObject(parent)
			production := manifest.Production{
				Owner:       parent.Named(),
				Declaration: generator.Named(),
				Authored:    generator.Named(),
				Source:      manifest.BootstrapSourceID,
				Path:        "/source/factory/generator.yaml",
				File:        "factory/generator.yaml",
			}
			facts := &solverFacts{
				store: st,
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					target: {Productions: []manifest.Production{production}},
					generator.Named(): {
						Path:        production.Path,
						File:        production.File,
						Declaration: generator,
						Parent:      parent.Named(),
					},
				}},
			}
			session, err := New(st, facts, Request{Changed: true})
			assert.NoError(t, err)
			preparation := &Preparation{closure: admissionClosure{session: session, facts: facts}}
			defer preparation.Retire()
			previous := &Admission{preparation: preparation}
			closure := admissionClosure{session: session, facts: facts, proposal: previous, objects: st}
			closure.requestProducer(target, &production)
			accepted := previous.result()
			previous.Retire()
			assert.Equal(t, st.GetDeclaration(generator.Named()), manifest.BaseManifest(nil))
			assert.Equal(t, production.Materializer(target), parent.Named())
			physical := state == "index-only" || state == "unpublished declaration"
			if state == "unpublished declaration" {
				record := facts.index.Records[generator.Named()]
				record.BuilderOwner, record.Builder = parent.Named(), "factory/kustomization.yaml"
				record.Authored = generator.Named()
				record.Authored.Namespace = ""
				record.Productions = []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}}
				facts.index.Records[generator.Named()] = record
				st.SetDiscoveryIndex(&facts.index)
				st.AddObject(generator)
				assert.Equal(t, st.GetObject(generator.Named()), manifest.BaseManifest(generator))
				assert.Equal(t, st.GetDeclaration(generator.Named()), manifest.BaseManifest(nil))
			}
			if !physical {
				st.RecordEmitted(
					parent.Named(),
					&store.Emission{
						Complete:    state != "partial parent",
						Fingerprint: "initial",
						Children:    []store.Child{{Obj: generator, Generated: true}},
					},
					nil,
				)
			}
			switch state {
			case "declaration without content":
				st.AddObject(&manifest.ExternalArtifact{Name: target.Name, Namespace: target.Namespace})
			case "metadata without files":
				st.SetArtifact(target, &store.SourceArtifact{Content: "metadata"})
			case "available":
				st.SetArtifact(target, &store.SourceArtifact{Content: "content", FS: fstest.MapFS{}})
			case "failed generator":
				st.UpdateStatus(generator.Named(), store.StatusInfo{Status: store.StatusFailed})
			case "withdrawn":
				facts.index.Records[target] = manifest.DiscoveryRecord{}
			}
			proposal, err := session.RefreshProducers(t.Context(), &accepted, PrepareRequest{Preparation: &preparation})
			assert.NoError(t, err)
			defer proposal.Retire()
			demands := slices.Collect(proposal.Demands())
			execution := !physical && state != "available" && state != "failed generator" && state != "withdrawn"
			assert.Equal(t, len(demands), map[bool]int{false: 0, true: 1}[physical])
			for _, demand := range demands {
				assert.Equal(t, demand.Identity, parent.Named())
				assert.Equal(t, manifest.KindOf(demand.Witness).Structural, true)
			}
			assert.Equal(t, len(proposal.Needed), 0)
			assert.Equal(t, len(proposal.Ordinary), 0)
			assert.Equal(t, len(proposal.producers) != 0, physical || execution)
			if state == "declaration without content" {
				var required store.ReadSet
				required.Init(st, manifest.NamedResource{}, nil)
				defer required.Retire()
				required.Require(manifest.DependencyRef{NamedResource: target, Artifact: true})
				var publication *Preparation
				defer func() { publication.Retire() }()
				ordinary, ordinaryErr := session.prepareTest(
					t.Context(),
					PrepareRequest{Reads: &required, Preparation: &publication},
				)
				assert.NoError(t, ordinaryErr)
				defer ordinary.Retire()
				assert.Equal(t, slices.ContainsFunc(ordinary.producers, func(obligation producerObligation) bool {
					return obligation.consumer == target && obligation.production == production
				}), true)
			}
			captured := proposal.reads
			snapshot := session.Snapshot()
			_, committed, err := session.publishProposal(t.Context(), proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, false)
			assert.Equal(t, session.Snapshot() == snapshot, true)
			if state == "replacement" {
				replacement := *generator
				replacement.Spec = map[string]any{
					"sources": []any{map[string]any{"kind": "GitRepository", "name": "replacement"}},
				}
				st.RecordEmitted(
					parent.Named(),
					&store.Emission{
						Complete:    true,
						Fingerprint: "replacement",
						Children:    []store.Child{{Obj: &replacement, Generated: true}},
					},
					nil,
				)
				assert.Equal(t, st.GetObject(generator.Named()) == &replacement, true)
				assert.Equal(t, captured.Valid(), false)
			}
		})
	}
}
