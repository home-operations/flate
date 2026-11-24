package depwait_test

import (
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/admissiontest"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAssess_OrderingProducerPublication(t *testing.T) {
	t.Parallel()
	for _, republish := range []bool{false, true} {
		name := "omitted"
		if republish {
			name = "republished"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			app := &manifest.Kustomization{
				Name: "app", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: "middle"}},
			}
			middle := &manifest.Kustomization{
				Name: "middle", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: "leaf"}},
			}
			leaf := &manifest.Kustomization{Name: "leaf", Namespace: "apps"}
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			for _, obj := range []manifest.BaseManifest{app, middle, producer} {
				st.AddObject(obj)
			}
			id, owner := leaf.Named(), producer.Named()
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{{Owner: owner}}},
			}}
			st.SetDiscoveryIndex(index)
			_, session := admissiontest.New(t, admissiontest.Config{
				Inventory: st, Index: index, Request: selection.Request{Changed: true},
			})
			admissiontest.Require(t, st, session, []manifest.NamedResource{owner}, false)
			st.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: leaf}}, Complete: true}, nil)
			assert.Equal(t, st.GetDeclaration(id), manifest.BaseManifest(leaf))
			st.RecordEmitted(owner, &store.Emission{Complete: true}, nil)
			st.UpdateStatus(owner, store.StatusInfo{Status: store.StatusPending})
			assert.Equal(t, st.GetDeclaration(id), nil)

			var reads store.ReadSet
			reads.Init(st, app.Named(), nil)
			defer reads.Retire()
			for _, gate := range depwait.OrderingGates(app, &reads) {
				reads.Require(gate)
			}
			waiter := depwait.Waiter{
				Store: &reads, Parent: app.Named(), Missing: depwait.NewMissingPolicy(nil),
				Decision: session.Snapshot(),
			}
			err := waiter.Assess(t.Context(), &reads, manifest.DrainCascade, depwait.AssessGates)
			blocked, ok := errors.AsType[*depwait.BlockedError](err)
			assert.FatalIf(t, !ok, "expected nested wait, got %v", err)
			assert.Diff(t, blocked.Deps, []manifest.NamedResource{id, middle.Named(), owner})
			assert.Equal(t, reads.Valid(), true)

			done := make(chan struct{})
			go func() {
				defer close(done)
				next := store.Emission{Complete: true}
				if republish {
					next.Children = []store.Child{{Obj: leaf}}
				}
				st.RecordEmitted(owner, &next, nil)
				st.UpdateStatus(owner, store.StatusInfo{Status: store.StatusReady})
			}()
			<-done
			assert.Equal(t, reads.Valid(), false)

			var current store.ReadSet
			current.Init(st, app.Named(), nil)
			defer current.Retire()
			for _, gate := range depwait.OrderingGates(app, &current) {
				current.Require(gate)
			}
			waiter.Store = &current
			err = waiter.Assess(t.Context(), &current, manifest.DrainCascade, depwait.AssessGates)
			if republish {
				assert.NoError(t, err)
				assert.Equal(t, current.GetDeclaration(id), manifest.BaseManifest(leaf))
				return
			}
			failure, ok := errors.AsType[*manifest.DependencyFailedError](err)
			assert.FatalIf(t, !ok, "expected missing leaf failure, got %v", err)
			assert.Diff(t, failure.Failed, []manifest.NamedResource{middle.Named()})
			assert.Diff(t, failure.Reasons[middle.Named()].Causes, []manifest.DiagnosticReason{{
				Subject: manifest.DiagnosticFor(id, nil), Message: "dependency not found",
			}})
		})
	}
}

func TestAssess_OrderingExpressionsAndCycles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		cycle bool
	}{
		{name: "shared prerequisite with distinct self operands"},
		{name: "evaluation-only cycle", cycle: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			app := &manifest.Kustomization{
				Name: "app", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: "first"}, {Name: "second"}},
			}
			const ready = "self.metadata.name == 'first' && dep.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')"
			first := &manifest.Kustomization{
				Name: "first", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: "leaf", ReadyExpr: ready}},
			}
			second := &manifest.Kustomization{
				Name: "second", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: "leaf", ReadyExpr: ready}},
			}
			leaf := &manifest.Kustomization{Name: "leaf", Namespace: "apps"}
			if tc.cycle {
				app.DependsOn = app.DependsOn[:1]
				first.DependsOn[0].ReadyExpr = ""
				leaf.DependsOn = []kustomizev1.DependencyReference{{Name: first.Name}}
			}
			for _, obj := range []manifest.BaseManifest{app, first, second, leaf} {
				st.AddObject(obj)
			}
			session, err := selection.New(st, nil, selection.Request{Changed: true})
			assert.NoError(t, err)
			var reads store.ReadSet
			reads.Init(st, app.Named(), nil)
			defer reads.Retire()
			for _, gate := range depwait.OrderingGates(app, &reads) {
				reads.Require(gate)
			}
			waiter := depwait.Waiter{
				Store: &reads, Parent: app.Named(), Missing: depwait.NewMissingPolicy(nil),
				Decision: session.Snapshot(),
			}
			err = waiter.Assess(t.Context(), &reads, manifest.DrainCascade, depwait.AssessGates)
			if tc.cycle {
				blocked, ok := errors.AsType[*depwait.BlockedError](err)
				assert.FatalIf(t, !ok, "expected a wait at the cycle, got %v", err)
				assert.Diff(t, blocked.Deps, []manifest.NamedResource{first.Named(), leaf.Named()})
				err = waiter.Assess(t.Context(), &reads, manifest.DrainForce, depwait.AssessGates)
				failure, ok := errors.AsType[*manifest.DependencyFailedError](err)
				assert.FatalIf(t, !ok, "expected structural drain to settle the cycle, got %v", err)
				assert.Diff(t, failure.Failed, []manifest.NamedResource{first.Named()})
				return
			}
			failure, ok := errors.AsType[*manifest.DependencyFailedError](err)
			assert.FatalIf(t, !ok, "expected the second consumer's expression to fail, got %v", err)
			assert.Diff(t, failure.Failed, []manifest.NamedResource{second.Named()})
			assert.Diff(t, failure.Reasons[second.Named()].Causes, []manifest.DiagnosticReason{
				{
					Subject: manifest.DiagnosticFor(
						leaf.Named(),
						leaf,
					),
					Message: "readyExpr timeout: context deadline exceeded",
				},
			})
		})
	}
}

func TestAssess_OrderingExpressionProducerPublication(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	app := testutil.KustomizationID("apps", "app")
	reader := testutil.KustomizationID("apps", "reader")
	dependency := testutil.KustomizationID("apps", "dependency")
	readerOwner := testutil.KustomizationID("apps", "reader-owner")
	dependencyOwner := testutil.KustomizationID("apps", "dependency-owner")
	files := fstest.MapFS{
		"flux/app.yaml": {Data: []byte(testutil.KustomizationYAML(app.Name, app.Namespace,
			"  dependsOn: [{name: reader}]\n"))},
		"flux/reader.yaml": {Data: []byte(testutil.KustomizationYAML(readerOwner.Name, readerOwner.Namespace,
			"  path: ./reader\n"))},
		"flux/dep.yaml": {Data: []byte(testutil.KustomizationYAML(dependencyOwner.Name, dependencyOwner.Namespace,
			"  path: ./dep\n"))},
		"reader/kustomization.yaml": {Data: []byte("resources: [reader.yaml]\n")},
		"dep/kustomization.yaml":    {Data: []byte("resources: [dependency.yaml]\n")},
		"reader/reader.yaml": {Data: []byte(testutil.KustomizationYAML(reader.Name, reader.Namespace, `  interval: 1m
  dependsOn:
    - name: dependency
      readyExpr: self.spec.interval == '1m0s' && dep.metadata.name == 'dependency'
`))},
		"dep/dependency.yaml": {Data: []byte(testutil.KustomizationYAML(dependency.Name, dependency.Namespace,
			"  interval: 1m\n"))},
	}
	previous := fstest.MapFS{}
	old := &store.SourceArtifact{FS: previous}
	current := &store.SourceArtifact{LocalPath: root, FS: files}
	for file, entry := range files {
		testutil.WriteFile(t, root, file, string(entry.Data))
		if file != "flux/app.yaml" {
			previous[file] = entry
		}
	}
	changes, err := change.Detect(t.Context(), old, current, change.DetectConfig{})
	assert.NoError(t, err)
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, current)
	l, session := admissiontest.New(t, admissiontest.Config{
		Changes: changes, Inventory: st, Root: root, Request: selection.Request{Changed: true},
	})
	_, err = l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	admissiontest.Execute(t, l, nil)
	decision := session.Snapshot()
	assert.Equal(t, decision.EvaluationOnly(reader), true)
	assert.Equal(t, decision.EvaluationOnly(dependency), true)
	assert.Equal(t, decision.OrdinaryDemand(readerOwner), true)
	assert.Equal(t, decision.OrdinaryDemand(dependencyOwner), true)
	depProducer := st.GetDeclaration(dependencyOwner)
	assert.FatalIf(t, depProducer == nil, "dependency producer has no declaration")
	assert.Equal(t, st.DeleteObject(dependencyOwner), true)

	var reads store.ReadSet
	reads.Init(st, app, nil)
	defer reads.Retire()
	indexedDependency := decision.OrderingDeclaration(&reads, dependency, false)
	assert.FatalIf(t, indexedDependency == nil, "missing indexed dependency")
	assert.Equal(t, decision.OrderingDeclaration(&reads, dependency, true), nil)
	reads.AssessOrdering()
	edge := store.OrderingEdge{Target: dependency, ReadyExpr: "dep.metadata.name == 'dependency'"}
	reads.RequireOrdering(&edge)
	reads.Require(manifest.DependencyRef{
		NamedResource: dependency, ReadyExpr: &edge.ReadyExpr, RequiresDeclaration: true,
	})
	waiter := depwait.Waiter{Store: &reads, Parent: app, Decision: decision, Missing: depwait.NewMissingPolicy(nil)}
	err = waiter.Assess(t.Context(), &reads, manifest.DrainNone, depwait.AssessGates)
	blocked, ok := errors.AsType[*depwait.BlockedError](err)
	assert.FatalIf(t, !ok, "expected the pending dependency declaration, got %v", err)
	assert.Diff(t, blocked.Deps, []manifest.NamedResource{dependency})

	st.AddObject(depProducer)
	st.RecordEmitted(
		dependencyOwner,
		&store.Emission{Children: []store.Child{{Obj: indexedDependency}}, Complete: true},
		nil,
	)
	assert.Equal(t, st.GetDeclaration(dependency), indexedDependency)
	readerProducer := st.GetDeclaration(readerOwner)
	assert.FatalIf(t, readerProducer == nil, "reader producer has no declaration")
	assert.Equal(t, st.DeleteObject(readerOwner), true)

	var nested, prediction store.ReadSet
	nested.Init(st, app, nil)
	defer nested.Retire()
	indexedReader := decision.OrderingDeclaration(&nested, reader, false)
	assert.FatalIf(t, indexedReader == nil, "missing indexed reader")
	assert.Equal(t, decision.OrderingDeclaration(&nested, reader, true), nil)
	for _, gate := range depwait.OrderingGates(st.GetDeclaration(app), &nested) {
		nested.Require(gate)
	}
	waiter.Store = &nested
	err = waiter.Assess(t.Context(), &nested, manifest.DrainNone, depwait.AssessGates)
	blocked, ok = errors.AsType[*depwait.BlockedError](err)
	assert.FatalIf(t, !ok, "expected the pending reader producer, got %v", err)
	assert.Diff(t, blocked.Deps, []manifest.NamedResource{dependency, reader})

	prediction.Init(st, reader, nil)
	defer prediction.Retire()
	waiter.Parent, waiter.Store = reader, &prediction
	err = waiter.Prediction(
		t.Context(),
		&prediction,
		manifest.References(indexedReader, manifest.ReferenceRelations),
		l.Index,
	)
	failure, ok := errors.AsType[*manifest.DependencyFailedError](err)
	assert.FatalIf(t, !ok, "expected readiness expression failure during prediction, got %v", err)
	assert.Equal(t, failure.Reasons[dependency].Message, "readyExpr timeout: context deadline exceeded")

	st.AddObject(readerProducer)
	st.RecordEmitted(readerOwner, &store.Emission{Children: []store.Child{{Obj: indexedReader}}, Complete: true}, nil)
	var settled store.ReadSet
	settled.Init(st, reader, nil)
	defer settled.Retire()
	waiter.Store = &settled
	assert.NoError(t, waiter.Prediction(
		t.Context(), &settled, manifest.References(indexedReader, manifest.ReferenceRelations), l.Index,
	))
}
