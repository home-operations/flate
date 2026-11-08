package selection

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAdmission_ConsumedSourceReplacementInvalidatesPreparation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: consumer, namespace: apps}\n" +
			"spec:\n" +
			"  path: ./app\n" +
			"  postBuild:\n" +
			"    substituteFrom: [{kind: ConfigMap, name: absent, optional: true}]\n")},
		"open.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\n" + "kind: ResourceSet\n" +
			"metadata: {name: open, namespace: apps}\n" +
			"spec: {resources: []}\n")},
		"index.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\n" + "kind: HelmRepository\n" +
			"metadata: {name: index, namespace: apps}\n" +
			"spec: {url: https://fixture.invalid/charts}\n")},
	}})
	consumer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{consumer}})
	declaration := &manifest.Kustomization{
		Name:      consumer.Name,
		Namespace: consumer.Namespace,
		Path:      "app",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "absent", Optional: true},
			},
		},
	}
	facts := session.facts.(*solverFacts)
	input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	record := manifest.DiscoveryRecord{
		File:        "consumer.yaml",
		Path:        filepath.Join(root, "consumer.yaml"),
		Declaration: declaration,
	}
	occurrence := OccurrenceFacts{
		Identity:     consumer,
		Authored:     consumer,
		Object:       declaration,
		Catalog:      declaration,
		Declaration:  OccurrenceID{Source: root, File: record.File},
		Mounted:      input,
		Located:      true,
		CatalogKnown: true,
	}
	facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{consumer: record}
	facts.candidates = map[manifest.NamedResource]store.Child{consumer: {Obj: declaration}}
	facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{consumer: occurrence}
	facts.roots = map[manifest.NamedResource]RootEvidence{
		consumer: {
			Identity:   consumer,
			Record:     RootRecordFrom(&record),
			Occurrence: occurrence,
			Scope: RootScopeFacts{
				Path:              record.Path,
				Eligibility:       RootEligible,
				Initial:           true,
				Bootstrap:         true,
				NamespaceEligible: true,
				Candidate:         true,
			},
		},
	}
	var seeded store.ReadSet
	seeded.Init(st, consumer, nil)
	seeded.Admit(
		store.Child{Obj: declaration, SourceSupport: manifest.BootstrapSourceID},
		manifest.BootstrapSourceID,
		manifest.NamedResource{},
	)
	st.Retire(&seeded)
	st.AddObject(&manifest.ResourceSet{Name: "open", Namespace: "apps"})
	st.AddObject(&manifest.HelmRepository{Name: "index", Namespace: "apps"})

	requester := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmRepository,
		Namespace: "apps",
		Name:      "index",
	}
	var reads store.ReadSet
	reads.Init(st, requester, nil)
	defer reads.Retire()
	reads.RequireData(consumer)
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	var work manifest.WorkRecorder
	reads.SetWork(&work)
	proposal, err := prepareSolver(t.Context(), t, session, PrepareRequest{Reads: &reads, Preparation: &preparation})
	assert.NoError(t, err)
	producer := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      "ResourceSet",
		Namespace: "apps",
		Name:      "open",
	}
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, reads.Valid(), true)
	_, committed, err := session.commitSolver(t.Context(), t, proposal, Construction)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, proposal.reads == nil && proposal.preparation == nil, true)
	before := work.Scope().Admission().Counts.OwnerPrepares
	for range st.Sweep() {
	}
	st.SetArtifact(requester, &store.SourceArtifact{LocalPath: root + "/acquired", FS: fstest.MapFS{}})
	if !reads.Valid() {
		t.Fatal("an unrelated source publication invalidated the consumer's materializer proof")
	}
	st.UpdateStatus(requester, store.StatusInfo{Status: store.StatusReady})
	var fresh store.ReadSet
	fresh.Init(st, requester, nil)
	fresh.SetWork(&work)
	defer fresh.Close()
	fresh.RequireData(consumer)
	proposal, err = prepareSolver(t.Context(), t, session, PrepareRequest{Reads: &fresh, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, proposal.reads != preparation.evidence, false)
	_, committed, err = session.commitSolver(t.Context(), t, proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().Reconcile(consumer), true)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, fresh.Valid(), true)
	assert.Equal(t, before, uint64(1))
	assert.Equal(t, work.Scope().Admission().Counts.OwnerPrepares, before)

	old := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	replacement := *old
	replacement.Content = manifest.Fingerprint("replaced consumed source")
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	var changedReads store.ReadSet
	changedReads.Init(st, requester, nil)
	changedReads.SetWork(&work)
	defer changedReads.Close()
	changedReads.RequireData(consumer)
	proposal, err = prepareSolver(
		t.Context(),
		t,
		session,
		PrepareRequest{Reads: &changedReads, Preparation: &preparation},
	)
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, work.Scope().Admission().Counts.OwnerPrepares, before+1)
	assert.Equal(t, slices.Contains(proposal.Needed, consumer), true)
}

func TestAdmission_UnrelatedParentPublicationKeepsPreparation(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	source := &manifest.GitRepository{Name: "source", Namespace: "sources"}
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "."}
	st.AddObjects([]manifest.BaseManifest{source, parent})
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})

	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{parent.Named()}})
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	var work manifest.WorkRecorder
	_, retry := publishDecision(t, session, new(Admission{
		Needed: []manifest.NamedResource{source.Named()}, Ordinary: []manifest.NamedResource{source.Named()},
	}), 0)
	assert.Equal(t, retry, false)
	generation := []store.Child{{Obj: source}}
	prepare := func(t *testing.T) {
		t.Helper()
		var reads store.ReadSet
		reads.Init(st, source.Named(), nil)
		reads.SetWork(&work)
		defer reads.Close()
		proposal, err := session.Prepare(
			t.Context(),
			PrepareRequest{Generation: generation, Reads: &reads, Preparation: &preparation},
		)
		assert.NoError(t, err)
		_, committed, err := session.commitSolver(t.Context(), t, proposal, Execution)
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
		assert.Equal(t, slices.Contains(proposal.Needed, source.Named()), true)
		assert.Equal(t, slices.Contains(proposal.Needed, parent.Named()), false)
	}
	prepared, published := make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "publish unrelated parent", func(context.Context) {
		<-prepared
		st.RecordEmitted(parent.Named(), &store.Emission{Complete: true, Children: []store.Child{
			{Obj: &manifest.Kustomization{Name: "late", Namespace: "apps", Path: "late"}},
		}}, nil)
		close(published)
	})
	prepare(t)
	before := work.Scope().Admission().Counts
	assert.Equal(t, before.Attempts, uint64(1))
	assert.Equal(t, before.OwnerPrepares, uint64(1))
	close(prepared)
	<-published
	tasks.BlockTillDone()
	for range st.Sweep() {
	}
	prepare(t)
	assert.Diff(t, work.Scope().Admission().Counts, before)
	assert.Equal(t, st.GetObject(source.Named()), manifest.BaseManifest(source))
}

func TestAdmission_NewGenerationUsesCurrentProducer(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"productions", "owner"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			previous := &manifest.Kustomization{Name: "previous", Namespace: "apps"}
			selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "app"}
			document := map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization",
				"metadata": map[string]any{"name": child.Name, "namespace": child.Namespace},
				"spec":     map[string]any{"path": child.Path},
			}
			st.AddObjects([]manifest.BaseManifest{previous, selected})
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})

			changes := change.NewSet(nil)
			session := newSolverSession(t, st, changes, Request{Changed: true})
			_, retry := publishDecision(t, session, new(Admission{
				Needed:   []manifest.NamedResource{selected.Named()},
				Selected: []manifest.NamedResource{selected.Named()},
				Dirty:    []manifest.NamedResource{selected.Named()},
				Ordinary: []manifest.NamedResource{selected.Named()},
			}), 0)
			assert.Equal(t, retry, false)
			var preparation *Preparation
			defer func() { preparation.Retire() }()
			for i, parent := range []*manifest.Kustomization{previous, selected} {
				request := PrepareRequest{
					Preparation: &preparation,
					Generation:  []store.Child{{Obj: child, Doc: document}},
				}
				if route == "productions" {
					request.Generation[0].Productions = []manifest.Production{{Owner: parent.Named()}}
				} else {
					request.Owner = parent.Named()
				}
				proposal, err := prepareSolver(t.Context(), t, session, request)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Needed, child.Named()), i == 1)
				assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), i == 1)
				_, committed, err := session.commitSolver(t.Context(), t, proposal, Execution)
				assert.NoError(t, err)
				assert.Equal(t, committed, true)
			}
			assert.Equal(t, session.Snapshot().Output(child.Named()), true)
		})
	}
}

func TestAdmission_UnrelatedSourcePublicationPreservesProof(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\n" + "kind: Kustomization\n" +
			"metadata: {name: consumer, namespace: apps}\n" +
			"spec:\n" +
			"  path: ./app\n" +
			"  postBuild:\n" +
			"    substituteFrom: [{kind: ConfigMap, name: absent, optional: true}]\n")},
		"open.yaml": {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\n" + "kind: ResourceSet\n" +
			"metadata: {name: open, namespace: apps}\n" +
			"spec: {resources: []}\n")},
		"index.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\n" + "kind: HelmRepository\n" +
			"metadata: {name: index, namespace: apps}\n" +
			"spec: {url: https://fixture.invalid/charts}\n")},
	}})
	consumer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{consumer}})
	st.AddObject(
		&manifest.Kustomization{
			Name:      consumer.Name,
			Namespace: consumer.Namespace,
			Path:      "app",
			PostBuild: &kustomizev1.PostBuild{
				SubstituteFrom: []manifest.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "absent", Optional: true},
				},
			},
		},
	)
	st.AddObject(&manifest.ResourceSet{Name: "open", Namespace: "apps"})
	st.AddObject(&manifest.HelmRepository{Name: "index", Namespace: "apps"})

	requester := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmRepository,
		Namespace: "apps",
		Name:      "index",
	}
	var reads store.ReadSet
	reads.Init(st, requester, nil)
	defer reads.Retire()
	reads.RequireData(consumer)
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	var work manifest.WorkRecorder
	reads.SetWork(&work)
	proposal, err := prepareSolver(t.Context(), t, session, PrepareRequest{Reads: &reads, Preparation: &preparation})
	assert.NoError(t, err)
	producer := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      "ResourceSet",
		Namespace: "apps",
		Name:      "open",
	}
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, reads.Valid(), true)
	_, committed, err := session.commitSolver(t.Context(), t, proposal, Construction)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, proposal.reads == nil && proposal.preparation == nil, true)
	before := work.Scope().Admission().Counts.OwnerPrepares
	for range st.Sweep() {
	}
	st.SetArtifact(requester, &store.SourceArtifact{LocalPath: root + "/acquired", FS: fstest.MapFS{}})
	if !reads.Valid() {
		t.Fatal("an unrelated source publication invalidated the consumer's materializer proof")
	}
	st.UpdateStatus(requester, store.StatusInfo{Status: store.StatusReady})
	unrelated := manifest.NewConfigMap(
		manifest.NamedResource{Name: "unrelated", Namespace: "apps"},
		map[string]any{"value": "runtime"},
	)
	st.AddObject(unrelated)
	var fresh store.ReadSet
	fresh.Init(st, requester, nil)
	fresh.SetWork(&work)
	defer fresh.Close()
	fresh.RequireData(consumer)
	proposal, err = prepareSolver(t.Context(), t, session, PrepareRequest{Reads: &fresh, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, proposal.reads != preparation.evidence, false)
	_, derived := preparation.closure.states[unrelated.Named()]
	assert.Equal(t, derived, false)
	_, committed, err = session.commitSolver(t.Context(), t, proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().Reconcile(consumer), true)
	assert.Equal(t, slices.Contains(proposal.Needed, producer), false)
	assert.Equal(t, fresh.Valid(), true)
	assert.Equal(t, before, uint64(1))
	assert.Equal(t, work.Scope().Admission().Counts.OwnerPrepares, before)
}
