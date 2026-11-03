package store

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func TestRecordEmitted_BorrowedPayloadOwners(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"complete", "close", "rejected/output", "rejected/a", "rejected/b", "rejected/c"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedBorrowedPayloadOwnersCase(t, &mode)
		},
		)
	}
}

func TestGetDeclaration_IndependenceLifecycle(t *testing.T) {
	t.Parallel()
	for _, support := range []string{"uncovered", "independent", "covered"} {
		for _, route := range []string{"pre_index", "post_index", "document", "batch", "included", "preflight", "readset", "readmit", "anchor", "batch_anchor"} {
			t.Run(support+"/"+route, func(t *testing.T) {
				t.Parallel()
				testGetDeclarationIndependenceLifecycleCase(
					t,
					&testGetDeclarationIndependenceLifecycleCaseInputs{support: &support, route: &route},
				)
			},
			)
		}
	}
}

func TestRecordEmitted_IndependencePublication(t *testing.T) {
	t.Parallel()
	for _, independent := range []bool{false, true} {
		for _, route := range []string{"first", "stable", "replacement", "hidden_producer", "reachability", "target", "expanded_coverage", "expanded_inventory", "metadata"} {
			t.Run(fmt.Sprintf("independent_%t/%s", independent, route), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedIndependencePublicationCase(
					t,
					&testRecordEmittedIndependencePublicationCaseInputs{independent: &independent, route: &route},
				)
			},
			)
		}
	}
}

func TestRecordEmitted_AcceptedFootprint(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"complete", "empty", "partial", "partial_reuse"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedAcceptedFootprintCase(t, &state)
		},
		)
	}
}

type acceptanceBarrier struct {
	manifest.BaseManifest

	tasks            *task.Service
	reached, release chan struct{}
	once             sync.Once
}

func (b *acceptanceBarrier) Named() manifest.NamedResource {
	b.once.Do(func() {
		close(b.reached)
		b.tasks.YieldSlot(func() { <-b.release })
	})
	return b.BaseManifest.Named()
}

func TestRecordEmitted_RejectedSuccessorRetainsOutputReads(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"stable", "replacement"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedRejectedSuccessorRetainsOutputReadsCase(
				t,
				&route,
			)
		},
		)
	}
}

func testRecordEmittedBorrowedPayloadOwnersCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	var output, first, second, third ReadSet
	output.Init(s, owner.Named(), nil)
	defer output.Retire()
	input := emissionChild("input-output", "output")
	s.AddObject(input.Obj)
	output.GetObject(input.Obj.Named())
	initial := &Emission{Complete: true}
	output.Bind(initial)
	s.RecordEmitted(owner.Named(), initial, nil)
	first.Init(s, owner.Named(), nil)
	second.Init(s, owner.Named(), nil)
	third.Init(s, owner.Named(), nil)
	defer first.Retire()
	defer second.Retire()
	defer third.Close()
	registrations := []*registrationHead{output.input(input.Obj.Named()).object.head}
	publish := func(reads *ReadSet, name string) {
		t.Helper()
		input := emissionChild("input-"+name, name)
		s.AddObject(input.Obj)
		reads.GetObject(input.Obj.Named())
		registrations = append(registrations, reads.input(input.Obj.Named()).object.head)
		_, _, next := reads.GetEmission(owner.Named())
		defer next.Finish()
		child := emissionChild(name, name)
		next.Children, next.Manifests = []Child{
			child,
		}, testutil.DocumentOccurrences(
			[]map[string]any{child.Doc},
		)
		reads.Bind(&next)
		s.RecordEmitted(
			owner.Named(),
			&next,
			map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: name}},
		)
	}
	publish(&first, "a")
	publish(&second, "b")
	publish(&third, "c")
	accepted := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	{
		assert.FatalIf(t, len(accepted.Children) != 3 || !accepted.Retains(&first) || !accepted.Retains(&second) ||
			!accepted.Retains(&third), "%s", "partial publication lost a borrowed payload owner")
	}
	{
		assert.FatalIf(
			t,
			first.retired.Load() || second.retired.Load(),
			"%s",
			"partial publication retired a descriptor still borrowed by the inventory",
		)
	}
	for _, head := range registrations {
		{
			assert.FatalIf(
				t,
				head.first.Load() == nil,
				"%s",
				"partial publication released a borrowed payload owner's registration",
			)
		}
	}
	if (*testCaseInput) == "close" {
		third.Close()
		for _, reads := range []*ReadSet{&output, &first, &second, &third} {
			{
				assert.FatalIf(t, !reads.retired.Load(), "%s", "closing retained a payload owner's registrations")
			}
		}
		assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
		for _, head := range registrations {
			{
				assert.FatalIf(
					t,
					head.first.Load() != nil,
					"%s",
					"sweep retained a closed payload owner's registration",
				)
			}
		}
		return
	}
	testRecordEmittedBorrowedPayloadOwnersCaseCompletion(t, &testRecordEmittedBorrowedPayloadOwnersCaseCompletionState{testCaseInput: &testCaseInput, s: &s, owner: &owner, output: &output, first: &first, second: &second, third: &third, registrations: &registrations, accepted: &accepted})
}

type testGetDeclarationIndependenceLifecycleCaseInputs struct {
	support *string
	route   *string
}

func testGetDeclarationIndependenceLifecycleCase(
	t *testing.T,
	testCaseInput *testGetDeclarationIndependenceLifecycleCaseInputs,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "input", Namespace: "apps", URL: "oci://raw.example"}
	st.AddObject(owner)
	proofs := []manifest.Production{{Owner: owner.Named()}}
	independent := (*testCaseInput.support) == "independent"
	if independent {
		proofs = append(proofs, manifest.Production{})
	}
	if (*testCaseInput.support) == "uncovered" {
		proofs, independent = nil, true
	}
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: proofs}},
	}
	if (*testCaseInput.route) == "anchor" || (*testCaseInput.route) == "batch_anchor" {
		index.Records[ownerID("descendant")] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: raw.Named()}},
		}
	}
	if (*testCaseInput.route) == "pre_index" {
		st.AddObject(raw)
		{
			assert.FatalIf(
				t,
				st.GetDeclaration(raw.Named()) != raw,
				"%s",
				"pre-index declaration lost independent support",
			)
		}
	}
	st.SetDiscoveryIndex(index)
	switch *testCaseInput.route {
	case "post_index":
		st.AddObject(raw)
	case "document":
		st.AddObject(raw, map[string]any{"apiVersion": manifest.SourceDomain + "/v1"})
	case "batch":
		st.AddObjects([]manifest.BaseManifest{raw})
	case "included", "preflight":
		if (*testCaseInput.route) == "included" {
			st.SetIncluded(raw.Named(), false)
		} else {
			st.SetPreflight(raw.Named(), "preflight")
		}
		st.AddObject(raw)
	case "readset":
		st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Key: "source"})
		var reads ReadSet
		reads.Init(st, owner.Named(), nil)
		defer reads.Retire()
		reads.Admit(raw, manifest.BootstrapSourceID, manifest.NamedResource{})
	case "readmit":
		st.AddObject(raw)
		{
			assert.FatalIf(t, !st.DeleteObject(raw.Named()), "%s", "fixture did not delete its first admission")
		}
		st.AddObject(raw)
	case "anchor", "batch_anchor":
		if (*testCaseInput.route) == "anchor" {
			st.AddObject(raw)
		} else {
			st.AddObjects([]manifest.BaseManifest{raw})
		}
	}
	testGetDeclarationIndependenceLifecycleCaseCompletion(t, &testGetDeclarationIndependenceLifecycleCaseCompletionState{testCaseInput: &testCaseInput, st: &st, raw: &raw, independent: &independent})
}

type testRecordEmittedIndependencePublicationCaseInputs struct {
	independent *bool
	route       *string
}

func testRecordEmittedIndependencePublicationCase(
	t *testing.T,
	testCaseInput *testRecordEmittedIndependencePublicationCaseInputs,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	child := emissionChild("output", "first")
	proofs := []manifest.Production{{Owner: owner.Named()}}
	if (*testCaseInput.route) == "expanded_coverage" {
		proofs[0].Owner = ownerID("nested")
	}
	if *testCaseInput.independent {
		proofs = append(proofs, manifest.Production{})
	}
	id := child.Obj.Named()
	if (*testCaseInput.route) == "hidden_producer" {
		id = owner.Named()
		proofs[0].Owner = ownerID("covering")
	} else {
		st.AddObject(owner)
	}
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: proofs}}
	if (*testCaseInput.route) == "reachability" || (*testCaseInput.route) == "expanded_coverage" {
		records[ownerID("descendant")] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: id}},
		}
	}
	if (*testCaseInput.route) == "target" {
		child = Child{
			Obj: &manifest.RawObject{
				APIVersion: "objectbucket.io/v1alpha1",
				Kind:       "ObjectBucketClaim",
				Name:       id.Name,
				Namespace:  id.Namespace,
			},
		}
	}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
	sh := st.shardFor(id)
	sh.mu.RLock()
	_, present := sh.cells[id]
	sh.mu.RUnlock()
	{
		assert.FatalIf(t, present, "%s", "index manufactured an absent object cell")
	}
	if (*testCaseInput.route) == "metadata" {
		st.SetIncluded(id, false)
		st.SetPreflight(id, "preflight")
	}
	var absent ReadSet
	absent.Init(st, ownerID("absent"), nil)
	defer absent.Retire()
	assert.Equal(t, absent.GetDeclaration(id), nil)
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Complete = true
	if (*testCaseInput.route) != "reachability" {
		output.Children = []Child{child}
	}
	nested := &manifest.Kustomization{Name: "nested", Namespace: "apps"}
	if (*testCaseInput.route) == "expanded_coverage" || (*testCaseInput.route) == "expanded_inventory" {
		output.Children = []Child{{Obj: nested}}
	}
	testRecordEmittedIndependencePublicationCaseCompletion(t, &testRecordEmittedIndependencePublicationCaseCompletionState{testCaseInput: &testCaseInput, st: &st, owner: &owner, child: &child, id: &id, sh: &sh, absent: &absent, output: &output, nested: &nested})
}

func testRecordEmittedAcceptedFootprintCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	st.AddObject(owner)
	emitterSource := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "sources",
		Name:      "emitter",
	}
	child := &manifest.Kustomization{
		Name:      "child",
		Namespace: "apps",
		Path:      "apps/red",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "content",
			Namespace: "sources",
		},
	}
	declaration := &manifest.RawObject{
		APIVersion: "objectbucket.io/v1alpha1",
		Kind:       "ObjectBucketClaim",
		Name:       "bucket",
		Namespace:  "apps",
	}
	covered := emissionChild("prediction", "raw")
	st.AddObject(covered.Obj)
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				covered.Obj.Named(): {
					Productions: []manifest.Production{{Owner: owner.Named(), Source: emitterSource}},
				},
			},
		},
	)
	_, _, next := st.GetEmission(owner.Named())
	defer next.Finish()
	next.Children, next.Render, next.Complete = []Child{
		{Obj: child},
		{Obj: declaration},
	}, manifest.NewRenderReceipt(
		"receipt",
		func(a, b any) bool { return a == b },
	), (*testCaseInput) != "partial"
	next.binding = sourceBinding{
		source:   emitterSource,
		artifact: &SourceArtifact{Key: "generation", Content: "generation"},
	}
	{
		assert.FatalIf(t, len(next.coverage) == 0, "%s", "fixture has no withdrawal coverage")
	}
	if (*testCaseInput) == "empty" {
		next.Children = nil
	}
	{
		assert.FatalIf(t, next.Footprint() != nil, "%s", "unpublished attempt exposed a footprint")
	}
	st.RecordEmitted(owner.Named(), &next, nil)
	if (*testCaseInput) == "partial_reuse" {
		next.Complete = false
		{
			assert.FatalIf(t, next.Footprint() != nil, "%s", "reused partial caller exposed complete history")
		}
		st.RecordEmitted(owner.Named(), &next, nil)
	}
	footprint := next.Footprint()
	if (*testCaseInput) == "partial" || (*testCaseInput) == "partial_reuse" {
		{
			assert.FatalIf(t, footprint != nil, "%s", "partial output exposed accepted history")
		}
		return
	}
	{
		assert.FatalIf(t, footprint == nil, "%s", "complete acceptance did not expose a footprint")
	}
	st.RecordEmitted(owner.Named(), &Emission{Complete: true, binding: next.binding}, nil)
	var ids []manifest.NamedResource
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	for id, record := range footprint {
		ids = append(ids, id)
		records[id] = record
		{
			assert.FatalIf(t, id == covered.Obj.Named(), "%s", "withdrawal coverage became actual history")
		}
		{
			assert.FatalIf(
				t,
				record.Authored != (manifest.NamedResource{}) || record.Path != "" || record.File != "",
				"%s",
				"generated content acquired raw-file provenance",
			)
		}
	}
	{
		assert.FatalIf(
			t,
			!slices.IsSortedFunc(ids, manifest.NamedResource.Compare),
			"%s",
			"footprint identity order is unstable",
		)
	}
	if (*testCaseInput) == "empty" {
		assert.Equal(t, len(records), 0)
		return
	}
	assert.Equal(t, len(records), 4)
	assert.Diff(
		t,
		records[child.Named()].ContentRoots,
		map[manifest.NamedResource][]string{child.SourceID(): {"apps/red"}},
	)
	assert.Diff(t, records[child.Named()].Refs, []manifest.NamedResource{child.SourceID()})
	assert.Diff(
		t,
		records[child.Named()].Productions,
		[]manifest.Production{{Owner: owner.Named(), Source: emitterSource}},
	)
	secret := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Name:      declaration.Name,
		Namespace: declaration.Namespace,
	}
	assert.Diff(
		t,
		records[secret].Productions,
		[]manifest.Production{{Owner: owner.Named(), Declaration: declaration.Named(), Source: emitterSource}},
	)
	assert.Equal(t, next.Render.Value().(string), "receipt")
	{
		assert.FatalIf(
			t,
			st.GetArtifact(owner.Named(), ArtifactContent).(*Emission).Footprint() != nil,
			"%s",
			"stored inventory manufactured caller acceptance",
		)
	}
}

func testRecordEmittedRejectedSuccessorRetainsOutputReadsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	a := &manifest.ConfigMap{Name: "a", Namespace: "apps", Data: map[string]any{"value": "A"}}
	b := &manifest.ConfigMap{Name: "b", Namespace: "apps", Data: map[string]any{"value": "B"}}
	st.AddObjects([]manifest.BaseManifest{owner, a, b})
	var first, successor ReadSet
	first.Init(st, owner.Named(), nil)
	successor.Init(st, owner.Named(), nil)
	defer first.Retire()
	defer successor.Retire()
	first.GetObject(a.Named())
	_, _, prior := first.GetEmission(owner.Named())
	defer prior.Finish()
	child := emissionChild("output", "old")
	prior.Children, prior.Manifests, prior.Render, prior.Complete = []Child{
		child,
	}, testutil.DocumentOccurrences(
		[]map[string]any{child.Doc},
	), manifest.NewRenderReceipt(
		"prior",
		func(a, b any) bool { return a == b },
	), true
	first.Bind(&prior)
	st.RecordEmitted(owner.Named(), &prior, nil)
	accepted := st.GetArtifact(owner.Named(), ArtifactContent)
	successor.GetObject(b.Named())
	_, _, next := successor.GetEmission(owner.Named())
	defer next.Finish()
	name := "output"
	if (*testCaseInput) == "replacement" {
		name = "replacement"
	}
	incoming := emissionChild(name, "new")
	tasks := task.NewBounded(2)
	barrier := &acceptanceBarrier{
		BaseManifest: incoming.Obj,
		tasks:        tasks,
		reached:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	next.Children, next.Manifests, next.Render, next.Complete = []Child{
		{Obj: barrier, Doc: incoming.Doc},
	}, testutil.DocumentOccurrences(
		[]map[string]any{incoming.Doc},
	), manifest.NewRenderReceipt(
		"successor",
		func(a, b any) bool { return a == b },
	), true
	var writes atomic.Int32
	tasks.Go(t.Context(), "publish", func(context.Context) {
		st.RecordEmitted(owner.Named(), &next, nil)
		if next.Footprint() != nil {
			writes.Add(1)
		}
	})
	tasks.Go(t.Context(), "invalidate", func(context.Context) {
		tasks.YieldSlot(func() { <-barrier.reached })
		st.AddObject(
			&manifest.ConfigMap{Name: b.Name, Namespace: b.Namespace, Data: map[string]any{"value": "changed"}},
		)
		close(barrier.release)
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), 0)
	assert.Equal(t, writes.Load(), 0)
	{
		assert.FatalIf(
			t,
			!first.Valid() || first.retired.Load() || st.GetArtifact(owner.Named(), ArtifactContent) != accepted,
			"%s",
			"rejected successor retired or replaced valid predecessor output",
		)
	}
	{
		assert.FatalIf(
			t,
			next.Footprint() != nil || successor.Valid(),
			"%s",
			"commit-boundary rejection supplied acceptance",
		)
	}
}

type testRecordEmittedBorrowedPayloadOwnersCaseCompletionState struct {
	testCaseInput **string
	s             **Store
	owner         **manifest.Kustomization
	output        *ReadSet
	first         *ReadSet
	second        *ReadSet
	third         *ReadSet
	registrations *[]*registrationHead
	accepted      **Emission
}

func testRecordEmittedBorrowedPayloadOwnersCaseCompletion(t *testing.T, scope *testRecordEmittedBorrowedPayloadOwnersCaseCompletionState) {
	t.Helper()
	if (*(*scope.testCaseInput)) != "complete" {
		var rejected ReadSet
		rejected.Init((*scope.s), (*scope.owner).Named(), nil)
		defer rejected.Retire()
		input := emissionChild("successor-only", "old")
		(*scope.s).AddObject(input.Obj)
		rejected.GetObject(input.Obj.Named())
		rejectedHead := rejected.input(input.Obj.Named()).object.head
		for _, head := range *scope.registrations {
			{
				assert.FatalIf(
					t,
					head == rejectedHead,
					"%s",
					"successor-only input shares an accepted registration head",
				)
			}
		}
		release := make(chan struct{})
		barrier := &admissionBarrier{
			ConfigMap: &manifest.ConfigMap{Name: "replacement", Namespace: "apps"},
			prepared:  make(chan struct{}),
			release:   release,
		}
		next := &Emission{Children: []Child{{Obj: barrier}}, Fingerprint: "stale", Complete: true}
		rejected.Bind(next)
		resume := sync.OnceFunc(func() { close(release) })
		done := make(chan struct{})
		go func() {
			defer close(done)
			(*scope.s).RecordEmitted((*scope.owner).Named(), next, nil)
		}()
		t.Cleanup(func() { resume(); <-done })
		select {
		case <-barrier.prepared:
		case <-done:
			t.Fatal("publication did not reach the admission barrier")
		}
		{
			assert.FatalIf(t, !rejected.Valid(), "%s", "reads invalid before concurrent input replacement")
		}
		(*scope.s).AddObject(emissionChild("successor-only", "new").Obj)
		{
			assert.FatalIf(t, rejected.Valid(), "%s", "input replacement did not invalidate the pending publication")
		}
		resume()
		<-done
		{
			assert.FatalIf(t, (*scope.s).GetArtifact((*scope.owner).Named(), ArtifactContent) != (*scope.accepted) ||
				(*scope.s).GetObject(
					barrier.ConfigMap.Named(),
				) != nil, "%s", "stale proposal replaced accepted payload inventory")
		}
		{
			assert.FatalIf(
				t,
				(*scope.accepted).OutputReads != &(*scope.output) || (*scope.accepted).payloadReads != &(*scope.first) || len((*scope.accepted).borrowed) != 2,
				"%s",
				"rejected proposal changed accepted output and payload provenance",
			)
		}
		for _, reads := range []*ReadSet{&(*scope.output), &(*scope.first), &(*scope.second), &(*scope.third)} {
			{
				assert.ErrorIf(
					t,
					reads.retired.Load() || !(*scope.accepted).Retains(reads),
					"%s",
					"rejected proposal retired accepted output or borrowed payload reads",
				)
			}
		}
		rejected.Retire()
		assert.Equal(t, len(slices.Collect((*scope.s).Sweep())), 0)
		{
			assert.ErrorIf(
				t,
				!rejected.retired.Load() || rejectedHead.first.Load() != nil,
				"%s",
				"sweep retained rejected attempt registrations",
			)
		}
		for _, head := range *scope.registrations {
			{
				assert.ErrorIf(
					t,
					head.first.Load() == nil,
					"%s",
					"sweep removed an accepted output or borrowed payload registration",
				)
			}
		}
		{
			assert.FatalIf(
				t,
				(*scope.s).GetArtifact((*scope.owner).Named(), ArtifactContent) != (*scope.accepted),
				"%s",
				"sweep removed accepted payload inventory",
			)
		}
		name := (*(*scope.testCaseInput))[len("rejected/"):]
		(*scope.s).AddObject(emissionChild("input-"+name, "new").Obj)
		{
			assert.FatalIf(
				t,
				!slices.Contains(slices.Collect((*scope.s).Sweep()), (*scope.owner).Named()),
				"%s",
				"accepted input change did not notify its owner after successor rejection",
			)
		}
		return
	}
	last := emissionChild("c", "c")
	_, _, next := (*scope.third).GetEmission((*scope.owner).Named())
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = []Child{
		last,
	}, testutil.DocumentOccurrences(
		[]map[string]any{last.Doc},
	), true
	(*scope.third).Bind(&next)
	(*scope.s).RecordEmitted((*scope.owner).Named(), &next, nil)
	{
		assert.FatalIf(
			t,
			!(*scope.first).retired.Load() || !(*scope.second).retired.Load() || (*scope.third).retired.Load(),
			"%s",
			"completion did not release exactly the superseded payload owners",
		)
	}
	{
		assert.FatalIf(
			t,
			(*scope.s).GetArtifact(last.Obj.Named(), ArtifactContent) == nil,
			"%s",
			"completion removed the current execution's accepted descriptor",
		)
	}
}

type testGetDeclarationIndependenceLifecycleCaseCompletionState struct {
	testCaseInput **testGetDeclarationIndependenceLifecycleCaseInputs
	st            **Store
	raw           **manifest.OCIRepository
	independent   *bool
}

func testGetDeclarationIndependenceLifecycleCaseCompletion(t *testing.T, scope *testGetDeclarationIndependenceLifecycleCaseCompletionState) {
	t.Helper()
	var want manifest.BaseManifest
	if (*scope.independent) || (*(*scope.testCaseInput).route) == "anchor" || (*(*scope.testCaseInput).route) == "batch_anchor" {
		want = (*scope.raw)
	}
	assert.Equal(t, (*scope.st).GetObject((*scope.raw).Named()), manifest.BaseManifest((*scope.raw)))
	assert.Equal(t, (*scope.st).GetDeclaration((*scope.raw).Named()), want)
	got, _, emission := (*scope.st).GetEmission((*scope.raw).Named())
	defer emission.Finish()
	assert.Equal(t, got, want)
	var reads ReadSet
	reads.Init((*scope.st), ownerID("reader"), nil)
	defer reads.Retire()
	assert.Equal(t, reads.GetDeclaration((*scope.raw).Named()), want)
	assert.Equal(t, reads.input((*scope.raw).Named()).view.independent, (*scope.independent))
	if (*(*scope.testCaseInput).route) == "included" {
		assert.Equal(t, reads.input((*scope.raw).Named()).view.excluded, true)
	}
	if (*(*scope.testCaseInput).route) == "preflight" {
		assert.Equal(t, reads.input((*scope.raw).Named()).view.preflight, "preflight")
	}
	if (*(*scope.testCaseInput).route) == "document" {
		added := 0
		defer (*scope.st).AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
			if id == (*scope.raw).Named() {
				added++
			}
		}, false)()
		doc := map[string]any{"apiVersion": manifest.SourceDomain + "/v1beta2"}
		(*scope.st).AddObject((*scope.raw), doc)
		{
			assert.FatalIf(t, reads.Valid(), "%s", "document replacement retained the captured generation")
		}
		assert.Equal(t, reads.GetDeclaration((*scope.raw).Named()), want)
		assert.Equal(t, reads.input((*scope.raw).Named()).view.independent, (*scope.independent))
		assert.Diff(
			t,
			reads.input((*scope.raw).Named()).view.child.Doc,
			map[string]any{"apiVersion": manifest.SourceDomain + "/v1"},
		)
		var current ReadSet
		current.Init((*scope.st), ownerID("current"), nil)
		defer current.Retire()
		got, document, replacement := current.GetEmission((*scope.raw).Named())
		defer replacement.Finish()
		assert.Equal(t, got, want)
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.raw).Named()), want)
		assert.Diff(t, document, doc)
		assert.Equal(t, current.input((*scope.raw).Named()).view.independent, (*scope.independent))
		{
			assert.FatalIf(
				t,
				replacement.inputHead.generation <= reads.input((*scope.raw).Named()).view.generation,
				"%s",
				"document replacement did not advance the generation",
			)
		}
		(*scope.st).AddObject((*scope.raw), manifest.DeepCopyMap(doc))
		if !current.Valid() || added != 1 {
			t.Fatalf(
				"equal document admission: valid=%t events=%d, want true and 1",
				current.Valid(),
				added,
			)
		}
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.raw).Named()), want)
		assert.Equal(t, current.GetDeclaration((*scope.raw).Named()), want)
		assert.Equal(t, current.input((*scope.raw).Named()).view.independent, (*scope.independent))
	}
}

type testRecordEmittedIndependencePublicationCaseCompletionState struct {
	testCaseInput **testRecordEmittedIndependencePublicationCaseInputs
	st            **Store
	owner         **manifest.Kustomization
	child         *Child
	id            *manifest.NamedResource
	sh            **shard
	absent        *ReadSet
	output        *Emission
	nested        **manifest.Kustomization
}

func testRecordEmittedIndependencePublicationCaseCompletion(t *testing.T, scope *testRecordEmittedIndependencePublicationCaseCompletionState) {
	t.Helper()
	(*scope.st).RecordEmitted((*scope.owner).Named(), &(*scope.output), nil)
	if (*(*scope.testCaseInput).route) == "expanded_inventory" {
		(*scope.st).RecordEmitted((*scope.nested).Named(), &Emission{Children: []Child{(*scope.child)}, Complete: true}, nil)
		(*scope.st).RecordEmitted((*scope.owner).Named(), &Emission{Complete: true}, nil)
	}
	{
		assert.FatalIf(
			t,
			!slices.Contains(slices.Collect((*scope.st).Sweep()), (*scope.absent).ID()) || (*scope.absent).Valid(),
			"%s",
			"first publication missed the registered absence",
		)
	}
	if (*(*scope.testCaseInput).route) == "stable" || (*(*scope.testCaseInput).route) == "replacement" {
		(*scope.child) = emissionChild("output", "second")
		children := []Child{(*scope.child)}
		if (*(*scope.testCaseInput).route) == "replacement" {
			children = append(children, emissionChild("extra", "value"))
		}
		(*scope.st).RecordEmitted((*scope.owner).Named(), &Emission{Children: children, Complete: true}, nil)
	}
	(*scope.sh).mu.RLock()
	cell := (*scope.sh).cells[(*scope.id)]
	fact := cell.independent
	(*scope.sh).mu.RUnlock()
	if (*(*scope.testCaseInput).route) == "metadata" {
		assert.Equal(t, cell.excluded, true)
		assert.Equal(t, cell.preflight, "preflight")
	}
	assert.Equal(t, fact, (*(*scope.testCaseInput).independent))
	if (*(*scope.testCaseInput).route) == "reachability" || (*(*scope.testCaseInput).route) == "target" ||
		(*(*scope.testCaseInput).route) == "expanded_coverage" ||
		(*(*scope.testCaseInput).route) ==
			"expanded_inventory" {
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.id)), nil)
		assert.Equal(t, (*scope.st).GetObject((*scope.id)), nil)
	} else if (*(*scope.testCaseInput).route) != "hidden_producer" {
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.id)), (*scope.child).Obj)
		var reads ReadSet
		reads.Init((*scope.st), (*scope.owner).Named(), nil)
		defer reads.Retire()
		_, _, next := reads.GetEmission((*scope.owner).Named())
		defer next.Finish()
		reads.Bind(&next)
		var want manifest.BaseManifest
		if *(*scope.testCaseInput).independent {
			want = (*scope.child).Obj
		}
		assert.Equal(t, reads.GetDeclaration((*scope.id)), want)
	}
}
