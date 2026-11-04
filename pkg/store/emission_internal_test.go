package store

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func emissionChild(name, value string) Child {
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": "apps"},
		"data":       map[string]any{"value": value},
	}
	parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, authoritative := parsed.Object, parsed.Document
	if err != nil {
		panic(err)
	}
	return Child{Obj: obj, Doc: authoritative}
}

func TestRecordEmitted_ParkedSnapshotReplay(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			testRecordEmittedParkedSnapshotReplayCase(
				t,
				&workers,
			)
		},
		)
	}
}

func accept(t *testing.T, s *Store, owner manifest.NamedResource, next *Emission) {
	t.Helper()
	s.RecordEmitted(owner, next, nil)
}

func ownerID(name string) manifest.NamedResource {
	return testutil.KustomizationID("apps", name)
}

func checkEffectiveObject(t *testing.T, s *Store, id manifest.NamedResource, want *Child) {
	t.Helper()
	assert.Equal(t, s.GetObject(id), want.Obj)
	obj, _ := s.Snapshot(id, false)
	assert.Equal(t, obj, want.Obj)
	obj, _, attempt := s.GetEmission(id)
	defer attempt.Finish()
	assert.Equal(t, obj, want.Obj)
	if s.EmissionOwner(id) != (manifest.NamedResource{}) {
		assert.Diff(t, attempt.inputDoc, want.Doc)
	}
	for _, objects := range [][]manifest.BaseManifest{
		s.ListObjects(id.Group, id.Kind), s.ListObjects(id.Group, id.Kind),
		s.ListObjects("", ""), s.ListObjects("", ""),
	} {
		var found manifest.BaseManifest
		for i, object := range objects {
			if object.Named() == id {
				found = object
			}
			assert.FatalIf(
				t,
				i > 0 && objects[i-1].Named().Compare(object.Named()) >= 0,
				"%s",
				"effective inventory is not sorted and unique",
			)
		}
		assert.Equal(t, found, want.Obj)
	}
	sh := s.shardFor(id)
	sh.mu.RLock()
	record, present := sh.conditions[id]
	record.conditions = slices.Clone(record.conditions)
	sh.mu.RUnlock()
	defer func() {
		sh.mu.Lock()
		if present {
			sh.conditions[id] = record
		} else {
			delete(sh.conditions, id)
		}
		sh.mu.Unlock()
	}()
	s.UpdateFailure(id, manifest.NewError(manifest.ErrInput, nil, "projection"))
	_, failed := s.FailedResources()[id]
	assert.Equal(t, failed, want.Obj != nil)
	var replayed manifest.BaseManifest
	s.AddListener(EventObjectAdded, func(got manifest.NamedResource, data any) {
		if got == id {
			replayed = data.(manifest.BaseManifest)
		}
	}, true)()
	assert.Equal(t, replayed, want.Obj)
}

func TestRecordEmitted_Replacement(t *testing.T) {
	t.Parallel()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
	a, b := emissionChild("a", "old"), emissionChild("b", "new")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	accept(
		t,
		s,
		p,
		&Emission{
			Children:  []Child{a},
			Manifests: testutil.DocumentOccurrences([]map[string]any{a.Doc}),
			Complete:  true,
		},
	)
	accept(
		t,
		s,
		q,
		&Emission{
			Children:  []Child{a},
			Manifests: testutil.DocumentOccurrences([]map[string]any{a.Doc}),
			Complete:  true,
		},
	)
	{
		got := s.Productions(a.Obj.Named(), nil)
		assert.FatalIf(t, len(got) != 2, "independent supports: %v", got)
	}
	bad := emissionChild("a", "unequal")
	s.RecordEmitted(q, &Emission{Children: []Child{b, bad}, Complete: true}, nil)
	assert.FatalIf(
		t,
		s.GetObject(b.Obj.Named()) != b.Obj || s.GetObject(bad.Obj.Named()) != bad.Obj,
		"%s",
		"unequal contribution lost accepted content",
	)
	accept(t, s, p, &Emission{Complete: true})
	assert.FatalIf(t, s.GetObject(a.Obj.Named()) == nil, "%s", "withdrawal lost surviving supporter")
	accept(t, s, q, &Emission{Complete: true})
	assert.FatalIf(t, s.GetObject(a.Obj.Named()) != nil, "%s", "complete empty retained unsupported output")
	accept(
		t,
		s,
		p,
		&Emission{
			Children:  []Child{a},
			Manifests: testutil.DocumentOccurrences([]map[string]any{a.Doc}),
			Complete:  true,
		},
	)
	s.UpdateFailure(p, manifest.NewError(manifest.ErrInput, nil, "failed successor"))
	_, _, attempt := s.GetEmission(p)
	assert.FatalIf(t, len(attempt.Manifests) != 1 || attempt.Complete, "failure lost accepted output: %+v", attempt)
	attempt.Children = []Child{b}
	attempt.Manifests = testutil.DocumentOccurrences([]map[string]any{b.Doc})
	accept(t, s, p, &attempt)
	assert.FatalIf(
		t,
		s.GetObject(a.Obj.Named()) == nil || s.GetObject(b.Obj.Named()) == nil,
		"%s",
		"partial update lost predecessor",
	)
	attempt.Finish()
	_, _, progress := s.GetEmission(p)
	progress.Children = nil
	accept(t, s, p, &progress)
	_, _, retained := s.GetEmission(p)
	assert.FatalIf(
		t,
		len(retained.Manifests) != 2,
		"inherited partial documents duplicated: %d",
		len(retained.Manifests),
	)
	progress.Finish()
	for _, tc := range []struct {
		name string
		edit func(*Emission)
	}{
		{"unreadable", func(e *Emission) { e.Unreadable[0] = b.Obj.Named() }},
		{"child", func(e *Emission) { e.Children[0] = b }},
		{"documents", func(e *Emission) { e.Manifests[0] = manifest.DocOccurrence{Document: b.Doc} }},
		{"complete", func(e *Emission) { e.Complete = true }},
		{"binding", func(e *Emission) { e.binding.artifact = &SourceArtifact{Key: "new"} }},
		{"outer_slices", func(e *Emission) {
			e.Children[0], e.Manifests[0], e.Unreadable[0] = b, manifest.DocOccurrence{Document: b.Doc}, b.Obj.Named()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedReplacementCase(t, &testRecordEmittedReplacementCaseInputs{p: &p, a: &a, tc: &tc})
		},
		)
	}
}

func TestRecordEmitted_ConcurrentSupport(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			t.Parallel()
			testRecordEmittedConcurrentSupportCase(t, &workers)
		},
		)
	}
}

func testRecordEmittedParkedSnapshotReplayCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	s, tasks := New(), task.NewBounded((*testCaseInput))
	parent := ownerID("parent")
	s.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
	old := emissionChild("settings", "old")
	s.RecordEmitted(parent, &Emission{Children: []Child{old}, Complete: true}, nil)
	s.UpdateStatus(old.Obj.Named(), StatusInfo{Status: StatusReady})
	captured, release := make(chan struct{}, (*testCaseInput)), make(chan struct{})
	results := make(chan error, (*testCaseInput))
	for range *testCaseInput {
		tasks.Go(t.Context(), "snapshot-consumer", func(context.Context) {
			obj, observation := s.Snapshot(old.Obj.Named(), true)
			captured <- struct{}{}
			<-release
			if obj != old.Obj || obj.(*manifest.ConfigMap).Data["value"] != "old" ||
				len(observation.Conditions) != 1 ||
				observation.Conditions[0].Status != "True" {
				results <- fmt.Errorf("retained snapshot changed: object=%v observation=%+v", obj, observation)
				return
			}
			results <- nil
		})
	}
	for range *testCaseInput {
		<-captured
	}
	fresh := emissionChild("settings", "new")
	s.RecordEmitted(parent, &Emission{Children: []Child{fresh}, Complete: true}, nil)
	assert.Equal(t, s.GetObject(fresh.Obj.Named()), fresh.Obj)
	var replay []string
	unsubscribe := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id == fresh.Obj.Named() && payload != nil {
			replay = append(replay, payload.(*manifest.ConfigMap).Data["value"].(string))
		}
	}, true)
	assert.Diff(t, replay, []string{"new"})
	unsubscribe()
	s.RecordEmitted(parent, &Emission{Complete: true}, nil)
	assert.Equal(t, s.GetObject(fresh.Obj.Named()), nil)
	close(release)
	tasks.BlockTillDone()
	for range *testCaseInput {
		assert.NoError(t, <-results)
	}
	assert.Equal(t, tasks.Failures(), int64(0))
	assert.Equal(t, len(s.ListObjects("", manifest.KindConfigMap)), 0)
	_, _, emitted := s.GetEmission(parent)
	defer emitted.Finish()
	assert.Equal(t, len(emitted.Children), 0)
}

type testRecordEmittedReplacementCaseInputs struct {
	p  *manifest.NamedResource
	a  *Child
	tc *struct {
		name string
		edit func(*Emission)
	}
}

func testRecordEmittedReplacementCase(t *testing.T, testCaseInput *testRecordEmittedReplacementCaseInputs) {
	t.Helper()

	s := New()
	s.AddObject(&manifest.Kustomization{Name: testCaseInput.p.Name, Namespace: testCaseInput.p.Namespace})
	_, _, initial := s.GetEmission((*testCaseInput.p))
	initial.Children, initial.Manifests = []Child{
		(*testCaseInput.a),
	}, testutil.DocumentOccurrences(
		[]map[string]any{testCaseInput.a.Doc},
	)
	initial.Unreadable, initial.Fingerprint = []manifest.NamedResource{testCaseInput.a.Obj.Named()}, "same"
	initial.Complete = testCaseInput.tc.name != "complete"
	s.RecordEmitted((*testCaseInput.p), &initial, nil)
	predecessor := s.GetArtifact((*testCaseInput.p), ArtifactContent).(*Emission)
	_, _, attempt := s.GetEmission((*testCaseInput.p))
	defer attempt.Finish()
	attempt.Complete, attempt.Fingerprint = initial.Complete, "same"
	attempt.Children, attempt.Manifests, attempt.Unreadable = slices.Clone(
		attempt.Children,
	), slices.Clone(
		attempt.Manifests,
	), slices.Clone(
		attempt.Unreadable,
	)
	testCaseInput.tc.edit(&attempt)
	checkPredecessor := func() {
		t.Helper()
		assert.FatalIf(t, predecessor.Children[0].Obj != testCaseInput.a.Obj ||
			!manifest.ValueEqual(predecessor.Manifests[0].Document, testCaseInput.a.Doc) ||
			predecessor.Unreadable[0] != testCaseInput.a.Obj.Named(), "%s", "captured outer slice changed accepted predecessor")
	}
	checkPredecessor()
	var events int
	defer s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == (*testCaseInput.p) {
			events++
		}
	}, false)()
	s.RecordEmitted((*testCaseInput.p), &attempt, nil)
	accepted := s.GetArtifact((*testCaseInput.p), ArtifactContent).(*Emission)
	assert.FatalIf(
		t,
		events != 1 || accepted == predecessor || accepted.Fingerprint != "same",
		"%s",
		"equal fingerprint discarded changed emission",
	)
	assert.FatalIf(
		t,
		!slices.Equal(accepted.Unreadable, attempt.Unreadable) || accepted.Complete != attempt.Complete ||
			accepted.binding != attempt.binding,
		"%s",
		"changed emission lost output or completion context",
	)
	assert.FatalIf(t, !slices.EqualFunc(
		accepted.Children,
		attempt.Children,
		func(a, b Child) bool { return a.Obj == b.Obj },
	) ||
		!slices.EqualFunc(
			accepted.Manifests,
			attempt.Manifests,
			func(a, b manifest.DocOccurrence) bool { return manifest.ValueEqual(a.Document, b.Document) },
		), "%s", "changed emission lost captured children or documents")
	checkPredecessor()
	_, _, replay := s.GetEmission((*testCaseInput.p))
	defer replay.Finish()
	replay.binding = accepted.binding
	replay.Complete = accepted.Complete
	s.RecordEmitted((*testCaseInput.p), &replay, nil)
	assert.FatalIf(
		t,
		events != 1 || s.GetArtifact((*testCaseInput.p), ArtifactContent) != accepted,
		"%s",
		"equal replay republished accepted output",
	)
}

func testRecordEmittedConcurrentSupportCase(t *testing.T, testCaseInput *int,
) {
	t.Helper()

	s := New()
	p := ownerID("p")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	covered := emissionChild("covered", "static")
	s.AddObject(covered.Obj)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		covered.Obj.Named(): {
			Productions: []manifest.Production{{Owner: emissionChild("0", "value").Obj.Named()}},
		},
	}})
	start := make(chan struct{})
	prepared := make(chan struct{}, (*testCaseInput))
	var wg sync.WaitGroup
	for i := range *testCaseInput {
		wg.Go(func() {
			_, _, next := s.GetEmission(p)
			defer next.Finish()
			c := emissionChild(strconv.Itoa(i), "value")
			next.Children, next.Manifests = []Child{c}, testutil.DocumentOccurrences([]map[string]any{c.Doc})
			prepared <- struct{}{}
			<-start
			accept(t, s, p, &next)
		})
	}
	for range *testCaseInput {
		<-prepared
	}
	close(start)
	wg.Wait()
	_, _, attempt := s.GetEmission(p)
	sh := s.shardFor(p)
	sh.mu.RLock()
	accepted := sh.slots[p].current.Load()
	sh.mu.RUnlock()
	if len(accepted.Children) != (*testCaseInput) || len(accepted.Manifests) != (*testCaseInput) {
		t.Fatalf(
			"lost concurrent partial entries: children=%d documents=%d",
			len(accepted.Children),
			len(accepted.Manifests),
		)
	}
	attempt.Finish()
	q := ownerID("q")
	s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
	accept(t, s, q, &Emission{Children: accepted.Children, Manifests: accepted.Manifests, Complete: true})
	start = make(chan struct{})
	for i := range *testCaseInput {
		wg.Go(func() {
			<-start
			next := &Emission{
				Children:  slices.Clone(accepted.Children),
				Manifests: slices.Clone(accepted.Manifests),
				Complete:  true,
			}
			for j, child := range next.Children {
				doc := manifest.DeepCopyMap(child.Doc)
				doc["metadata"].(map[string]any)["labels"] = map[string]any{
					"kustomize.toolkit.fluxcd.io/name": strconv.Itoa(i),
				}
				next.Children[j].Doc = doc
				next.Manifests[j] = manifest.DocOccurrence{Document: doc}
			}
			if i%2 == 0 {
				slices.Reverse(next.Children)
				slices.Reverse(next.Manifests)
			}
			accept(t, s, p, next)
		})
	}
	capturedID := accepted.Children[0].Obj.Named()
	captured := s.Productions(capturedID, nil)
	marker := emissionChild("reader-anchor", "value")
	wg.Go(func() {
		<-start
		observed := make([]manifest.Production, 0, len(captured))
		{
			proofValues := captured
			for proofIndex := range proofValues {
				proof := proofValues[proofIndex]
				observed = append(observed, proof)
				s.AddObject(marker.Obj)
			}
		}
		assert.ErrorIf(
			t,
			!slices.Equal(observed, []manifest.Production{{Owner: p}, {Owner: q}}),
			"concurrent captured productions=%v",
			observed,
		)
		testRecordEmittedConcurrentSupportReads(t, &testRecordEmittedConcurrentSupportReadState{
			s: &s, p: &p, q: &q, accepted: &accepted,
		})
	})
	close(start)
	wg.Wait()
	for _, child := range accepted.Children {
		id := child.Obj.Named()
		{
			got := s.GetObject(id)
			assert.FatalIf(t, got == nil || got.Named() != id, "%s", "reverse position lost object")
		}
		obj, _ := s.Snapshot(id, true)
		assert.FatalIf(t, obj == nil || obj.Named() != id, "%s", "reverse position changed snapshot identity")
		{
			got := s.Productions(id, nil)
			assert.FatalIf(
				t,
				len(got) != 2 || got[0].Owner != p || got[1].Owner != q,
				"reverse position changed production: %v",
				got,
			)
		}
	}
	testRecordEmittedConcurrentSupportCaseCompletion(
		t,
		&testRecordEmittedConcurrentSupportCaseCompletionState{
			testCaseInput: testCaseInput,
			s:             s,
			p:             &p,
			covered:       &covered,
			accepted:      accepted,
			q:             &q,
			captured:      &captured,
		},
	)
}

type testRecordEmittedConcurrentSupportCaseCompletionState struct {
	testCaseInput *int
	s             *Store
	p             *manifest.NamedResource
	covered       *Child
	accepted      *Emission
	q             *manifest.NamedResource
	captured      *[]manifest.Production
}

func testRecordEmittedConcurrentSupportCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedConcurrentSupportCaseCompletionState,
) {
	t.Helper()
	assert.Diff(t, (*scope.captured), []manifest.Production{{Owner: (*scope.p)}, {Owner: (*scope.q)}})
	scope.s.SetArtifact(scope.covered.Obj.Named(), &SourceArtifact{Key: "covered"})
	scope.s.UpdateStatus(scope.covered.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	accept(t, scope.s, (*scope.p), &Emission{Complete: true})
	for _, child := range scope.accepted.Children {
		parent, ok := scope.s.RenderedParent(child.Obj.Named(), nil)
		assert.FatalIf(
			t,
			!ok || parent != (*scope.q),
			"withdrawal parent=%v/%t, want %v/true",
			parent,
			ok,
			(*scope.q),
		)
		{
			got := scope.s.Productions(child.Obj.Named(), nil)
			assert.FatalIf(
				t,
				len(got) != 1 || got[0].Owner != (*scope.q),
				"withdrawal changed surviving contributor: %v",
				got,
			)
		}
	}
	assert.FatalIf(t, scope.s.GetObject(scope.covered.Obj.Named()) != scope.covered.Obj ||
		scope.s.GetArtifact(
			scope.covered.Obj.Named(),
			ArtifactContent,
		) == nil, "%s", "surviving Q support lost frozen descendant")
	{
		info, ok := scope.s.GetStatus(scope.covered.Obj.Named())
		assert.FatalIf(t, !ok || info.Status != StatusReady, "%s", "surviving Q support lost Ready")
	}
	accept(t, scope.s, (*scope.q), &Emission{Complete: true})
	assert.FatalIf(
		t,
		scope.s.GetObject(scope.covered.Obj.Named()) != nil ||
			scope.s.GetArtifact(scope.covered.Obj.Named(), ArtifactContent) != nil,
		"%s",
		"last boundary support left frozen descendant live",
	)
	{
		_, ok := scope.s.GetStatus(scope.covered.Obj.Named())
		assert.FatalIf(t, ok, "%s", "last boundary support left frozen descendant Ready")
	}
	for _, labels := range []bool{false, true} {
		t.Run(fmt.Sprintf("stable_replay/labels_%t", labels), func(t *testing.T) {
			t.Parallel()

			testRecordEmittedStableSupportReplay(t, scope, &labels)
		})
	}
}

type testRecordEmittedConcurrentSupportReadState struct {
	s        **Store
	p        *manifest.NamedResource
	q        *manifest.NamedResource
	accepted **Emission
}

func testRecordEmittedConcurrentSupportReads(t *testing.T, scope *testRecordEmittedConcurrentSupportReadState) {
	t.Helper()
	for _, child := range (*scope.accepted).Children {
		id := child.Obj.Named()
		parent, ok := (*scope.s).RenderedParent(id, nil)
		assert.ErrorIf(
			t,
			!ok || parent != (*scope.p),
			"concurrent parent=%v/%t, want %v/true",
			parent,
			ok,
			(*scope.p),
		)
		{
			got := (*scope.s).Productions(
				id,
				nil,
			)
			assert.ErrorIf(t, !slices.Equal(
				got,
				[]manifest.Production{{Owner: (*scope.p)}, {Owner: (*scope.q)}},
			), "concurrent productions=%v", got)
		}
		{
			obj := (*scope.s).GetObject(id)
			assert.ErrorIf(t, obj == nil || obj.Named() != id, "%s", "concurrent object lost identity")
		}
		obj, _, next := (*scope.s).GetEmission(id)
		next.Finish()
		assert.ErrorIf(
			t,
			obj == nil || obj.Named() != id || (*scope.s).EmissionOwner(id) != (*scope.q),
			"%s",
			"concurrent rich read lost representative",
		)
		assert.ErrorIf(t, !slices.ContainsFunc(
			(*scope.s).ListObjects(id.Group, id.Kind),
			func(obj manifest.BaseManifest) bool { return obj.Named() == id },
		), "%s", "concurrent listing lost child")
	}
}

func testRecordEmittedStableSupportReplay(
	t *testing.T,
	scope *testRecordEmittedConcurrentSupportCaseCompletionState,
	labels *bool,
) {
	t.Helper()
	s := New()
	producer := &manifest.Kustomization{Name: "stable", Namespace: "apps"}
	s.AddObject(producer)
	child := emissionChild("stable", "value")
	_, _, initial := s.GetEmission(producer.Named())
	initial.Children, initial.Manifests, initial.Complete = []Child{
		child,
	}, testutil.DocumentOccurrences(
		[]map[string]any{child.Doc},
	), true
	s.RecordEmitted(producer.Named(), &initial, nil)
	producerShard, childShard := s.shardFor(producer.Named()), s.shardFor(child.Obj.Named())
	targets := manifest.ProducerTargets(child.Obj)
	involved := make([]*shard, 2, 2+len(targets))
	involved[0], involved[1] = producerShard, childShard
	for _, target := range targets {
		involved = append(involved, s.shardFor(target.NamedResource))
	}
	var unrelated *shard
	for _, candidate := range s.shards {
		if !slices.Contains(involved, candidate) {
			unrelated = candidate
			break
		}
	}
	assert.FatalIf(
		t,
		unrelated == nil || slices.Contains(involved, unrelated),
		"%s",
		"fixture has no unrelated shard",
	)
	start, ready, done := make(
		chan struct{},
	), make(
		chan struct{},
		(*scope.testCaseInput),
	), make(
		chan struct{},
	)
	var replay sync.WaitGroup
	for i := range *scope.testCaseInput {
		replay.Go(func() {
			_, _, next := s.GetEmission(producer.Named())
			defer next.Finish()
			next.Complete = true
			if *labels {
				next.Children, next.Manifests = slices.Clone(
					next.Children,
				), slices.Clone(
					next.Manifests,
				)
				doc := manifest.DeepCopyMap(child.Doc)
				doc["metadata"].(map[string]any)["labels"] = map[string]any{
					"kustomize.toolkit.fluxcd.io/name": strconv.Itoa(i),
				}
				next.Children[0].Doc, next.Manifests[0] = doc, manifest.DocOccurrence{Document: doc}
			}
			ready <- struct{}{}
			<-start
			s.RecordEmitted(producer.Named(), &next, nil)
		})
	}
	for range *scope.testCaseInput {
		<-ready
	}
	unrelated.mu.Lock()
	close(start)
	go func() { replay.Wait(); close(done) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	blocked := false
	select {
	case <-done:
	case <-deadline.C:
		blocked = true
	}
	unrelated.mu.Unlock()
	<-done
	assert.FatalIf(t, blocked, "%s", "stable replay waited for an unrelated shard")
}
