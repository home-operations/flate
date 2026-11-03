package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func TestRecordEmitted_RetiresOnlyReplacedOutputBorrower(t *testing.T) {
	t.Parallel()
	for _, payload := range []bool{false, true} {
		for _, mode := range []string{"partial", "rebound", "keyed_replay", "rejected"} {
			t.Run(fmt.Sprintf("payload=%t/%s", payload, mode), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedRetiresOnlyReplacedOutputBorrowerCase(
					t,
					&testRecordEmittedRetiresOnlyReplacedOutputBorrowerCaseInputs{payload: &payload, mode: &mode},
				)
			},
			)
		}
	}
}

func TestRecordEmitted_RejectedPublicationPreservesOutputWatches(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"stable", "replacement"} {
		for _, rejection := range []string{"invalid_reads", "stale_generation"} {
			t.Run(route+"/"+rejection, func(t *testing.T) {
				t.Parallel()
				testRecordEmittedRejectedPublicationPreservesOutputWatchesCase(
					t,
					&testRecordEmittedRejectedPublicationPreservesOutputWatchesCaseInputs{
						route:     &route,
						rejection: &rejection,
					},
				)
			},
			)
		}
	}
}

func TestReadSet_CloseRetiresHiddenBorrowedOutput(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"changed", "missing", "source_hidden"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testReadSetCloseRetiresHiddenBorrowedOutputCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func TestRecordEmitted_IndirectAuthorityArrival(t *testing.T) {
	t.Parallel()
	st := New()
	root := &manifest.Kustomization{Name: "root", Namespace: "apps"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	child := &manifest.OCIRepository{Name: "child", Namespace: "apps", URL: "oci://accepted.example"}
	st.AddObject(root)
	accept(t, st, root.Named(), &Emission{Children: []Child{{Obj: producer}}, Complete: true})
	accept(t, st, producer.Named(), &Emission{Children: []Child{{Obj: child}}, Complete: true})
	var arrivals atomic.Int64
	unsub := st.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == child.Named() {
			arrivals.Add(1)
		}
	}, false)
	defer unsub()
	accept(
		t,
		st,
		root.Named(),
		&Emission{Children: []Child{{Obj: producer.Clone()}, emissionChild("sibling", "new")}, Complete: true},
	)
	if st.GetDeclaration(child.Named()) != child || arrivals.Load() != 0 {
		t.Fatalf("unchanged descendant arrivals=%d, want 0", arrivals.Load())
	}
}

func TestGetDeclaration_CoveredAuthority(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"covered", "failed", "empty", "accepted", "accepted failed", "surviving", "withdrawn", "self", "independent", "root"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testGetDeclarationCoveredAuthorityCase(t, &state)
		},
		)
	}
}

func TestRecordEmitted_ReplacedInputRejected(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"entry_stable", "entry_replacement", "locked_stable", "locked_replacement"} {
		for _, loss := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/loss_%t", route, loss), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedReplacedInputRejectedCase(
					t,
					&testRecordEmittedReplacedInputRejectedCaseInputs{route: &route, loss: &loss},
				)
			},
			)
		}
	}
}

func TestRecordEmitted_EqualDeclarationRetainsCapture(t *testing.T) {
	t.Parallel()
	st := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	input := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
	st.AddObject(parent)
	accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: input}}, Fingerprint: "parent-A", Complete: true})
	captured, _, inventory := st.GetEmission(input.Named())
	defer inventory.Finish()
	{
		assert.FatalIf(t, captured != input, "%s", "fixture did not capture accepted child")
	}
	var arrivals atomic.Int64
	stop := st.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == input.Named() {
			arrivals.Add(1)
		}
	}, false)
	defer stop()
	equivalent := input.Clone()
	accept(
		t,
		st,
		parent.Named(),
		&Emission{Children: []Child{{Obj: equivalent}}, Fingerprint: "parent-B", Complete: true},
	)
	if !manifest.Equal(st.GetDeclaration(input.Named()), captured) || arrivals.Load() != 0 {
		t.Fatalf("equal publication invalidated capture without arrival: arrivals=%d", arrivals.Load())
	}
	inventory.Fingerprint, inventory.Complete = "child-A", true
	st.RecordEmitted(input.Named(), &inventory, nil)
}

func TestRecordEmitted_SameOutputUsesCurrentInput(t *testing.T) {
	t.Parallel()
	s := New()
	a := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "A"}
	s.AddObject(a)
	_, _, first := s.GetEmission(a.Named())
	defer first.Finish()
	first.Children = []Child{emissionChild("child", "same")}
	first.Fingerprint, first.Complete = manifest.Fingerprint("same"), true
	s.RecordEmitted(a.Named(), &first, nil)
	b := a.Clone()
	b.Path = "B"
	s.AddObject(b)
	_, _, next := s.GetEmission(b.Named())
	defer next.Finish()
	next.Complete, next.Fingerprint = true, first.Fingerprint
	s.RecordEmitted(b.Named(), &next, nil)
	current := s.GetArtifact(b.Named(), ArtifactContent).(*Emission)
	if current.input != b || current.Fingerprint != first.Fingerprint ||
		s.GetObject(first.Children[0].Obj.Named()) == nil {
		t.Fatalf("equal output retained obsolete input: input=%v fingerprint=%q", current.input, current.Fingerprint)
	}
}

func TestReferenceFacts_PendingPeerAfterEmptySupplier(t *testing.T) {
	t.Parallel()
	s := New()
	first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
	empty := &manifest.Kustomization{Name: "empty", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{first, empty})
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{{Owner: first.Named()}, {Owner: empty.Named()}}},
	}})
	s.UpdateStatus(first.Named(), StatusInfo{Status: StatusPending, Reason: "", Message: "pending supplier"})
	_, _, next := s.GetEmission(empty.Named())
	defer next.Finish()
	next.Complete = true
	s.RecordEmitted(empty.Named(), &next, nil)
	facts := s.ReferenceFacts(target, nil)
	{
		assert.FatalIf(
			t,
			len(facts.Productions) != 1 || facts.Productions[0].Owner != first.Named() || facts.Data != nil ||
				facts.Accepted,
			"pending peer lost after empty supplier: facts=%+v",
			facts,
		)
	}
}

type testRecordEmittedRetiresOnlyReplacedOutputBorrowerCaseInputs struct {
	payload *bool
	mode    *string
}

func testRecordEmittedRetiresOnlyReplacedOutputBorrowerCase(
	t *testing.T,
	testCaseInput *testRecordEmittedRetiresOnlyReplacedOutputBorrowerCaseInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	var first, second ReadSet
	first.Init(s, owner.Named(), nil)
	second.Init(s, owner.Named(), nil)
	defer first.Retire()
	defer second.Retire()
	_, _, output := first.GetEmission(owner.Named())
	child := emissionChild("output", "old")
	output.Children, output.Manifests = []Child{
		child,
	}, testutil.DocumentOccurrences(
		[]map[string]any{child.Doc},
	)
	output.Fingerprint = manifest.Fingerprint(output.Manifests)
	output.Complete = true
	first.Bind(&output)
	var payloads map[manifest.NamedResource]*SourceArtifact
	if *testCaseInput.payload {
		payloads = map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "content"}}
	}
	s.RecordEmitted(owner.Named(), &output, payloads)
	output.Finish()
	_, _, next := second.GetEmission(owner.Named())
	defer next.Finish()
	next.Complete = (*testCaseInput.mode) != "partial"
	if (*testCaseInput.mode) == "keyed_replay" {
		next.Fingerprint = output.Fingerprint
	}
	if (*testCaseInput.mode) == "rejected" {
		child = emissionChild("output", "new")
		next.Children, next.Manifests = []Child{
			child,
		}, testutil.DocumentOccurrences(
			[]map[string]any{child.Doc},
		)
		next.Fingerprint = manifest.Fingerprint(next.Manifests)
		sh := s.shardFor(owner.Named())
		sh.mu.Lock()
		sh.slots[owner.Named()].generation = ^uint64(0)
		sh.mu.Unlock()
	}
	second.Bind(&next)
	if (*testCaseInput.mode) == "rejected" {
		func() {
			defer func() {
				{
					message, ok := recover().(string)
					assert.ErrorIf(t, !ok ||
						message != "store: authority generation exhausted", "%s", "expected exhausted publication rejection")
				}
			}()
			s.RecordEmitted(owner.Named(), &next, nil)
		}()
	} else {
		s.RecordEmitted(owner.Named(), &next, nil)
	}
	current := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	if (*testCaseInput.mode) == "rebound" || (*testCaseInput.mode) == "keyed_replay" && (*testCaseInput.payload) {
		{
			assert.FatalIf(
				t,
				!first.retired.Load() || current.OutputReads != &second,
				"%s",
				"accepted rebound retained the predecessor borrower",
			)
		}
	} else {
		assert.FatalIf(
			t,
			first.retired.Load() || current.OutputReads != &first,
			"%s",
			"publication retired an output borrower it did not replace",
		)
	}
	{
		assert.FatalIf(t, !current.OutputReads.Valid(), "%s", "current output lost valid provenance")
	}
}

type testRecordEmittedRejectedPublicationPreservesOutputWatchesCaseInputs struct {
	route     *string
	rejection *string
}

func testRecordEmittedRejectedPublicationPreservesOutputWatchesCase(
	t *testing.T,
	testCaseInput *testRecordEmittedRejectedPublicationPreservesOutputWatchesCaseInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	a := &manifest.Kustomization{Name: "input-a", Namespace: "apps", Path: "old"}
	b := &manifest.Kustomization{Name: "input-b", Namespace: "apps", Path: "old"}
	s.AddObjects([]manifest.BaseManifest{owner, a, b})
	var first, second ReadSet
	first.Init(s, owner.Named(), nil)
	second.Init(s, owner.Named(), nil)
	defer first.Retire()
	defer second.Retire()
	_, _, output := first.GetEmission(owner.Named())
	assert.Equal(t, first.GetObject(a.Named()), manifest.BaseManifest(a))
	child := emissionChild("output", "accepted")
	output.Children, output.Manifests = []Child{
		child,
	}, testutil.DocumentOccurrences(
		[]map[string]any{child.Doc},
	)
	output.Fingerprint = manifest.Fingerprint(output.Manifests)
	output.Complete = true
	first.Bind(&output)
	s.RecordEmitted(owner.Named(), &output, nil)
	output.Finish()
	accepted := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)

	tasks := task.NewBounded(2)
	captured, stale := make(chan struct{}), make(chan struct{})
	var next Emission
	tasks.Go(t.Context(), "publish", func(context.Context) {
		_, _, next = second.GetEmission(owner.Named())
		defer next.Finish()
		assert.Equal(t, second.GetObject(b.Named()), manifest.BaseManifest(b))
		proposedChild := emissionChild("output", "proposed")
		if (*testCaseInput.route) == "replacement" {
			proposedChild = emissionChild("replacement", "proposed")
		}
		next.Children, next.Manifests = []Child{
			proposedChild,
		}, testutil.DocumentOccurrences(
			[]map[string]any{proposedChild.Doc},
		)
		next.Fingerprint = manifest.Fingerprint(next.Manifests)
		next.Complete = true
		second.Bind(&next)
		close(captured)
		tasks.YieldSlot(func() { <-stale })
		if (*testCaseInput.rejection) == "stale_generation" {
			{
				assert.ErrorIf(
					t,
					!second.Valid() || next.inputHead.generation == output.inputHead.generation,
					"%s",
					"fixture did not isolate stale generation with valid reads",
				)
			}
		} else {
			assert.ErrorIf(t, second.Valid(), "%s", "fixture did not invalidate proposal reads")
		}
		s.RecordEmitted(owner.Named(), &next, nil)
	})
	tasks.Go(t.Context(), "stale", func(context.Context) {
		defer close(stale)
		tasks.YieldSlot(func() { <-captured })
		if (*testCaseInput.rejection) == "invalid_reads" {
			changed := b.Clone()
			changed.Path = "new"
			s.AddObject(changed)
		} else {
			next.inputHead.generation--
		}
	})
	tasks.BlockTillDone()
	{
		assert.FatalIf(t, tasks.Failures() != 0, "%s", "publication task panicked")
	}
	current := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	{
		assert.FatalIf(
			t,
			current != accepted || current.OutputReads != &first,
			"%s",
			"rejection replaced the accepted emission or borrower",
		)
	}
	{
		assert.ErrorIf(
			t,
			first.retired.Load() || !first.Valid(),
			"%s",
			"rejection retired or invalidated the accepted output borrower",
		)
	}
	for id := range s.Sweep() {
		t.Errorf("rejection invalidated unchanged accepted owner %v", id)
	}
	_, _, retained := s.GetEmission(owner.Named())
	defer retained.Finish()
	{
		diff := cmp.Diff(retained.Children, []Child{child}, cmp.AllowUnexported(Child{}))
		assert.FatalIf(t, diff != "", "rejection changed accepted children (-want +got): %s", diff)
	}
	assert.Diff(t, retained.Manifests, output.Manifests)

	changed := a.Clone()
	changed.Path = "new"
	s.AddObject(changed)
	{
		delivered := slices.Collect(s.Sweep())
		assert.ErrorIf(
			t,
			!slices.Contains(delivered, owner.Named()),
			"%s",
			"accepted input change did not deliver its owner through Sweep",
		)
	}
	{
		assert.ErrorIf(t, first.Valid(), "%s", "accepted output remained valid after its input changed")
	}
	_, _, invalid := s.GetEmission(owner.Named())
	defer invalid.Finish()
	{
		assert.FatalIf(
			t,
			len(invalid.Children) != 0 || len(invalid.Manifests) != 0,
			"%s",
			"invalid accepted output retained children or manifests",
		)
	}
}

func testReadSetCloseRetiresHiddenBorrowedOutputCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	var declaration manifest.BaseManifest = &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "old"}
	if (*testCaseInput) == "source_hidden" {
		declaration = &manifest.GitRepository{Name: "consumer", Namespace: "apps"}
	}
	s.AddObject(parent)
	for i := range 64 {
		s.AddObject(&manifest.ConfigMap{Name: fmt.Sprintf("input-%d", i), Namespace: parent.Namespace})
	}
	s.UpdateStatus(parent.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	publish := func(children ...Child) {
		s.RecordEmitted(parent.Named(), &Emission{Children: children, Complete: true}, nil)
	}
	publish(Child{Obj: declaration})
	var first, second ReadSet
	first.Init(s, declaration.Named(), nil)
	defer first.Retire()
	first.GetObject(declaration.Named())
	first.GetStatus(parent.Named())
	first.RequireData(parent.Named())
	first.ListObjects("", manifest.KindConfigMap)
	if (*testCaseInput) == "source_hidden" {
		s.SetArtifact(declaration.Named(), &SourceArtifact{Reads: &first, Key: "content"})
	} else {
		_, _, output := first.GetEmission(declaration.Named())
		child := emissionChild("output", "old")
		output.Children, output.Manifests = []Child{
			child,
		}, testutil.DocumentOccurrences(
			[]map[string]any{child.Doc},
		)
		output.Complete = true
		first.Bind(&output)
		s.RecordEmitted(declaration.Named(), &output, nil)
		output.Finish()
	}
	first.Accept()
	if (*testCaseInput) == "changed" {
		changed := *declaration.(*manifest.Kustomization)
		changed.Path = "new"
		publish(Child{Obj: &changed})
	} else {
		publish()
	}
	{
		assert.FatalIf(
			t,
			s.GetArtifact(declaration.Named(), ArtifactContent) != nil,
			"%s",
			"changed parent input left the old artifact visible",
		)
	}
	second.Init(s, declaration.Named(), nil)
	second.GetObject(declaration.Named())
	second.Close()
	for range s.Sweep() {
	}
	testReadSetCloseRetiresHiddenBorrowedOutputCaseCompletion(t, &testReadSetCloseRetiresHiddenBorrowedOutputCaseCompletionState{testCaseInput: testCaseInput, s: s, parent: parent, declaration: &declaration, first: &first})
}

func testGetDeclarationCoveredAuthorityCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	peer := &manifest.Kustomization{Name: "peer", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "source", Namespace: "apps", URL: "oci://raw.example"}
	st.AddObjects([]manifest.BaseManifest{parent, peer, raw})
	proofs := []manifest.Production{
		{
			Owner:    parent.Named(),
			Source:   manifest.BootstrapSourceID,
			Authored: raw.Named(),
			Path:     "/input/source.yaml",
			File:     "source.yaml",
		},
	}
	switch *testCaseInput {
	case "self":
		proofs = append(proofs, manifest.Production{Owner: raw.Named()})
	case "independent":
		proofs = append(proofs, manifest.Production{Source: manifest.BootstrapSourceID})
	}
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: proofs}},
		},
	)
	st.UpdateStatus(parent.Named(), StatusInfo{Status: StatusPending, Reason: "", Message: "other children"})
	var want manifest.BaseManifest
	switch *testCaseInput {
	case "self", "independent":
		want = raw
	case "root":
		accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: raw}}})
		st.AddObject(raw)
		want = raw
	case "failed":
		st.UpdateStatus(
			parent.Named(),
			StatusInfo{Status: StatusFailed, Reason: "", Message: "failed before child"},
		)
	case "empty":
		accept(t, st, parent.Named(), &Emission{Complete: true})
	case "accepted", "accepted failed", "surviving", "withdrawn":
		accepted := *raw
		accepted.URL = "oci://accepted.example"
		accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: &accepted}}})
		want = &accepted
		if (*testCaseInput) == "accepted failed" {
			st.UpdateStatus(
				parent.Named(),
				StatusInfo{Status: StatusFailed, Reason: "", Message: "failed successor"},
			)
		}
		if (*testCaseInput) == "surviving" {
			accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: &accepted}}})
		}
		if (*testCaseInput) == "surviving" || (*testCaseInput) == "withdrawn" {
			accept(t, st, parent.Named(), &Emission{Complete: true})
		}
		if (*testCaseInput) == "withdrawn" {
			want = nil
		}
	}
	{
		got := st.GetDeclaration(raw.Named())
		assert.FatalIf(t, got != want, "effective=%v want=%v", got, want)
	}
	got, _, inventory := st.GetEmission(raw.Named())
	defer inventory.Finish()
	{
		assert.FatalIf(t, got != want, "emission input=%v want=%v", got, want)
	}
	{
		assert.FatalIf(
			t,
			(*testCaseInput) == "covered" && st.GetObject(raw.Named()) != raw,
			"%s",
			"coverage hid raw admission identity",
		)
	}
}

type testRecordEmittedReplacedInputRejectedCaseInputs struct {
	route *string
	loss  *bool
}

func testRecordEmittedReplacedInputRejectedCase(
	t *testing.T,
	testCaseInput *testRecordEmittedReplacedInputRejectedCaseInputs,
) {
	t.Helper()

	st := New()
	owner, peer := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "apps",
	}, &manifest.Kustomization{
		Name:      "peer",
		Namespace: "apps",
	}
	input := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "A"}
	st.AddObjects([]manifest.BaseManifest{owner, peer, input})
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				input.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
			},
		},
	)
	accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: input}}, Complete: true})
	child := emissionChild("prior", "A")
	accept(
		t,
		st,
		input.Named(),
		&Emission{
			Children:  []Child{child},
			Manifests: testutil.DocumentOccurrences([]map[string]any{child.Doc}),
			Complete:  true,
		},
	)
	tasks := task.NewBounded(2)
	captured, replaced := make(chan struct{}), make(chan struct{})
	tasks.Go(t.Context(), "publish", func(context.Context) {
		var reads ReadSet
		reads.Init(st, input.Named(), nil)
		defer reads.Retire()
		_, _, next := reads.GetEmission(input.Named())
		defer next.Finish()
		next.Fingerprint = "stale"
		if strings.HasSuffix((*testCaseInput.route), "replacement") {
			next.Children = []Child{emissionChild("stale", "A")}
		}
		sh := st.shardFor(input.Named())
		sh.mu.RLock()
		slot := sh.slots[input.Named()]
		previous := slot.current.Load()
		generation := slot.generation
		sh.mu.RUnlock()
		var proposal *Emission
		if strings.HasPrefix((*testCaseInput.route), "locked_") {
			prepared := make([]admission, 0, len(next.Children))
			for _, c := range next.Children {
				prepared = append(
					prepared,
					admission{child: c, targets: manifest.ProducerTargets(c.Obj), binding: next.binding},
				)
			}
			proposal = prepareEmission(
				&next,
				previous,
				prepared,
				make(map[manifest.NamedResource]int, len(prepared)),
			)
		}
		close(captured)
		tasks.YieldSlot(func() { <-replaced })
		result := publicationRejected
		switch *testCaseInput.route {
		case "locked_stable":
			result = st.commitStable(slot, previous, proposal, generation)
		case "locked_replacement":
			result = st.commitReplacement(input.Named(), previous, proposal, generation)
		default:
			st.RecordEmitted(input.Named(), &next, nil)
		}
		{
			assert.ErrorIf(t, result != publicationRejected || reads.Valid(), "%s", "stale inventory accepted")
		}
	})
	tasks.Go(t.Context(), "replace", func(context.Context) {
		tasks.YieldSlot(func() { <-captured })
		if *testCaseInput.loss {
			accept(t, st, peer.Named(), &Emission{Complete: true})
			{
				assert.ErrorIf(
					t,
					st.GetObject(input.Named()) != input || st.GetDeclaration(input.Named()) != nil,
					"%s",
					"fixture did not retain raw input without authority",
				)
			}
		} else {
			replacement := input.Clone()
			replacement.Path = "B"
			accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: replacement}}, Complete: true})
		}
		close(replaced)
	})
	tasks.BlockTillDone()
	{
		assert.FatalIf(t, tasks.Failures() != 0, "%s", "publication task panicked")
	}
	_, _, retained := st.GetEmission(input.Named())
	defer retained.Finish()
	{
		assert.FatalIf(t, retained.Fingerprint == "stale" || len(retained.Children) != 1 ||
			retained.Children[0].Obj != child.Obj, "%s", "rejection replaced accepted output")
	}
	assert.Diff(t, retained.Children[0].Doc, child.Doc)
	assert.Diff(t, testutil.DocumentMaps(retained.Manifests), []map[string]any{child.Doc})
}

type testReadSetCloseRetiresHiddenBorrowedOutputCaseCompletionState struct {
	testCaseInput *string
	s             *Store
	parent        *manifest.Kustomization
	declaration   *manifest.BaseManifest
	first         *ReadSet
}

func testReadSetCloseRetiresHiddenBorrowedOutputCaseCompletion(t *testing.T, scope *testReadSetCloseRetiresHiddenBorrowedOutputCaseCompletionState) {
	t.Helper()
	registrations := 0
	for input := (*scope.first).inputs; input != nil; input = input.next {
		for kind := objectFacet; kind <= bucketFacet; kind++ {
			cell := input.cell(kind)
			if cell == nil || cell.head == nil {
				continue
			}
			for current := cell.head.first.Load(); current != nil; current = current.next.Load() {
				if current == cell {
					registrations++
				}
			}
		}
	}
	if !(*scope.first).retired.Load() || registrations != 0 {
		t.Fatalf(
			"cancelled successor left hidden borrower active: retired=%t registrations=%d",
			(*scope.first).retired.Load(),
			registrations,
		)
	}
	{
		assert.FatalIf(
			t,
			(*scope.first).GetObject((*scope.declaration).Named()) != (*scope.declaration),
			"%s",
			"closing discarded the accepted read snapshot",
		)
	}
	{
		assert.FatalIf(
			t,
			!slices.Contains(slices.Collect((*scope.first).Required()), scope.parent.Named()),
			"%s",
			"closing discarded the accepted required-input footprint",
		)
	}
	sh := scope.s.shardFor((*scope.declaration).Named())
	sh.mu.RLock()
	var retained *ReadSet
	if (*scope.testCaseInput) == "source_hidden" {
		retained = sh.artifacts[(*scope.declaration).Named()].value.Reads
	} else {
		retained = sh.slots[(*scope.declaration).Named()].current.Load().OutputReads
	}
	sh.mu.RUnlock()
	{
		assert.FatalIf(t, retained != &(*scope.first), "%s", "closing discarded borrowed output provenance")
	}
}
