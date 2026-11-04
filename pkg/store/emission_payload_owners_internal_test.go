package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCapture_PayloadAuthority(t *testing.T) {
	t.Parallel()
	for _, static := range []bool{false, true} {
		t.Run(fmt.Sprintf("static=%t", static), func(t *testing.T) {
			t.Parallel()
			testCapturePayloadAuthorityCase(t, &static)
		},
		)
	}
}

func TestRecordEmitted_StalePayloadRejected(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	input := ownerID("input")
	s.SetArtifact(input, &SourceArtifact{Key: "first"})
	var reads ReadSet
	reads.Init(s, producer.Named(), nil)
	defer reads.Retire()
	reads.GetArtifact(input, ArtifactContent)
	_, _, next := reads.GetEmission(producer.Named())
	defer next.Finish()
	child := emissionChild("output", "old")
	next.Children, next.Complete = []Child{child}, true
	reads.Bind(&next)
	s.SetArtifact(input, &SourceArtifact{Key: "changed"})
	s.RecordEmitted(
		producer.Named(),
		&next,
		map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "stale"}},
	)
	{
		assert.FatalIf(
			t,
			s.GetObject(child.Obj.Named()) != nil || s.GetArtifact(child.Obj.Named(), ArtifactContent) != nil ||
				s.HasInventory(producer.Named()),
			"%s",
			"invalid captured inputs published stale declaration or bytes",
		)
	}
}

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
	testRecordEmittedBorrowedPayloadOwnersCaseCompletion(
		t,
		&testRecordEmittedBorrowedPayloadOwnersCaseCompletionState{
			testCaseInput: &testCaseInput,
			s:             &s,
			owner:         &owner,
			output:        &output,
			first:         &first,
			second:        &second,
			third:         &third,
			registrations: &registrations,
			accepted:      &accepted,
		},
	)
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

func testRecordEmittedBorrowedPayloadOwnersCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedBorrowedPayloadOwnersCaseCompletionState,
) {
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
				(*scope.accepted).OutputReads != scope.output || (*scope.accepted).payloadReads != scope.first ||
					len((*scope.accepted).borrowed) != 2,
				"%s",
				"rejected proposal changed accepted output and payload provenance",
			)
		}
		for _, reads := range []*ReadSet{scope.output, scope.first, scope.second, scope.third} {
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
	_, _, next := scope.third.GetEmission((*scope.owner).Named())
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = []Child{
		last,
	}, testutil.DocumentOccurrences(
		[]map[string]any{last.Doc},
	), true
	scope.third.Bind(&next)
	(*scope.s).RecordEmitted((*scope.owner).Named(), &next, nil)
	{
		assert.FatalIf(
			t,
			!scope.first.retired.Load() || !scope.second.retired.Load() || scope.third.retired.Load(),
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
