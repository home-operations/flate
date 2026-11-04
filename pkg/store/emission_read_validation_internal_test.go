package store

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

type admissionBarrier struct {
	*manifest.ConfigMap

	calls    atomic.Int64
	prepared chan struct{}
	release  <-chan struct{}
}

func (b *admissionBarrier) Named() manifest.NamedResource {
	// Two collection lookups and one proven-target lookup precede prepareEmission indexing.
	if b.calls.Add(1) == 4 {
		close(b.prepared)
		<-b.release
	}
	return b.ConfigMap.Named()
}

func TestRecordEmitted_ReadsInvalidatedBeforeCommit(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"stable", "replacement", "payload"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedReadsInvalidatedBeforeCommitCase(
				t,
				&mode,
			)
		},
		)
	}
}

func testRecordEmittedReadsInvalidatedBeforeCommitCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	owner := ownerID("owner")
	s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	input := emissionChild("input", "old")
	s.AddObject(input.Obj)
	acceptedInput := emissionChild("accepted-input", "old")
	s.AddObject(acceptedInput.Obj)
	var accepted ReadSet
	accepted.Init(s, owner, nil)
	defer accepted.Retire()
	accepted.GetObject(acceptedInput.Obj.Named())
	acceptedHead := accepted.input(acceptedInput.Obj.Named()).object.head
	child := &manifest.ConfigMap{Name: "output", Namespace: "apps"}
	output := &Emission{Children: []Child{{Obj: child}}, Fingerprint: "accepted", Complete: true}
	accepted.Bind(output)
	s.RecordEmitted(owner, output, nil)
	previous := s.GetArtifact(owner, ArtifactContent).(*Emission)
	var reads ReadSet
	reads.Init(s, owner, nil)
	defer reads.Retire()
	reads.GetObject(input.Obj.Named())
	rejectedHead := reads.input(input.Obj.Named()).object.head
	assert.FatalIf(
		t,
		acceptedHead == rejectedHead,
		"%s",
		"accepted and successor-only inputs share a registration head",
	)
	name := child.Name
	if (*testCaseInput) == "replacement" {
		name = "replacement"
	}
	release := make(chan struct{})
	barrier := &admissionBarrier{
		ConfigMap: &manifest.ConfigMap{Name: name, Namespace: "apps"},
		prepared:  make(chan struct{}),
		release:   release,
	}
	next := &Emission{Children: []Child{{Obj: barrier}}, Fingerprint: "stale", Complete: true}
	reads.Bind(next)
	resume := sync.OnceFunc(func() { close(release) })
	var payloads map[manifest.NamedResource]*SourceArtifact
	if (*testCaseInput) == "payload" {
		payloads = map[manifest.NamedResource]*SourceArtifact{
			child.Named(): {Key: "stale", Content: "stale", FS: fstest.MapFS{}},
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RecordEmitted(owner, next, payloads)
	}()
	t.Cleanup(func() { resume(); <-done })
	select {
	case <-barrier.prepared:
	case <-done:
		t.Fatal("publication did not reach the admission barrier")
	}
	assert.FatalIf(t, !reads.Valid(), "%s", "reads invalid before concurrent input replacement")
	s.AddObject(emissionChild("input", "new").Obj)
	assert.FatalIf(t, reads.Valid(), "%s", "input replacement did not invalidate the pending publication")
	resume()
	<-done
	assert.FatalIf(t, s.GetArtifact(owner, ArtifactContent) != previous || s.GetObject(child.Named()) != child ||
		s.GetArtifact(child.Named(), ArtifactContent) != nil, "%s", "stale proposal replaced authoritative output")
	assert.FatalIf(
		t,
		(*testCaseInput) == "replacement" && s.GetObject(barrier.ConfigMap.Named()) != nil,
		"%s",
		"stale proposal installed a replacement child",
	)
	assert.ErrorIf(
		t,
		previous.OutputReads != &accepted || accepted.retired.Load(),
		"%s",
		"rejected proposal retired accepted output provenance",
	)
	reads.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	assert.ErrorIf(
		t,
		!reads.retired.Load() || rejectedHead.first.Load() != nil,
		"%s",
		"sweep retained rejected attempt registrations",
	)
	assert.ErrorIf(
		t,
		acceptedHead.first.Load() == nil || s.GetArtifact(owner, ArtifactContent) != previous,
		"%s",
		"sweep removed accepted output or its input registration",
	)
	s.AddObject(emissionChild("accepted-input", "new").Obj)
	assert.FatalIf(
		t,
		!slices.Contains(slices.Collect(s.Sweep()), owner),
		"%s",
		"accepted input change did not notify its owner after successor rejection",
	)
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
	assert.FatalIf(
		t,
		!first.Valid() || first.retired.Load() || st.GetArtifact(owner.Named(), ArtifactContent) != accepted,
		"%s",
		"rejected successor retired or replaced valid predecessor output",
	)
	assert.FatalIf(
		t,
		next.Footprint() != nil || successor.Valid(),
		"%s",
		"commit-boundary rejection supplied acceptance",
	)
}
