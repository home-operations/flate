package store

import (
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadRegistration_AbsentAdmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"batch", "emission", "hidden-producer"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testReadRegistrationAbsentAdmissionCase(t, &mode)
		},
		)
	}
}

func TestReadRegistration_AbsentDetachedAdmission(t *testing.T) {
	t.Parallel()
	for _, mask := range []ArtifactRead{0, ArtifactContent, ArtifactRevision, ArtifactOrigin, ArtifactContent | ArtifactRevision | ArtifactOrigin} {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			t.Parallel()
			testReadRegistrationAbsentDetachedAdmissionCase(
				t,
				&mask,
			)
		},
		)
	}
}

func TestReadRegistration_AbsenceExhaustionIsAtomic(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"object", "condition", "artifact", "revision", "origin", "union", "emission"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			testReadRegistrationAbsenceExhaustionIsAtomicCase(
				t,
				&operation,
			)
		},
		)
	}
}

func testReadRegistrationAbsentAdmissionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	child := emissionChild("input", "new")
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	if (*testCaseInput) == "emission" {
		s.AddObject(producer)
	}
	id := child.Obj.Named()
	if (*testCaseInput) == "hidden-producer" {
		id = producer.Named()
	}
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	reads.GetObject(id)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if (*testCaseInput) == "batch" {
			s.AddObjects([]manifest.BaseManifest{child.Obj})
		} else {
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
		}
	}()
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	{
		assert.FatalIf(t, reads.Valid(), "%s", "first authority admission left consumed absence valid")
	}
	reads.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func testReadRegistrationAbsentDetachedAdmissionCase(
	t *testing.T,
	testCaseInput *ArtifactRead,
) {
	t.Helper()

	s := New()
	read := func(r *ReadSet, id manifest.NamedResource) {
		if (*testCaseInput) == 0 {
			r.GetObject(id)
		} else {
			r.GetArtifact(id, (*testCaseInput))
		}
	}
	publish := func(id manifest.NamedResource) {
		if (*testCaseInput) == 0 {
			s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		} else {
			s.SetArtifact(
				id,
				&SourceArtifact{
					Revision: "present",
					Metadata: map[string]string{"org.opencontainers.image.revision": ""},
				},
			)
		}
	}
	var first, later, retired, fresh ReadSet
	for _, reads := range []*ReadSet{&first, &later, &retired, &fresh} {
		reads.Init(s, ownerID("reader"), nil)
	}
	first.id, later.id = ownerID("first-reader"), ownerID("later-reader")
	firstID, laterID := ownerID("first-input"), ownerID("later-input")
	read(&later, laterID)
	read(&retired, ownerID("retired-input"))
	read(&first, firstID)
	retired.Retire()
	publish(firstID)
	detached, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for id := range s.Sweep() {
			assert.Equal(t, id, first.id)
			close(detached)
			<-resume
		}
	}()
	<-detached
	first.Retire()
	read(&fresh, ownerID("still-absent"))
	publish(laterID)
	close(resume)
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{later.id})
	{
		assert.FatalIf(t, !fresh.Valid(), "%s", "new live absence registration was invalidated")
	}
	later.Retire()
	fresh.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	sh := s.shardFor(laterID)
	{
		assert.FatalIf(
			t,
			sh.absence.first.Load() != nil || sh.absence.watch != nil || sh.absence.pending.Load(),
			"%s",
			"retired absence registrations survived reclamation",
		)
	}
}

func testReadRegistrationAbsenceExhaustionIsAtomicCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	obj := &manifest.ConfigMap{Name: "new", Namespace: "apps"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	var reads ReadSet
	reads.Init(s, producer.Named(), nil)
	switch *testCaseInput {
	case "condition":
		reads.GetStatus(obj.Named())
	case "artifact", "revision", "origin", "union":
		reads.GetArtifact(obj.Named(), ArtifactContent|ArtifactRevision|ArtifactOrigin)
	default:
		reads.GetObject(obj.Named())
	}
	sh := s.shardFor(obj.Named())
	generation := ^uint64(0)
	if (*testCaseInput) == "object" || (*testCaseInput) == "emission" {
		generation--
	}
	if (*testCaseInput) == "union" {
		generation -= 2
	}
	sh.absence.generation.Store(generation)
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		switch *testCaseInput {
		case "object":
			s.AddObject(obj)
		case "condition":
			s.UpdateStatus(obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
		case "artifact":
			s.SetArtifact(obj.Named(), &SourceArtifact{})
		case "revision":
			s.SetArtifact(obj.Named(), &SourceArtifact{Revision: "next"})
		case "origin":
			s.SetArtifact(
				obj.Named(),
				&SourceArtifact{Metadata: map[string]string{"org.opencontainers.image.revision": ""}},
			)
		case "union":
			s.SetArtifact(
				obj.Named(),
				&SourceArtifact{
					Revision: "next",
					Metadata: map[string]string{"org.opencontainers.image.revision": ""},
				},
			)
		case "emission":
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: obj}}, Complete: true}, nil)
		}
	}()
	if !panicked || sh.absence.generation.Load() != generation || !reads.Valid() {
		t.Fatalf("panic=%t generation=%d valid=%t", panicked, sh.absence.generation.Load(), reads.Valid())
	}
	{
		assert.FatalIf(
			t,
			s.GetObject(obj.Named()) != nil || s.GetArtifact(obj.Named(), ArtifactContent) != nil,
			"%s",
			"exhaustion partially admitted target",
		)
	}
	{
		_, ok := s.GetStatus(obj.Named())
		assert.FatalIf(t, ok, "%s", "exhaustion partially published status")
	}
	{
		assert.FatalIf(
			t,
			s.GetArtifact(producer.Named(), ArtifactContent) != nil,
			"%s",
			"exhaustion partially published inventory",
		)
	}
}
