package store

import (
	"context"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

type acquisitionObservationCase struct {
	name                                string
	initial, next                       AcquisitionState
	retained, cleanup, acceptedObserver bool
}

func TestAcquisition_AcceptedObservation(t *testing.T) {
	t.Parallel()
	for _, tc := range []acquisitionObservationCase{
		{name: "pending_to_prepared", next: AcquisitionPrepared},
		{name: "equal_input_replacement", initial: AcquisitionPrepared, next: AcquisitionPrepared},
		{name: "retained_settlement", next: AcquisitionPrepared, retained: true},
		{name: "prior_validity_accepted", next: AcquisitionPrepared, acceptedObserver: true},
		{name: "accepted_cleanup", initial: AcquisitionPrepared, next: AcquisitionPrepared, retained: true, cleanup: true},
		{name: "accepted_skipped_cleanup", initial: AcquisitionSkipped, next: AcquisitionSkipped, retained: true, cleanup: true},
		{name: "live_replacement", initial: AcquisitionPrepared},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAcquisitionAcceptedObservation(t, tc)
		})
	}
}

func testAcquisitionAcceptedObservation(t *testing.T, tc acquisitionObservationCase) {
	t.Helper()
	s := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	observer := &manifest.Kustomization{Name: "observer", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{source, observer})
	if tc.initial == AcquisitionSkipped {
		s.UpdateStatus(
			source.Named(),
			StatusInfo{Status: StatusReady, Reason: ReasonSkipped, Message: "credentials unavailable"},
		)
	}
	var origin, successor, old, declaration ReadSet
	origin.Init(s, source.Named(), nil)
	origin.GetDeclaration(source.Named())
	origin.SetAcquisition(tc.initial)
	origin.Accept(AssessmentReconciled)
	defer origin.Close()
	writer := &successor
	if tc.retained {
		writer = &origin
	} else {
		successor.Init(s, source.Named(), nil)
		successor.GetDeclaration(source.Named())
		defer successor.Close()
	}
	old.Init(s, observer.Named(), nil)
	defer old.Close()
	assert.Equal(t, old.GetAcquisition(source.Named()), tc.initial)
	if tc.acceptedObserver {
		old.Accept(AssessmentReconciled)
	}
	declaration.Init(s, observer.Named(), nil)
	defer declaration.Close()
	declaration.GetDeclaration(source.Named())
	declaration.GetArtifact(source.Named(), ArtifactContent)
	staged, allow, accepted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "stage-and-accept", func(context.Context) {
		writer.SetAcquisition(tc.next)
		close(staged)
		<-allow
		if tc.cleanup {
			writer.Close()
		} else {
			writer.Accept(AssessmentReconciled)
		}
		close(accepted)
	})
	tasks.Go(t.Context(), "capture-and-sweep", func(context.Context) {
		<-staged
		var before ReadSet
		before.Init(s, observer.Named(), nil)
		assert.Equal(t, before.GetAcquisition(source.Named()), tc.initial)
		assert.Equal(t, before.Valid(), true)
		close(allow)
		<-accepted
		assert.Equal(t, old.GetAcquisition(source.Named()), tc.initial)
		assert.Equal(t, old.Valid(), tc.cleanup)
		assert.Equal(t, declaration.Valid(), true)
		before.Retire()
		got := slices.Collect(s.Sweep())
		var want []manifest.NamedResource
		if !tc.cleanup {
			want = []manifest.NamedResource{observer.Named()}
		}
		assert.Diff(t, got, want)
		assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
		var fresh ReadSet
		fresh.Init(s, observer.Named(), nil)
		assert.Equal(t, fresh.GetAcquisition(source.Named()), tc.next)
		assert.Equal(t, fresh.Valid(), true)
		if tc.next == AcquisitionSkipped {
			status, ok := fresh.GetStatus(source.Named())
			assert.Equal(t, ok, true)
			assert.Equal(t, status.Outcome(), OutcomeSkipped)
			assert.Equal(t, fresh.GetArtifact(source.Named(), ArtifactContent) == nil, true)
		}
		fresh.Retire()
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), 0)
}

func TestAcquisition_IndependentAcceptedBorrowers(t *testing.T) {
	t.Parallel()
	s := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	observer := &manifest.Kustomization{Name: "observer", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{source, observer})
	var reconciliation, binding, reader, cleanup ReadSet
	for _, reads := range []*ReadSet{&reconciliation, &binding} {
		reads.Init(s, source.Named(), nil)
		reads.GetObject(source.Named())
	}
	reconciliation.AddWarning(
		manifest.Warning{
			Resource: source.Named(),
			Category: manifest.WarnConflictingProducers,
			Message:  "reconciliation",
		},
	)
	reconciliation.SetAcquisition(AcquisitionPrepared)
	reconciliation.Accept(AssessmentReconciled)
	reader.Init(s, observer.Named(), nil)
	defer reader.Close()
	assert.Equal(t, reader.GetAcquisition(source.Named()), AcquisitionPrepared)
	binding.AddWarning(
		manifest.Warning{Resource: source.Named(), Category: manifest.WarnConflictingProducers, Message: "binding"},
	)
	binding.Accept(AssessmentEntryBinding)
	assert.Equal(t, reader.Valid(), true)
	assert.Equal(t, len(s.Warnings()), 2)
	cleanup.Init(s, source.Named(), nil)
	cleaned := make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "cleanup", func(context.Context) {
		cleanup.Close()
		for range s.Sweep() {
		}
		close(cleaned)
	})
	tasks.Go(t.Context(), "preserved-results", func(context.Context) {
		<-cleaned
		assert.Equal(t, reconciliation.retired.Load(), true)
		assert.Equal(t, binding.retired.Load(), true)
		assert.Equal(t, reader.Valid(), true)
		assert.Equal(t, len(s.Warnings()), 2)
		var fresh ReadSet
		fresh.Init(s, observer.Named(), nil)
		assert.Equal(t, fresh.GetAcquisition(source.Named()), AcquisitionPrepared)
		fresh.Retire()
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), 0)
	var replacement ReadSet
	replacement.Init(s, source.Named(), nil)
	defer replacement.Close()
	replacement.SetAcquisition(AcquisitionPending)
	replacement.Accept(AssessmentReconciled)
	assert.Equal(t, len(s.Warnings()), 1)
	assert.Equal(t, s.Warnings()[0].Message, "binding")
	reconciliation.Close()
	assert.Equal(t, len(s.Warnings()), 1)
}

func TestAcquisition_SupersededAcceptance(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		state AcquisitionState
	}{
		{name: "pending", state: AcquisitionPending},
		{name: "prepared", state: AcquisitionPrepared},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Parallel()
				s := New()
				source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
				observer := &manifest.Kustomization{Name: "observer", Namespace: "apps"}
				s.AddObjects([]manifest.BaseManifest{source, observer})
				var old, next, reader ReadSet
				old.Init(s, source.Named(), nil)
				old.SetAcquisition(tc.state)
				old.Accept(AssessmentReconciled)
				s.SetArtifact(source.Named(), &SourceArtifact{Reads: &old, Content: "retained", FS: fstest.MapFS{}})
				next.Init(s, source.Named(), nil)
				next.SetAcquisition(tc.state)
				next.Accept(AssessmentReconciled)
				defer next.Close()
				reader.Init(s, observer.Named(), nil)
				defer reader.Close()
				assert.Equal(t, reader.GetAcquisition(source.Named()), tc.state)
				old.Accept(AssessmentReconciled)
				assert.Equal(t, old.Valid(), false)
				assert.Equal(t, reader.Valid(), true)
				assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
				old.Close()
				assert.Equal(t, reader.Valid(), true)
				next.Accept(AssessmentReconciled)
				assert.Equal(t, reader.Valid(), true)
				assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
			},
		)
	}
}

func TestAcquisition_UnrelatedContent(t *testing.T) {
	t.Parallel()
	s := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	observer := &manifest.Kustomization{Name: "observer", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{source, observer})
	s.SetArtifact(source.Named(), &SourceArtifact{Content: "same", FS: fstest.MapFS{}})
	var first, second, reader ReadSet
	first.Init(s, source.Named(), nil)
	first.SetAcquisition(AcquisitionPrepared)
	first.Accept(AssessmentReconciled)
	defer first.Close()
	reader.Init(s, observer.Named(), nil)
	defer reader.Close()
	reader.GetDeclaration(source.Named())
	artifact := reader.GetArtifact(source.Named(), ArtifactContent)
	second.Init(s, source.Named(), nil)
	second.SetAcquisition(AcquisitionPrepared)
	second.Accept(AssessmentReconciled)
	defer second.Close()
	assert.Equal(t, reader.Valid(), true)
	assert.Equal(t, reader.GetArtifact(source.Named(), ArtifactContent) == artifact, true)
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func TestAcquisition_FrozenSettlement(t *testing.T) {
	t.Parallel()
	s := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	observer := &manifest.Kustomization{Name: "observer", Namespace: "apps"}
	first := &manifest.Secret{Name: "first", Namespace: "apps"}
	second := &manifest.Secret{Name: "second", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{source, observer, first, second})
	var origin ReadSet
	origin.Init(s, source.Named(), nil)
	origin.GetObject(first.Named())
	origin.Require(manifest.DependencyRef{NamedResource: first.Named()})
	origin.SetAcquisition(AcquisitionPrepared)
	origin.Accept(AssessmentReconciled)
	defer origin.Close()
	grow, grew, read, accepted := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "retained-settlement", func(context.Context) {
		<-grow
		origin.GetObject(second.Named())
		expression := "true"
		origin.Require(manifest.DependencyRef{NamedResource: first.Named(), ReadyExpr: &expression})
		origin.SetAcquisition(AcquisitionPending)
		close(grew)
		<-read
		origin.Accept(AssessmentReconciled)
		close(accepted)
	})
	tasks.Go(t.Context(), "accepted-evidence", func(context.Context) {
		var old ReadSet
		old.Init(s, observer.Named(), nil)
		defer old.Retire()
		assert.Equal(t, old.GetAcquisition(source.Named()), AcquisitionPrepared)
		close(grow)
		assert.Equal(t, old.Valid(), true)
		<-grew
		var fresh ReadSet
		fresh.Init(s, observer.Named(), nil)
		assert.Equal(t, fresh.GetAcquisition(source.Named()), AcquisitionPrepared)
		fresh.Retire()
		close(read)
		<-accepted
		assert.Equal(t, old.GetAcquisition(source.Named()), AcquisitionPrepared)
		assert.Equal(t, old.Valid(), false)
		assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{observer.Named()})
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), 0)
}
