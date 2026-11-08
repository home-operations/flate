package store

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAcquisition_PostCleanupCredentialAuthority(t *testing.T) {
	t.Parallel()
	for _, present := range []bool{false, true} {
		name := "absent_credential"
		if present {
			name = "present_credential"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testAcquisitionPostCleanupCredentialAuthority(t, present)
		})
	}
}

func testAcquisitionPostCleanupCredentialAuthority(t *testing.T, present bool) {
	t.Helper()
	s := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	observer := &manifest.Kustomization{Name: "observer", Namespace: "apps"}
	credential := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	s.AddObjects([]manifest.BaseManifest{source, observer})
	if present {
		s.AddObject(
			&manifest.Secret{
				Name:       credential.Name,
				Namespace:  credential.Namespace,
				StringData: map[string]any{"token": "before"},
			},
		)
	}
	var origin ReadSet
	origin.Init(s, source.Named(), nil)
	origin.GetObject(source.Named())
	origin.GetObject(credential)
	origin.AddWarning(
		manifest.Warning{
			Resource: source.Named(),
			Category: manifest.WarnConflictingProducers,
			Message:  "accepted credential observation",
		},
	)
	s.UpdateStatus(
		source.Named(),
		StatusInfo{Status: StatusReady, Reason: ReasonSkipped, Message: "unavailable credentials"},
	)
	origin.SetAcquisition(AcquisitionSkipped)
	origin.Accept(AssessmentReconciled)
	if len(s.Warnings()) != 1 {
		t.Fatal("acceptance did not retain warning")
	}
	cleaned, published := make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "cleanup-and-publication", func(context.Context) {
		origin.Close()
		for range s.Sweep() {
		}
		close(cleaned)
		<-published
		s.AddObject(
			&manifest.Secret{
				Name:       credential.Name,
				Namespace:  credential.Namespace,
				StringData: map[string]any{"token": "after"},
			},
		)
	})
	tasks.Go(t.Context(), "preserved-result", func(context.Context) {
		<-cleaned
		if len(s.Warnings()) != 1 {
			t.Error("cleanup erased accepted warning")
		}
		var reader ReadSet
		reader.Init(s, observer.Named(), nil)
		if got := reader.GetAcquisition(source.Named()); got != AcquisitionSkipped {
			t.Errorf("after cleanup acquisition = %v, want skipped", got)
		}
		reader.Close()
		for range s.Sweep() {
		}
		close(published)
	})
	tasks.BlockTillDone()
	if got := tasks.Failures(); got != 0 {
		t.Fatalf("task failures = %d", got)
	}
	var fresh ReadSet
	fresh.Init(s, observer.Named(), nil)
	if got := fresh.GetAcquisition(source.Named()); got != AcquisitionPending {
		t.Errorf("after credential publication acquisition = %v, want pending", got)
	}
	fresh.Close()
	for range s.Sweep() {
	}
}
