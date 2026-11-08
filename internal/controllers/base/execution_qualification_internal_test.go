package base

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/depwait"
	"github.com/home-operations/flate/internal/schedule"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExecution_QualificationDemandPromotion(t *testing.T) {
	t.Parallel()
	for _, timing := range []string{"after settlement", "before finish", "selected after settlement", "selected before finish"} {
		t.Run(timing, func(t *testing.T) {
			t.Parallel()
			testQualificationDemandPromotion(t, timing)
		})
	}
}

func testQualificationDemandPromotion(t *testing.T, timing string) {
	t.Helper()
	beforeFinish := strings.Contains(timing, "before finish")
	st := store.New()
	producer := &manifest.ResourceSet{Name: "candidate", Namespace: "apps"}
	st.AddObject(producer)
	st.SetIncluded(producer.Named(), true, false)
	tasks := task.NewBounded(2)
	l, filter := admissiontest.New(
		t,
		admissiontest.Config{
			Inventory: st,
			Changes:   change.NewSet(nil),
			Index: &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					producer.Named(): {File: "candidate.yaml"},
				},
			},
			Request: selection.Request{Changed: true, Roots: []manifest.NamedResource{producer.Named()}},
		},
	)
	members := []manifest.NamedResource{producer.Named()}
	admissiontest.Require(t, st, filter, members, true)
	controller := New(st, tasks)
	controller.Configure(Options{Session: filter, Loader: l})
	controller.StartLifecycle()
	t.Cleanup(tasks.BlockTillDone)
	var calls atomic.Int64
	var screened Execution
	screened.Init(controller, producer.Named())
	defer screened.Reads.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	run := func(e *Execution, ctx context.Context, object *manifest.ResourceSet, _ manifest.DrainLevel) error {
		if e.AdmitQualification(false) {
			close(entered)
			if beforeFinish {
				<-release
			}
			return nil
		}
		var attempt store.Emission
		dedup, err := e.FingerprintDedup(ctx, object.Named(), "unchanged content", &attempt)
		if err != nil || dedup.Reused {
			return err
		}
		calls.Add(1)
		attempt.Complete, attempt.Fingerprint = true, dedup.Fingerprint
		return e.Emit(ctx, object.Named(), &attempt, nil)
	}
	tasks.Go(t.Context(), "screen", func(ctx context.Context) {
		screened.Dispatch(ctx, manifest.DrainNone, &screened, run)
	})
	<-entered
	if !beforeFinish {
		tasks.BlockTillDone()
		assert.Equal(t, screened.Valid(), true)
		assert.Equal(t, screened.Outcome(), schedule.Terminal)
		assert.Equal(t, screened.Disposition, store.AssessmentQualificationSettled)
		assert.Equal(t, screened.Progressed(), false)
		assert.Equal(t, st.GetArtifact(producer.Named(), store.ArtifactContent), store.Artifact(nil))
		_, known := st.GetStatus(producer.Named())
		assert.Equal(t, known, false)
		screened.Retire()
	}
	if strings.HasPrefix(timing, "selected ") {
		proposal, err := filter.Prepare(
			t.Context(),
			selection.PrepareRequest{
				Changes:    change.NewSet([]string{"candidate.yaml"}),
				Generation: []store.Child{{Obj: producer}},
			},
		)
		assert.NoError(t, err)
		admissiontest.Commit(t, filter, proposal)
	} else {
		admissiontest.Require(t, st, filter, members, false)
	}
	st.SetIncluded(producer.Named(), true, true)
	if beforeFinish {
		close(release)
	}
	tasks.BlockTillDone()
	assert.Equal(t, screened.Valid(), false)
	assert.Equal(t, calls.Load(), int64(0))
	assert.Equal(t, st.GetArtifact(producer.Named(), store.ArtifactContent), store.Artifact(nil))
	_, known := st.GetStatus(producer.Named())
	assert.Equal(t, known, false)
	var required Execution
	required.Init(controller, producer.Named())
	defer required.Reads.Close()
	tasks.Go(t.Context(), "required", func(ctx context.Context) {
		required.Dispatch(ctx, manifest.DrainNone, &required, run)
	})
	tasks.BlockTillDone()
	assert.Equal(t, required.Outcome(), schedule.Terminal)
	assert.Equal(t, required.Disposition, store.AssessmentReconciled)
	assert.Equal(t, required.Progressed(), true)
	assert.Equal(t, calls.Load(), int64(1))
	output, ok := st.GetArtifact(producer.Named(), store.ArtifactContent).(*store.Emission)
	assert.Equal(t, ok && output.Complete && output.OutputReads.Valid(), true)
	status, _ := st.GetStatus(producer.Named())
	assert.Equal(t, status.Outcome(), store.OutcomePassed)
	var reused Execution
	reused.Init(controller, producer.Named())
	defer reused.Reads.Close()
	tasks.Go(t.Context(), "reuse", func(ctx context.Context) {
		reused.Dispatch(ctx, manifest.DrainNone, &reused, run)
	})
	tasks.BlockTillDone()
	assert.Equal(t, reused.Valid(), true)
	assert.Equal(t, reused.Disposition, store.AssessmentReused)
	assert.Equal(t, reused.Progressed(), false)
	assert.Equal(t, calls.Load(), int64(1))
}

func TestExecution_QualificationFailurePrecedence(t *testing.T) {
	t.Parallel()
	for _, cause := range []string{"error", "cancellation", "blocked", "invalid"} {
		t.Run(cause, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			producer := &manifest.ResourceSet{Name: "candidate", Namespace: "apps"}
			input := &manifest.ConfigMap{Name: "input", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{producer, input})
			st.SetIncluded(producer.Named(), true, false)
			controller := New(st, task.NewBounded(2))
			var run Execution
			run.Init(controller, producer.Named())
			defer run.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			run.Dispatch(
				ctx,
				manifest.DrainNone,
				&run,
				func(e *Execution, _ context.Context, _ *manifest.ResourceSet, _ manifest.DrainLevel) error {
					e.Reads.GetObject(input.Named())
					assert.Equal(t, e.AdmitQualification(false), true)
					switch cause {
					case "error":
						return errors.New("qualification input failed")
					case "cancellation":
						cancel()
					case "blocked":
						return &depwait.ErrBlocked{Deps: []manifest.NamedResource{producer.Named()}}
					case "invalid":
						st.AddObject(
							&manifest.ConfigMap{
								Name:      input.Name,
								Namespace: input.Namespace,
								Data:      map[string]any{"value": "replacement"},
							},
						)
					}
					return nil
				},
			)
			var frontier store.ReadSet
			frontier.Init(st, manifest.NamedResource{}, nil)
			defer frontier.Close()
			assert.Equal(t, frontier.QualificationSettled(producer.Named()), false)
			assert.Equal(t, st.HasInventory(producer.Named()), false)
			status, known := st.GetStatus(producer.Named())
			switch cause {
			case "error", "cancellation":
				assert.Equal(t, status.Status, store.StatusFailed)
			case "blocked":
				assert.Equal(t, run.Outcome(), schedule.Blocked)
			case "invalid":
				assert.Equal(t, run.Valid(), false)
			}
			assert.Equal(t, known && status.Status == store.StatusReady, false)
		})
	}
}
