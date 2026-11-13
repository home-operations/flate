package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"context"
	"io/fs"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcecontroller "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

//nolint:containedctx // This context wrapper probes propagation into cache operations.
type acquireProbe struct {
	context.Context

	calls *atomic.Int64
}

func (p acquireProbe) Value(key any) any {
	var pcs [64]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		frame, more := frames.Next()
		if !strings.HasPrefix(frame.Function, "context.") {
			if frame.Function == "github.com/home-operations/flate/pkg/source/cache.(*Cache).acquire" && more {
				parent, _ := frames.Next()
				if parent.Function == "github.com/home-operations/flate/pkg/source/cache.(*Cache).Acquire" {
					p.calls.Add(1)
				}
			}
			break
		}
		if !more {
			break
		}
	}
	return p.Context.Value(key)
}

type acquireControlFS struct {
	*observedFS
}

func (f acquireControlFS) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(f.FS, name)
}

//nolint:paralleltest,tparallel // Subtests share ordered cold-to-warm cache state.
func TestArtifactGenerator_AcquireObservationControl(
	t *testing.T,
) {
	t.Parallel()
	// Valid fixed/extract keys, no cancellation and no producer retry keep every Acquire on the probed path.
	owner, err := cache.New(t.Context(), "")

	assert.FatalIf(t, err != nil, "%v", err)

	t.Cleanup(func() {
		{
			closeErr := owner.Close()
			assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
		}
	})
	var calls, producers atomic.Int64
	probe := acquireProbe{Context: t.Context(), calls: &calls}
	key := cache.ArchiveKey{Policy: "fixed", Content: "consumed-input-observation"}
	produce := func(_ context.Context, input cache.ProducerInput) (cache.ProducerResult, error) {
		producers.Add(1)
		archive := tar.NewWriter(input.Writer)
		if err := archive.AddFS(fstest.MapFS{"file": {Data: []byte("input")}}); err != nil {
			return cache.ProducerResult{}, err
		}
		return cache.ProducerResult{}, archive.Close()
	}
	var firstKey string
	for _, phase := range []string{"cold", "warm", "derived-context-warm"} {
		t.Run(phase, func(t *testing.T) {
			testArtifactGeneratorAcquireObservationControlCase(
				t,
				testArtifactGeneratorAcquireObservationControlCaseInputs{
					owner:     &owner,
					calls:     &calls,
					producers: &producers,
					probe:     &probe,
					key:       &key,
					produce:   &produce,
					firstKey:  &firstKey,
					phase:     &phase,
				},
			)
		},
		)
	}
	input := fstest.MapFS{
		"file":        {Data: []byte("input")},
		"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"file": {Data: []byte("input")}})},
	}
	var archiveOpens atomic.Int64
	observed := &observedFS{FS: input, onOpen: func(name string) {
		if name == "archive.tgz" {
			archiveOpens.Add(1)
		}
	}}
	var generatedKey string
	for _, phase := range []string{"generator-cold", "fresh-controller-warm", "extract-cold", "extract-inner-warm"} {
		t.Run(phase, func(t *testing.T) {
			testArtifactGeneratorAcquireObservationControlCase2(
				t,
				testArtifactGeneratorAcquireObservationControlCase2Inputs{
					owner:        &owner,
					calls:        &calls,
					probe:        &probe,
					archiveOpens: &archiveOpens,
					observed:     &observed,
					generatedKey: &generatedKey,
					phase:        &phase,
				},
			)
		},
		)
	}
}

type generatorWork struct {
	admitted, slots, dispatched, settled, acquires atomic.Int64
	active                                         atomic.Int64
}

type workSnapshot struct {
	Admitted, Slots, Dispatched, Settled, Acquires int64
}

func (w *generatorWork) snapshot() workSnapshot {
	return workSnapshot{w.admitted.Load(), w.slots.Load(), w.dispatched.Load(), w.settled.Load(), w.acquires.Load()}
}

type convergenceTasks struct {
	*task.Service

	generator manifest.NamedResource
	work      *generatorWork
	completed func(string)
}

func (ts *convergenceTasks) Go(ctx context.Context, name string, fn func(context.Context)) {
	generator := name == "schedule/"+ts.generator.String() || name == "schedule/settle/"+ts.generator.String()
	if generator {
		ts.work.admitted.Add(1)
	}
	ts.Service.Go(ctx, name, func(ctx context.Context) {
		if generator {
			ts.work.slots.Add(1)
			ts.work.active.Add(1)
			defer ts.work.active.Add(-1)
		}
		fn(ctx)
		ts.completed(name)
	})
}

type convergenceAssessment struct {
	*base.Execution

	work *generatorWork
}

func (a *convergenceAssessment) Settle(ctx context.Context, level manifest.DrainLevel) {
	a.work.settled.Add(1)
	a.Execution.Settle(acquireProbe{Context: ctx, calls: &a.work.acquires}, level)
}

type convergenceDispatcher struct {
	generator *Controller
	id        manifest.NamedResource
	consumer  *kustomization.Controller
	source    *sourcecontroller.Controller
	driver    manifest.NamedResource
	drive     func(context.Context)
	work      *generatorWork
	sweeps    atomic.Int64
}

func (d *convergenceDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
	previous schedule.Result,
) schedule.Result {
	switch id {
	case d.id:
		d.work.dispatched.Add(1)
		assessment := d.generator.ReconcileNode(
			acquireProbe{Context: ctx, calls: &d.work.acquires},
			id,
			level,
			previous,
		)
		return schedule.Result{Assessment: &convergenceAssessment{Execution: assessment, work: d.work}}
	case d.driver:
		e := controllertest.NewExecution(d.generator.Controller, id)
		e.Dispatch[*manifest.Kustomization](
			ctx,
			level,
			d,
			func(d *convergenceDispatcher, ctx context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
				d.drive(ctx)
				return ctx.Err()
			},
		)
		return schedule.Result{Assessment: e}
	default:
		if d.source.Owns(id) {
			return schedule.Result{Assessment: d.source.ReconcileNode(ctx, id, level, previous)}
		}
		return schedule.Result{Assessment: d.consumer.ReconcileNode(ctx, id, level, previous)}
	}
}

func (d *convergenceDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	ids := slices.Collect(d.generator.Store.Sweep())
	d.sweeps.Add(1)
	return ids
}

func TestArtifactGenerator_ConsumedInputConvergence(t *testing.T) {
	t.Parallel()

	// A real krusty build must not wait on BuildMutex held outside its synctest bubble.
	for _, publication := range []string{"direct", "emitted"} {
		t.Run(publication, func(t *testing.T) {
			t.Parallel()

			first := consumedPublication(t, publication, "selected-union")
			second := consumedPublication(t, publication, "selected-union")

			assert.FatalIf(
				t,
				!bytes.Equal(first, second),
				"final documents changed across two runs:\n%s\n%s",
				first,
				second,
			)
		})
	}
}

func TestArtifactGenerator_ConsumedInputWork(t *testing.T) {
	t.Parallel()

	for _, publication := range []string{"direct", "emitted"} {
		t.Run(publication, func(t *testing.T) {
			t.Parallel()

			for _, change := range []string{"equal", "ignored-artifact-metadata", "ignored-declaration-metadata", "unselected-revision", "unselected-origin", "selected-revision", "selected-origin-presence", "origin-without-revision", "selected-origin-fallback", "selected-uid", "selected-binding"} {
				t.Run(change, func(t *testing.T) {
					t.Parallel()
					consumedPublication(t, publication, change)
				})
			}
		})
	}
}

type testArtifactGeneratorAcquireObservationControlCaseInputs struct {
	owner     **cache.Cache
	calls     *atomic.Int64
	producers *atomic.Int64
	probe     *acquireProbe
	key       *cache.ArchiveKey
	produce   *func(_ context.Context, input cache.ProducerInput) (cache.ProducerResult, error)
	firstKey  *string
	phase     *string
}

func testArtifactGeneratorAcquireObservationControlCase(
	t *testing.T,
	testCaseInput testArtifactGeneratorAcquireObservationControlCaseInputs,
) {
	t.Helper()

	var ctx context.Context = (*testCaseInput.probe)
	if (*testCaseInput.phase) == "derived-context-warm" {
		child, cancel := context.WithCancel(ctx)
		defer cancel()
		ctx = child
	}
	before := testCaseInput.calls.Load()
	input, _, id, err := (*testCaseInput.owner).Acquire(
		ctx,
		(*testCaseInput.key),
		&cache.Policy{MaxAge: cache.Forever},
		(*testCaseInput.produce),
	)

	assert.FatalIf(t, err != nil, "%v", err)

	data, err := fs.ReadFile(input, "file")
	if err != nil || string(data) != "input" || testCaseInput.calls.Load()-before != 1 ||
		testCaseInput.producers.Load() != 1 {
		t.Fatalf(
			"Acquire control: calls=%d producers=%d data=%q error=%v",
			testCaseInput.calls.Load()-before,
			testCaseInput.producers.Load(),
			data,
			err,
		)
	}
	if (*testCaseInput.firstKey) == "" {
		(*testCaseInput.firstKey) = id
	} else {
		assert.FatalIf(t, id != (*testCaseInput.firstKey), "warm key = %q, want %q", id, (*testCaseInput.firstKey))
	}
}

type testArtifactGeneratorAcquireObservationControlCase2Inputs struct {
	owner        **cache.Cache
	calls        *atomic.Int64
	probe        *acquireProbe
	archiveOpens *atomic.Int64
	observed     **observedFS
	generatedKey *string
	phase        *string
}

func testArtifactGeneratorAcquireObservationControlCase2(
	t *testing.T,
	testCaseInput testArtifactGeneratorAcquireObservationControlCase2Inputs,
) {
	t.Helper()

	st, tasks := store.New(), task.NewBounded(2)
	name, operations, wantCalls := "output", []any{
		map[string]any{"from": "@repo/file", "to": "@artifact/file"},
	}, int64(
		1,
	)
	if strings.HasPrefix((*testCaseInput.phase), "extract-") {
		name, operations, wantCalls = (*testCaseInput.phase), []any{
			map[string]any{"from": "@repo/archive.tgz", "to": "@artifact/", "strategy": "Extract"},
		}, 2
	}
	generatorForTestResult := generatorForTest(t, []any{map[string]any{"name": name, "copy": operations}})
	obj, doc := generatorForTestResult.object, generatorForTestResult.document
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	st.SetArtifact(
		repo,
		&store.SourceArtifact{
			Key:     "source",
			Content: "control-content",
			Digest:  "content",
			FS:      acquireControlFS{(*testCaseInput.observed)},
		},
	)
	controller := New(st, tasks, (*testCaseInput.owner))
	controller.StartLifecycle()
	before := testCaseInput.calls.Load()
	beforeFiles, beforeArchives := (*testCaseInput.observed).opens.Load(), testCaseInput.archiveOpens.Load()
	tasks.Go((*testCaseInput.probe), "control", func(ctx context.Context) {
		t.Cleanup(controller.ReconcileNode(ctx, obj.Named(), manifest.DrainNone, schedule.Result{}).Close)
	})
	tasks.BlockTillDone()
	output := st.GetArtifact(
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: "apps",
			Name:      name,
		},
		store.ArtifactContent,
	)
	artifact, ok := output.(*store.SourceArtifact)
	if !ok || tasks.Failures() != 0 {
		t.Fatalf("control output=%v task failures=%d", output, tasks.Failures())
	}
	data, err := fs.ReadFile(artifact.FS, "file")
	if err != nil || string(data) != "input" || testCaseInput.calls.Load()-before != wantCalls {
		t.Fatalf(
			"generator control: calls=%d want=%d data=%q error=%v", testCaseInput.calls.Load()-before, wantCalls,
			data,
			err,
		)
	}
	if (*testCaseInput.phase) == "generator-cold" {
		(*testCaseInput.generatedKey) = artifact.Key
	} else if (*testCaseInput.phase) == "fresh-controller-warm" && artifact.Key != (*testCaseInput.generatedKey) {
		t.Fatalf("fresh controller missed the warm output key: %s != %s", artifact.Key, (*testCaseInput.generatedKey))
	}

	assert.FatalIf(
		t,
		(*testCaseInput.phase) == "fresh-controller-warm" && (*testCaseInput.observed).opens.Load() != beforeFiles ||
			(*testCaseInput.phase) ==
				"extract-inner-warm" && testCaseInput.archiveOpens.Load() != beforeArchives,
		"%v",
		"warm Acquire invoked its suppressed producer",
	)

	assert.FatalIf(
		t,
		(*testCaseInput.phase) == "generator-cold" && (*testCaseInput.observed).opens.Load() <= beforeFiles ||
			(*testCaseInput.phase) ==
				"extract-cold" && testCaseInput.archiveOpens.Load() <= beforeArchives,
		"%v",
		"cold Acquire did not invoke its producer",
	)
}
