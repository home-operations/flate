package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcecontroller "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sources "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type acquireProbe struct {
	context.Context
	calls *atomic.Int64
}

func (p acquireProbe) Value(key any) any {
	var pcs [64]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		frame, more := frames.Next()
		if strings.HasPrefix(frame.Function, "github.com/home-operations/flate/pkg/source/cache.") {
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

func TestArtifactGenerator_AcquireObservationControl(t *testing.T) { //nolint:tparallel // Subtests share ordered cold-to-warm cache state.
	t.Parallel()
	// Valid fixed/extract keys, no cancellation and no producer retry keep every Acquire on the probed path.
	owner, err := cache.New(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
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
			var ctx context.Context = probe
			if phase == "derived-context-warm" {
				child, cancel := context.WithCancel(ctx)
				defer cancel()
				ctx = child
			}
			before := calls.Load()
			input, _, id, err := owner.Acquire(ctx, key, cache.Policy{MaxAge: cache.Forever}, produce)
			if err != nil {
				t.Fatal(err)
			}
			data, err := fs.ReadFile(input, "file")
			if err != nil || string(data) != "input" || calls.Load()-before != 1 || producers.Load() != 1 {
				t.Fatalf("Acquire control: calls=%d producers=%d data=%q error=%v", calls.Load()-before, producers.Load(), data, err)
			}
			if firstKey == "" {
				firstKey = id
			} else if id != firstKey {
				t.Fatalf("warm key = %q, want %q", id, firstKey)
			}
		})
	}
	input := fstest.MapFS{"file": {Data: []byte("input")}, "archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"file": {Data: []byte("input")}})}}
	var archiveOpens atomic.Int64
	observed := &observedFS{FS: input, onOpen: func(name string) {
		if name == "archive.tgz" {
			archiveOpens.Add(1)
		}
	}}
	var generatedKey string
	for _, phase := range []string{"generator-cold", "fresh-controller-warm", "extract-cold", "extract-inner-warm"} {
		t.Run(phase, func(t *testing.T) {
			st, tasks := store.New(), task.NewBounded(2)
			name, operations, wantCalls := "output", []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}}, int64(1)
			if strings.HasPrefix(phase, "extract-") {
				name, operations, wantCalls = phase, []any{map[string]any{"from": "@repo/archive.tgz", "to": "@artifact/", "strategy": "Extract"}}, 2
			}
			obj, doc := generatorForTest(t, []any{map[string]any{"name": name, "copy": operations}})
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
			st.SetArtifact(repo, &store.SourceArtifact{Key: "source", Content: "control-content", Digest: "content", FS: acquireControlFS{observed}})
			controller := New(st, tasks, owner)
			controller.StartLifecycle()
			before := calls.Load()
			beforeFiles, beforeArchives := observed.opens.Load(), archiveOpens.Load()
			tasks.Go(probe, "control", func(ctx context.Context) {
				t.Cleanup(controller.ReconcileNode(ctx, obj.Named(), manifest.DrainNone).Close)
			})
			tasks.BlockTillDone()
			output := st.GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: name}, store.ArtifactContent)
			artifact, ok := output.(*store.SourceArtifact)
			if !ok || tasks.Failures() != 0 {
				t.Fatalf("control output=%v task failures=%d", output, tasks.Failures())
			}
			data, err := fs.ReadFile(artifact.FS, "file")
			if err != nil || string(data) != "input" || calls.Load()-before != wantCalls {
				t.Fatalf("generator control: calls=%d want=%d data=%q error=%v", calls.Load()-before, wantCalls, data, err)
			}
			if phase == "generator-cold" {
				generatedKey = artifact.Key
			} else if phase == "fresh-controller-warm" && artifact.Key != generatedKey {
				t.Fatalf("fresh controller missed the warm output key: %s != %s", artifact.Key, generatedKey)
			}
			if phase == "fresh-controller-warm" && observed.opens.Load() != beforeFiles || phase == "extract-inner-warm" && archiveOpens.Load() != beforeArchives {
				t.Fatal("warm Acquire invoked its suppressed producer")
			}
			if phase == "generator-cold" && observed.opens.Load() <= beforeFiles || phase == "extract-cold" && archiveOpens.Load() <= beforeArchives {
				t.Fatal("cold Acquire did not invoke its producer")
			}
		})
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

func (d *convergenceDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	switch id {
	case d.id:
		d.work.dispatched.Add(1)
		assessment := d.generator.ReconcileNode(acquireProbe{Context: ctx, calls: &d.work.acquires}, id, level)
		return schedule.Result{Assessment: &convergenceAssessment{Execution: assessment, work: d.work}}
	case d.driver:
		e := controllertest.NewExecution(d.generator.Controller, id)
		e.Dispatch[*manifest.Kustomization](ctx, level, d, func(d *convergenceDispatcher, ctx context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
			d.drive(ctx)
			return ctx.Err()
		})
		return schedule.Result{Assessment: e}
	default:
		if d.source.Owns(id) {
			return schedule.Result{Assessment: d.source.ReconcileNode(ctx, id, level)}
		}
		return schedule.Result{Assessment: d.consumer.ReconcileNode(ctx, id, level)}
	}
}

func (d *convergenceDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	ids := slices.Collect(d.generator.Store.Sweep())
	d.sweeps.Add(1)
	return ids
}

func TestArtifactGenerator_ConsumedInputConvergence(t *testing.T) {
	// A real krusty build must not wait on BuildMutex held outside its synctest bubble.
	for _, publication := range []string{"direct", "emitted"} {
		t.Run(publication, func(t *testing.T) {
			first := consumedPublication(t, publication, "selected-union")
			second := consumedPublication(t, publication, "selected-union")
			if !bytes.Equal(first, second) {
				t.Fatalf("final documents changed across two runs:\n%s\n%s", first, second)
			}
		})
	}
}

func TestArtifactGenerator_ConsumedInputWork(t *testing.T) {
	for _, publication := range []string{"direct", "emitted"} {
		t.Run(publication, func(t *testing.T) {
			for _, change := range []string{"equal", "ignored-artifact-metadata", "ignored-declaration-metadata", "unselected-revision", "unselected-origin", "selected-revision", "selected-origin-presence", "origin-without-revision", "selected-origin-fallback", "selected-uid", "selected-binding"} {
				t.Run(change, func(t *testing.T) { consumedPublication(t, publication, change) })
			}
		})
	}
}

func consumedPublication(t *testing.T, publication, change string) []byte {
	t.Helper()
	var documents []byte
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		st, bodies := store.New(), task.NewBounded(2)
		owner, err := cache.New(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		}()
		acquire := func(value string) *store.SourceArtifact {
			t.Helper()
			input, descriptor, id, err := owner.Acquire(ctx, cache.ArchiveKey{Policy: "fixed", Content: value}, cache.Policy{MaxAge: cache.Forever}, func(_ context.Context, target cache.ProducerInput) (cache.ProducerResult, error) {
				archive := tar.NewWriter(target.Writer)
				files := fstest.MapFS{
					"app/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
					"app/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-config, namespace: apps}\ndata: {value: " + value + "}\n")},
				}
				if err := archive.AddFS(files); err != nil {
					return cache.ProducerResult{}, err
				}
				return cache.ProducerResult{}, archive.Close()
			})
			if err != nil {
				t.Fatal(err)
			}
			return &store.SourceArtifact{FS: input, Key: id, Content: cache.AcquisitionIdentity(input), Digest: descriptor.Digest.String(), Revision: value + "-revision", Metadata: map[string]string{"org.opencontainers.image.revision": value + "-origin"}}
		}
		s1, s2 := acquire("S1"), acquire("S2")
		if change == "selected-origin-presence" || change == "selected-origin-fallback" {
			delete(s1.Metadata, "org.opencontainers.image.revision")
		}
		if change != "selected-union" {
			clone := *s1
			clone.Metadata = maps.Clone(s1.Metadata)
			s2 = &clone
			if change == "ignored-artifact-metadata" {
				s2.Metadata["ignored"] = "new"
			}
			if change == "unselected-revision" || change == "selected-revision" || change == "origin-without-revision" || change == "selected-origin-fallback" {
				s2.Revision = "next-revision"
			}
			if change == "unselected-origin" {
				s2.Metadata["org.opencontainers.image.revision"] = "next-origin"
			}
			if change == "selected-origin-presence" {
				s2.Metadata["org.opencontainers.image.revision"] = ""
			}
		}
		obj, doc := generatorForTest(t, []any{map[string]any{"name": "output", "revision": "@repo", "originRevision": "@repo", "copy": []any{
			map[string]any{"from": "@repo/app/kustomization.yaml", "to": "@artifact/app/kustomization.yaml"},
			map[string]any{"from": "@repo/app/cm.yaml", "to": "@artifact/app/cm.yaml"},
		}}})
		if strings.HasPrefix(change, "unselected-") || change == "origin-without-revision" || change == "selected-origin-fallback" {
			output := doc["spec"].(map[string]any)["artifacts"].([]any)[0].(map[string]any)
			delete(output, "revision")
			if strings.HasPrefix(change, "unselected-") {
				delete(output, "originRevision")
			}
			parsed, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			obj, doc = parsed.(*manifest.RawObject), authoritative
		}
		repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
		if publication == "emitted" {
			repo.Kind = manifest.KindExternalArtifact
			doc["spec"].(map[string]any)["sources"].([]any)[0].(map[string]any)["kind"] = repo.Kind
			parsed, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			obj, doc = parsed.(*manifest.RawObject), authoritative
		}
		st.AddObject(obj, doc)
		rebound := repo
		rebound.Name = "other"
		if change == "selected-binding" {
			st.SetArtifact(rebound, s1)
		}
		consumerDoc := map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "consumer", "namespace": "apps"}, "spec": map[string]any{"interval": "1m", "path": "./app", "sourceRef": map[string]any{"kind": manifest.KindExternalArtifact, "name": "output"}}}
		consumer, authoritative, _, err := manifest.ParseDoc(consumerDoc, manifest.ParseDocOptions{})
		if err != nil {
			t.Fatal(err)
		}
		st.AddObject(consumer, authoritative)
		driver := &manifest.Kustomization{Name: "driver", Namespace: "apps"}
		st.AddObject(driver)
		generator := New(st, bodies, owner)
		generator.StartLifecycle()
		trees := kustomize.NewTreeCache(owner, nil, bodies)
		defer trees.Close()
		ks := kustomization.New(st, bodies, trees, false)
		ks.StartLifecycle()
		src := sourcecontroller.New(st, bodies)
		src.Fetchers[manifest.KindExternalArtifact] = sources.Wrap(manifest.KindExternalArtifact, &external.Fetcher{})
		src.StartLifecycle()
		publish := func(input *store.SourceArtifact) { st.SetArtifact(repo, input) }
		if publication == "emitted" {
			supplier := &manifest.ResourceSet{Name: "supplier", Namespace: "apps"}
			st.AddObject(supplier)
			producer := controllertest.NewExecution(generator.Controller, supplier.Named())
			defer producer.Close()
			publish = func(input *store.SourceArtifact) {
				_, _, next := producer.Reads.GetEmission(supplier.Named())
				defer next.Finish()
				childDoc := map[string]any{"apiVersion": manifest.SourceDomain + "/v1", "kind": repo.Kind, "metadata": map[string]any{"name": repo.Name, "namespace": repo.Namespace}}
				children, docs := testutil.ParseChildren(generator, supplier, []map[string]any{childDoc}, manifest.ParseDocOptions{})
				next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(docs), true
				if err := producer.Emit(t.Context(), supplier.Named(), &next, map[manifest.NamedResource]*store.SourceArtifact{repo: input}); err != nil {
					t.Error(err)
				}
				producer.Reads.Accept()
			}
			publish(s1)
			inventory, ok := st.GetArtifact(supplier.Named(), store.ArtifactContent).(*store.Emission)
			if !ok || !inventory.Complete || st.EmissionOwner(repo) != supplier.Named() {
				t.Fatal("S1 supplying emission is not complete and visible")
			}
			st.UpdateStatus(supplier.Named(), store.StatusInfo{Status: store.StatusReady})
		} else {
			st.AddObject(&manifest.GitRepository{Name: repo.Name, Namespace: repo.Namespace})
			publish(s1)
		}
		if st.GetArtifact(repo, store.ArtifactContent) != s1 {
			t.Fatal("S1 source is not visible before scheduler start")
		}
		firstGenerator, firstConsumer, startPublication := make(chan struct{}), make(chan struct{}), make(chan struct{})
		committed, release, settled, finishDriver := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		driverDone := make(chan struct{})
		var genSignaled, consumerSignaled, successorSignaled, paused atomic.Bool
		var work generatorWork
		tasks := &convergenceTasks{Service: bodies, generator: obj.Named(), work: &work}
		tasks.completed = func(name string) {
			if name == "schedule/"+driver.Named().String() {
				close(driverDone)
			}
			if name == "schedule/"+obj.Named().String() || name == "schedule/settle/"+obj.Named().String() {
				status, _ := st.GetStatus(obj.Named())
				if status.Status == store.StatusReady && genSignaled.CompareAndSwap(false, true) {
					close(firstGenerator)
				}
			}
			if name == "schedule/"+consumer.Named().String() || name == "schedule/settle/"+consumer.Named().String() {
				inventory, ok := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
				if ok && inventory.Complete && len(inventory.Manifests) == 1 {
					value := inventory.Manifests[0].Document["data"].(map[string]any)["value"]
					if value == "S1" && consumerSignaled.CompareAndSwap(false, true) {
						close(firstConsumer)
					}
					if value == "S2" && successorSignaled.CompareAndSwap(false, true) {
						close(settled)
					}
				}
			}
		}
		wait := func(ctx context.Context, ch <-chan struct{}) bool {
			var received bool
			bodies.YieldSlot(func() {
				select {
				case <-ch:
					received = true
				case <-ctx.Done():
				}
			})
			return received
		}
		d := &convergenceDispatcher{generator: generator, id: obj.Named(), consumer: ks, source: src, driver: driver.Named(), work: &work}
		d.drive = func(ctx context.Context) {
			if !wait(ctx, firstGenerator) || !wait(ctx, firstConsumer) || !wait(ctx, startPublication) {
				return
			}
			bodies.YieldSlot(func() {
				if change == "ignored-declaration-metadata" || change == "selected-uid" || change == "selected-binding" {
					updated := manifest.DeepCopyMap(doc)
					updated["metadata"].(map[string]any)["annotations"] = map[string]any{"ignored": "new"}
					if change == "selected-uid" {
						updated["metadata"].(map[string]any)["uid"] = "next"
					}
					selected := manifest.BaseManifest(obj)
					if change == "selected-binding" {
						updated["spec"].(map[string]any)["sources"].([]any)[0].(map[string]any)["name"] = rebound.Name
						parsed, authoritative, _, err := manifest.ParseDoc(updated, manifest.ParseDocOptions{})
						if err != nil {
							t.Fatal(err)
						}
						selected, updated = parsed, authoritative
					}
					st.AddObject(selected, updated)
				} else {
					publish(s2)
				}
			})
			if change == "selected-union" {
				wait(ctx, settled)
			}
			wait(ctx, finishDriver)
		}
		s := schedule.New(tasks, d)
		pause := func(id manifest.NamedResource, _ any) {
			if change == "selected-union" && id == repo && st.GetArtifact(repo, store.ArtifactContent) == s2 && paused.CompareAndSwap(false, true) {
				close(committed)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
		}
		for _, event := range []store.EventKind{store.EventObjectAdded, store.EventArtifactUpdated} {
			defer st.AddListener(event, pause, false)()
		}
		defer st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) }, false)()
		for _, event := range []store.EventKind{store.EventArtifactUpdated, store.EventStatusUpdated} {
			defer st.AddListener(event, func(manifest.NamedResource, any) { s.OnPublication() }, false)()
		}
		s.Seed([]schedule.NodeID{obj.Named(), consumer.Named(), driver.Named()})
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		defer func() {
			cancel()
			bodies.BlockTillDone()
		}()
		synctest.Wait()
		select {
		case <-firstGenerator:
		default:
			status, _ := st.GetStatus(obj.Named())
			t.Fatalf("initial generator did not settle: %+v", status)
		}
		select {
		case <-firstConsumer:
		default:
			status, _ := st.GetStatus(consumer.Named())
			t.Fatalf("initial consumer did not settle: status=%+v inventory=%+v generator=%+v", status, st.GetArtifact(consumer.Named(), store.ArtifactContent), st.GetArtifact(obj.Named(), store.ArtifactContent))
		}
		before := work.snapshot()
		if work.active.Load() != 0 || before.Admitted != 1 || before.Dispatched != 1 || before.Settled != 0 || before.Acquires != 1 {
			t.Fatalf("S1 boundary has outstanding or unexpected generator work: active=%d work=%+v", work.active.Load(), before)
		}
		beforeSweep := d.sweeps.Load()
		close(startPublication)
		if change == "selected-union" {
			synctest.Wait()
			select {
			case <-committed:
			default:
				t.Fatal("S2 publication did not reach its paused notification")
			}
			if st.GetArtifact(repo, store.ArtifactContent) != s2 {
				t.Fatal("publication listener ran before S2 commit/unlock")
			}
			inventory := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
			if inventory.Manifests[0].Document["data"].(map[string]any)["value"] != "S1" {
				t.Fatal("paused notification did not permit the last completed S1 inventory")
			}
			select {
			case <-driverDone:
				t.Fatal("driver finished before notification release")
			default:
			}
			close(release)
			synctest.Wait()
			select {
			case <-settled:
			default:
				t.Fatalf("consumer did not settle to S2 after release: generator work=%+v inventory=%+v", work.snapshot(), st.GetArtifact(consumer.Named(), store.ArtifactContent))
			}
		}
		synctest.Wait()
		// An explicit publication requests an acknowledged real Sweep even when the descriptor deduplicates.
		s.OnPublication()
		synctest.Wait()
		if d.sweeps.Load() <= beforeSweep {
			t.Fatal("no acknowledged Sweep after publication")
		}
		want := before
		if strings.HasPrefix(change, "selected-") {
			want.Admitted++
			want.Slots++
			want.Dispatched++
			want.Acquires++
		}
		atSweep := work.snapshot()
		if diff := cmp.Diff(want, atSweep); diff != "" {
			t.Errorf("%s work after acknowledged Sweep (-want +got):\n%s", change, diff)
		}
		if work.active.Load() != 0 {
			t.Error("generator closure remains in flight after acknowledged Sweep")
		}
		close(finishDriver)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		bodies.BlockTillDone()
		if bodies.Failures() != 0 || work.active.Load() != 0 || len(slices.Collect(st.Sweep())) != 0 {
			t.Fatalf("undrained boundary: task failures=%d", bodies.Failures())
		}
		after := work.snapshot()
		if diff := cmp.Diff(atSweep, after); diff != "" {
			t.Errorf("%s delayed generator work after final drain (-want +got):\n%s", change, diff)
		}
		// No outstanding task, dispatch or Settle structurally excludes slot reacquisition and other Acquire paths.
		value := "S1"
		if change == "selected-union" {
			value = "S2"
		}
		artifact := st.GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}, store.ArtifactContent).(*store.SourceArtifact)
		if strings.HasPrefix(change, "unselected-") {
			if artifact.Revision != "latest@"+artifact.Digest || len(artifact.Metadata) != 0 {
				t.Fatalf("content-only output consumed metadata: %+v", artifact)
			}
		} else {
			expected := s1
			if strings.HasPrefix(change, "selected-") {
				expected = s2
			}
			origin := expected.Metadata["org.opencontainers.image.revision"]
			if origin == "" {
				origin = expected.Revision
			}
			revision := expected.Revision
			if change == "origin-without-revision" || change == "selected-origin-fallback" {
				revision = "latest@" + artifact.Digest
			}
			if artifact.Revision != revision || artifact.Metadata["org.opencontainers.image.revision"] != origin {
				t.Fatalf("final selected metadata = %+v", artifact)
			}
		}
		if change == "selected-uid" {
			_, doc, emission := st.GetEmission(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"})
			defer emission.Finish()
			if doc["metadata"].(map[string]any)["labels"].(map[string]any)["source.extensions.fluxcd.io/generator"] != "next" {
				t.Fatal("successor retained the old UID")
			}
		}
		inventory := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
		if !inventory.Complete || len(inventory.Manifests) != 1 || inventory.Manifests[0].Document["data"].(map[string]any)["value"] != value {
			t.Fatalf("final consumer inventory = %+v", inventory)
		}
		documents, err = json.Marshal(inventory.Manifests)
		if err != nil {
			t.Fatal(err)
		}
	})
	return documents
}
