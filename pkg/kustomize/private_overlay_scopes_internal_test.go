package kustomize

import (
	"errors"
	"fmt"
	"io/fs"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type privateOverlayRender struct {
	entered, exited chan struct{}
	once            sync.Once
	reached         bool
}

type privateOverlayBatch struct {
	released chan struct{}
	release  func()
	complete bool
}

type privateOverlayBarrier struct {
	renders []privateOverlayRender
	batches []privateOverlayBatch
}

func newPrivateOverlayBarrier(workers int) *privateOverlayBarrier {
	b := &privateOverlayBarrier{
		renders: make([]privateOverlayRender, workers),
		batches: make([]privateOverlayBatch, workers),
	}
	for n := range workers {
		b.renders[n].entered = make(chan struct{})
		b.renders[n].exited = make(chan struct{})
		released := make(chan struct{})
		b.batches[n].released = released
		b.batches[n].release = sync.OnceFunc(func() { close(released) })
	}
	return b
}

type privateOverlayFS struct {
	fs.FS

	render  *privateOverlayRender
	batches []privateOverlayBatch
}

func (f *privateOverlayFS) ReadFile(name string) ([]byte, error) {
	if name == "kustomization.yaml" {
		f.render.once.Do(func() {
			// This source read MUST pause before Render acquires BuildMutex.
			f.render.reached = true
			close(f.render.entered)
			for n := range f.batches {
				<-f.batches[n].released
			}
		})
	}
	return fs.ReadFile(f.FS, name)
}

func (b *privateOverlayBatch) run(renders []privateOverlayRender, check func() bool) bool {
	defer b.release()
	for n := range renders {
		select {
		case <-renders[n].entered:
		case <-renders[n].exited:
			select {
			case <-renders[n].entered:
			default:
				return false
			}
		}
	}
	b.complete = check()
	return b.complete
}

func testRenderConcurrentPrivateOverlaysCase(
	t *testing.T,
	testCaseInput *testRenderConcurrentPrivateOverlaysCaseInputs,
	enforceOverlap bool,
) {
	t.Helper()
	cache := newTestTreeCache(t, t.TempDir())
	done := make(chan struct{})
	barrier := newPrivateOverlayBarrier(*testCaseInput.workers)
	lookupRenders := barrier.renders
	if !enforceOverlap {
		lookupRenders = nil
	}
	var renders, lookups sync.WaitGroup
	for n := range *testCaseInput.workers {
		renders.Go(func() {
			defer close(barrier.renders[n].exited)
			original := parsedKS(t, ksDoc(map[string]any{"postBuild": map[string]any{}}))
			projected := original.Clone()
			projected.NamePrefix = fmt.Sprintf("render%d-", n)
			projected.TargetNamespace = fmt.Sprintf("namespace%d", n)
			projected.PostBuild.Substitute = map[string]string{"SLOT": strconv.Itoa(n)}
			spec, err := Prepare(projected, nil, depwait.NewMissingPolicy(nil))
			if err != nil {
				t.Error(err)
				return
			}
			source := **testCaseInput.artifact
			if enforceOverlap {
				source.FS = &privateOverlayFS{FS: source.FS, render: &barrier.renders[n], batches: barrier.batches}
			} else {
				for b := range barrier.batches {
					<-barrier.batches[b].released
				}
			}
			out, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					cache,
					RenderRequest{Source: &source, Label: "/absent", SubPath: ".", Spec: spec},
					sourcecache.RetryConfig{},
				),
			)
			assert.ErrorIf(
				t,
				err != nil || !hasRenderField(out, "name", fmt.Sprintf("render%d-shared", n)) ||
					!hasRenderField(
						out,
						"marker",
						"patched",
					) || !hasRenderField(out, "payload", "remote") ||
					!hasRenderField(
						out,
						"namespace",
						fmt.Sprintf("namespace%d", n),
					) || spec.PostBuild.Substitute["SLOT"] != strconv.Itoa(n) ||
					original.TargetNamespace != "" || len(original.PostBuild.Substitute) != 0,
				"private output = %s, %v",
				out,
				err,
			)
		})
		lookups.Go(func() {
			runPrivateOverlayLookup(t, &privateOverlayLookupInputs{
				scenario: &testCaseInput, done: done,
				renders: lookupRenders, batch: &barrier.batches[n],
			})
		})
	}
	renders.Wait()
	close(done)
	lookups.Wait()
	for n := range barrier.renders {
		if enforceOverlap {
			assert.ErrorIf(t, !barrier.renders[n].reached, "render %d returned without entering its read barrier", n)
		}
		assert.ErrorIf(t, !barrier.batches[n].complete, "lookup %d did not complete its initial scope batch", n)
	}
	for path, want := range *testCaseInput.files {
		got, err := fs.ReadFile((*testCaseInput.artifact).FS, path)
		assert.FatalIf(
			t,
			err != nil || string(got) != want,
			"shared source %s mutated: %s, %v",
			path,
			got,
			err,
		)
	}
}

type privateOverlayLookupInputs struct {
	scenario **testRenderConcurrentPrivateOverlaysCaseInputs
	done     <-chan struct{}
	renders  []privateOverlayRender
	batch    *privateOverlayBatch
}

func runPrivateOverlayLookup(t *testing.T, inputs *privateOverlayLookupInputs) {
	t.Helper()

	ok := inputs.batch.run(inputs.renders, func() bool {
		return checkPrivateOverlayScopes(t, inputs.scenario)
	})
	if !ok {
		t.Error("lookup did not complete a scope batch inside every render")
		return
	}
	for {
		select {
		case <-inputs.done:
			return
		default:
		}
		if !checkPrivateOverlayScopes(t, inputs.scenario) {
			return
		}
	}
}

func TestPrivateOverlayBarrier_FailureRelease(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		run  func(*testing.T)
	}{
		{"render_exit_before_entry", testPrivateOverlayBarrierRenderExit},
		{"lookup_exit_before_successful_batch", testPrivateOverlayBarrierLookupExit},
		{"read_failure_after_entry", testPrivateOverlayBarrierReadFailure},
		{"repeated_target_reads", testPrivateOverlayBarrierRepeatedReads},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testPrivateOverlayBarrierRenderExit(t *testing.T) {
	t.Helper()
	barrier := newPrivateOverlayBarrier(2)
	source := &privateOverlayFS{
		FS:     fstest.MapFS{"kustomization.yaml": {Data: []byte("source")}},
		render: &barrier.renders[0], batches: barrier.batches,
	}
	var renders, lookups sync.WaitGroup
	var readErr error
	renders.Go(func() {
		defer close(barrier.renders[0].exited)
		_, readErr = fs.ReadFile(source, "kustomization.yaml")
	})
	<-barrier.renders[0].entered
	close(barrier.renders[1].exited)
	started := make([]bool, 2)
	for n := range barrier.batches {
		lookups.Go(func() {
			barrier.batches[n].run(barrier.renders, func() bool {
				started[n] = true
				return true
			})
		})
	}
	renders.Wait()
	lookups.Wait()
	assert.NoError(t, readErr)
	assert.ErrorIf(t, barrier.renders[1].reached, "%s", "exited render counted as entered")
	for n := range barrier.batches {
		assert.ErrorIf(
			t,
			started[n] || barrier.batches[n].complete,
			"lookup %d accepted a missing render entry",
			n,
		)
	}
}

func testPrivateOverlayBarrierLookupExit(t *testing.T) {
	t.Helper()
	barrier := newPrivateOverlayBarrier(2)
	var renders, lookups sync.WaitGroup
	readErrors := make([]error, 2)
	for n := range barrier.renders {
		renders.Go(func() {
			defer close(barrier.renders[n].exited)
			source := &privateOverlayFS{
				FS:     fstest.MapFS{"kustomization.yaml": {Data: []byte("source")}},
				render: &barrier.renders[n], batches: barrier.batches,
			}
			_, readErrors[n] = fs.ReadFile(source, "kustomization.yaml")
		})
		lookups.Go(func() {
			barrier.batches[n].run(barrier.renders, func() bool {
				if n == 0 {
					runtime.Goexit()
				}
				return true
			})
		})
	}
	renders.Wait()
	lookups.Wait()
	for n := range barrier.renders {
		assert.NoError(t, readErrors[n])
		assert.ErrorIf(t, !barrier.renders[n].reached, "render %d missed entry", n)
	}
	assert.ErrorIf(t, barrier.batches[0].complete, "%s", "lookup exit counted as batch success")
	assert.ErrorIf(t, !barrier.batches[1].complete, "%s", "peer lookup did not complete")
}

func testPrivateOverlayBarrierReadFailure(t *testing.T) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		barrier := newPrivateOverlayBarrier(2)
		var renders sync.WaitGroup
		readErrors := make([]error, 2)
		for n := range barrier.renders {
			renders.Go(func() {
				defer close(barrier.renders[n].exited)
				source := &privateOverlayFS{
					FS: fstest.MapFS{}, render: &barrier.renders[n], batches: barrier.batches,
				}
				_, readErrors[n] = fs.ReadFile(source, "kustomization.yaml")
			})
		}
		for n := range barrier.renders {
			<-barrier.renders[n].entered
		}
		for n := range barrier.batches {
			synctest.Wait()
			for r := range barrier.renders {
				select {
				case <-barrier.renders[r].exited:
					t.Errorf("render %d resumed before batch %d released", r, n)
				default:
				}
			}
			barrier.batches[n].run(barrier.renders, func() bool { return true })
		}
		renders.Wait()
		for n := range barrier.renders {
			assert.ErrorIf(t, !barrier.batches[n].complete, "lookup %d lost batch evidence", n)
			assert.ErrorIf(
				t,
				!errors.Is(readErrors[n], fs.ErrNotExist),
				"source read %d = %v",
				n,
				readErrors[n],
			)
		}
	})
}

func testPrivateOverlayBarrierRepeatedReads(t *testing.T) {
	t.Helper()
	barrier := newPrivateOverlayBarrier(1)
	files := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("first")},
		"cm.yaml":            {Data: []byte("other")},
	}
	source := &privateOverlayFS{FS: files, render: &barrier.renders[0], batches: barrier.batches}
	other, err := fs.ReadFile(source, "cm.yaml")
	assert.FatalIf(t, err != nil || string(other) != "other", "other read = %s, %v", other, err)
	var renders sync.WaitGroup
	var first, second []byte
	var firstErr, secondErr error
	renders.Go(func() {
		defer close(barrier.renders[0].exited)
		first, firstErr = fs.ReadFile(source, "kustomization.yaml")
		files["kustomization.yaml"].Data = []byte("second")
		second, secondErr = fs.ReadFile(source, "kustomization.yaml")
	})
	ok := barrier.batches[0].run(barrier.renders, func() bool { return true })
	renders.Wait()
	assert.ErrorIf(t, !ok || !barrier.renders[0].reached, "%s", "repeated reads lost overlap evidence")
	assert.ErrorIf(t, firstErr != nil || string(first) != "first", "first read = %s, %v", first, firstErr)
	assert.ErrorIf(t, secondErr != nil || string(second) != "second", "second read = %s, %v", second, secondErr)
}

func checkPrivateOverlayScopes(t *testing.T, scenario **testRenderConcurrentPrivateOverlaysCaseInputs) bool {
	t.Helper()

	for _, tc := range *(*scenario).scopeCases {
		doc := map[string]any{
			"apiVersion": tc.version,
			"kind":       tc.kind,
			"metadata":   map[string]any{"name": "scope"},
		}
		parsed, err := manifest.ParseDoc(
			doc,
			manifest.ParseDocOptions{DefaultNamespace: "apps"},
		)
		obj, authoritative, changed := parsed.Object, parsed.Document, parsed.Changed
		if err != nil {
			t.Error(err)
			return false
		}
		metadata := manifest.DocMetadata(authoritative)
		ns := metadata.Namespace
		if obj.Named().Namespace != tc.namespace || ns != tc.namespace ||
			changed != (tc.namespace != "") {
			t.Errorf(
				"%s %s: identity=%s document=%q changed=%t",
				tc.version,
				tc.kind,
				obj.Named(),
				ns,
				changed,
			)
			return false
		}
		r := &manifest.ResourceSet{Namespace: "apps"}
		r.DependsOn = []fluxopv1.Dependency{
			{APIVersion: tc.version, Kind: tc.kind, Name: "scope"},
		}
		for ref := range manifest.References(r, manifest.ReferenceRelations) {
			if ref.Namespace != tc.namespace {
				t.Errorf(
					"%s %s dependency namespace=%q, want %q",
					tc.version,
					tc.kind,
					ref.Namespace,
					tc.namespace,
				)
				return false
			}
		}
	}
	return true
}
