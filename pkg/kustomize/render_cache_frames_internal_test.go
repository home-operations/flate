package kustomize

import (
	"io/fs"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRender_CacheFrames(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("render persistence eligible=%v", eligible)
	for _, tc := range []struct {
		name      string
		snapshot  readSetSnapshot
		malformed bool
		corrupt   bool
		invalid   bool
		empty     bool
		immutable bool
	}{
		{name: "malformed", malformed: true},
		{name: "validated corrupt payload", corrupt: true},
		{name: "corrupt payload invalid source", corrupt: true, invalid: true},
		{name: "mismatched", snapshot: readSetSnapshot{Files: []localEntry{{Path: "cm.yaml", Hash: "mismatch"}}}},
		{name: "empty", empty: true},
		{name: "immutable", immutable: true, snapshot: readSetSnapshot{Files: []localEntry{{Path: "unavailable", Hash: "mismatch"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderCacheFramesCase(t, &testRenderCacheFramesCaseInputs{eligible: &eligible, tc: &tc})
		},
		)
	}
}

type pausedRenderFS struct {
	fs.FS

	entered chan struct{}
	release chan struct{}
	once    sync.Once
	builds  *atomic.Int64
}

func (p *pausedRenderFS) ReadFile(name string) ([]byte, error) {
	if name != "cm.yaml" {
		return fs.ReadFile(p.FS, name)
	}
	if p.entered != nil {
		p.once.Do(func() { close(p.entered); <-p.release })
	}
	var pcs [64]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if frame.Function == "github.com/home-operations/flate/internal/kustomize.Build" {
			if p.builds != nil {
				p.builds.Add(1)
			}
			break
		}
		if !more {
			break
		}
	}

	return fs.ReadFile(p.FS, name)
}

type testRenderCacheFramesCaseInputs struct {
	eligible *bool
	tc       *struct {
		name      string
		snapshot  readSetSnapshot
		malformed bool
		corrupt   bool
		invalid   bool
		empty     bool
		immutable bool
	}
}

func testRenderCacheFramesCase(t *testing.T, testCaseInput *testRenderCacheFramesCaseInputs) {
	t.Helper()

	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("fresh"),
	})
	if testCaseInput.tc.invalid {
		testutil.WriteFile(t, root, "cm.yaml", "apiVersion: [\n")
	}
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	source := localSource(t, root)
	if testCaseInput.tc.immutable {
		source.Key, source.Content = "immutable-pin", "immutable-generation"
		source.Revision, source.Digest = "revision", "digest"
	}
	req := RenderRequest{Source: source, Label: root, SubPath: ".", Spec: demoSpec}
	var fresh []map[string]any
	var freshErr error
	if testCaseInput.tc.corrupt {
		fresh, _, freshErr = testutil.RenderMaps(
			Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{}),
		)
	}
	key := renderKey(&req, root, nil)
	frame := testRenderFrame(t, new(testCaseInput.tc.snapshot), testRenderPayload(t, "STALE"))
	switch {
	case testCaseInput.tc.malformed:
		frame = []byte{0, 0, 0, 1, 'x'}

	case testCaseInput.tc.corrupt || testCaseInput.tc.name == "mismatched":
		frame = testRenderFrame(t, new(testCaseInput.tc.snapshot), []byte("corrupt"))

	case testCaseInput.tc.empty:
		frame = testRenderFrame(t, new(testCaseInput.tc.snapshot), testRenderEncoding(t, nil))
	}
	publishTestFrame(
		t,
		cache.owner,
		sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
		frame,
	)
	out, receipt22, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt22},
			)
			assert.NoError(t, recordErr)
		}
	}

	if testCaseInput.tc.invalid {
		assert.FatalIf(
			t,
			err == nil || freshErr == nil || err.Error() != freshErr.Error() || len(out) != 0,
			"fresh render failure: %v, %v; want %v",
			out,
			err,
			freshErr,
		)
		return
	}
	assert.NoError(t, err)
	if testCaseInput.tc.corrupt {
		assert.NoError(t, freshErr)
		assert.Diff(t, out, fresh)
		again, receipt23, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := cache.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt23},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, again, fresh)
	}
	switch {
	case testCaseInput.tc.immutable:
		assert.FatalIf(
			t,
			!hasRenderField(out, "witness", "STALE"),
			"immutable pin replayed mutable validation: %s",
			out,
		)

	case testCaseInput.tc.empty:
		assert.FatalIf(t, len(out) != 0, "complete empty frame missed: %s", out)

	default:
		assert.FatalIf(t, !hasRenderField(out, "name", "fresh"), "candidate prevented a fresh render: %s", out)
	}
	cache.Close()
	{
		err := cache.owner.Close()
		assert.NoError(t, err)
	}
	cache = newTestTreeCache(t, cacheDir)
	_, stored, hit := readTestRenderFrame(t, cache, key)
	if hit != (*testCaseInput.eligible) || hit && !reflect.DeepEqual(stored, out) {
		t.Fatalf("completed frame = %s, %v; eligible = %v", stored, hit, renderPersistenceEligible(t))
	}
	if testCaseInput.tc.corrupt {
		again, receipt24, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := cache.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt24},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Diff(t, again, fresh)
		if *testCaseInput.eligible {
			t.Log("verified corrupt payload recovery, replacement, subsequent and reopened hits")
		}
	}
}
