package kustomize

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
)

type remoteIsolationResult struct {
	request RenderRequest
	snap    readSetSnapshot
	output  []map[string]any
	err     error
}

func TestRender_RemoteCacheIsolation(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted isolation assertions executed=%v; concurrent snapshot assertions always execute", eligible)
	var shared, distinct atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/shared" {
			shared.Add(1)
			entered <- struct{}{}
			<-release
		} else {
			distinct.Add(1)
		}
		_, _ = io.WriteString(w, "contents")
	}))
	t.Cleanup(srv.Close)
	c := newTestTreeCache(t, t.TempDir())

	results := make(chan remoteIsolationResult, 2)
	requests := make([]RenderRequest, 2)
	for i := range requests {
		name := fmt.Sprintf("local-%d", i)
		root := testutil.WriteTree(t, map[string]string{
			"kustomization.yaml": "resources: [" + name + ".yaml]\nconfigMapGenerator: [{name: generated, files: ['first=" + srv.URL + "/shared', 'second=" + srv.URL + "/shared', 'distinct=" + srv.URL + "/" + name + "']}]\n",
			name + ".yaml":       cmYAML(name),
		})
		requests[i] = RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	for _, request := range requests {
		wg.Go(func() {
			rec := newReadSet(request.Label)
			view := newTreeFS(request.Source.FS, request.Label, rec)
			err := preflightRemoteResources(t.Context(), c, view, request.Label)
			if err != nil {
				results <- remoteIsolationResult{err: err}
				return
			}
			if _, buildTestTreeFSErr := buildTestTreeFS(view); buildTestTreeFSErr != nil {
				results <- remoteIsolationResult{err: buildTestTreeFSErr}
				return
			}
			out, receipt32, err := testutil.RenderMaps(Render(t.Context(), c, request, sourcecache.RetryConfig{}))
			if err == nil {
				{
					recordErr := c.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt32},
					)
					assert.NoError(t, recordErr)
				}
			}

			results <- remoteIsolationResult{request: request, snap: rec.snapshot(), output: out, err: err}
		})
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent renders did not reach shared acquisition")
	}
	close(release)
	released = true
	assertRemoteIsolationResults(t, c, results, len(requests))
	if shared.Load() != 1 || distinct.Load() != 2 {
		t.Fatalf("shared requests=%d, distinct=%d", shared.Load(), distinct.Load())
	}
}

func assertRemoteIsolationResults(t *testing.T, c *TreeCache, results <-chan remoteIsolationResult, count int) {
	t.Helper()
	for range count {
		got := <-results
		assert.FatalIf(
			t,
			got.err != nil || !hasRenderField(got.output, "first", "contents"),
			"concurrent render: %s, %v",
			got.output,
			got.err,
		)
		snap := got.snap
		if persisted, _, hit := readTestRenderFrame(t, c, renderKey(&got.request, got.request.Label, nil)); hit {
			snap = persisted
		}
		assert.FatalIf(
			t,
			len(snap.Remotes) != 2 || snap.Remotes[0].Input.URL >= snap.Remotes[1].Input.URL,
			"remote descriptors not private/deduplicated/sorted: %+v",
			snap,
		)
		control, err := fs.ReadFile(got.request.Source.FS, "kustomization.yaml")
		assert.NoError(t, err)
		for index := range snap.Remotes {
			entry := &snap.Remotes[index]
			assert.FatalIf(
				t,
				!bytes.Contains(control, []byte(entry.Input.URL)),
				"another render's dependency leaked: %+v",
				*entry,
			)
		}
		for _, file := range snap.Files {
			{
				_, err := fs.Stat(got.request.Source.FS, file.Path)
				assert.FatalIf(t, err != nil, "another source's read leaked: %+v, %v", file, err)
			}
		}
		{
			valid, err := snap.stillValid(
				t.Context(),
				c,
				&treeFS{lower: got.request.Source.FS},
				false,
			)
			assert.FatalIf(t, err != nil ||
				!valid, "private snapshot invalid: %+v, %t, %v", snap, valid, err)
		}
	}
}
