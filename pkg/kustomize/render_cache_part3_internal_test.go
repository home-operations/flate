package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/opencontainers/go-digest"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRender_CacheAcquisitionOverlap(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk_%t", disk), func(t *testing.T) {
			t.Parallel()
			files := map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("old")}
			root, changed := testutil.WriteTree(t, files), testutil.WriteTree(t, files)
			storage := ""
			if disk {
				storage = t.TempDir()
			}
			owner := newTestTreeCache(t, storage).owner
			firstCache, secondCache := NewTreeCache(
				owner,
				nil,
				task.NewBounded(2),
			), NewTreeCache(
				owner,
				nil,
				task.NewBounded(2),
			)
			defer firstCache.Close()
			defer secondCache.Close()
			var builds atomic.Int64
			synctest.Test(t, func(t *testing.T) {
				first := &pausedRenderFS{
					FS:      os.DirFS(root),
					entered: make(chan struct{}),
					release: make(chan struct{}),
					builds:  &builds,
				}
				second := &pausedRenderFS{FS: os.DirFS(changed), builds: &builds}
				run := func(c *TreeCache, input fs.FS, want string) {
					out, _, err := testutil.RenderMaps(
						Render(
							t.Context(),
							c,
							RenderRequest{
								Source:  &store.SourceArtifact{FS: input},
								Label:   root,
								SubPath: ".",
								Spec:    demoSpec,
							},
							sourcecache.RetryConfig{},
						),
					)
					assert.NoError(t, err)
					assert.Equal(t, hasRenderField(out, "name", want), true)
				}
				done := make(chan struct{})
				go func() { defer close(done); run(firstCache, first, "old") }()
				<-first.entered
				var queued sync.WaitGroup
				for range 2 {
					queued.Go(func() { run(secondCache, second, "new") })
				}
				synctest.Wait()
				// A different acquisition key MUST progress while this render owns its key.
				_, _, _, err := owner.Blob(
					t.Context(),
					sourcecache.BlobKey{Content: "unrelated-source"},
					sourcecache.Policy{MaxAge: sourcecache.Forever},
					func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := io.WriteString(w, "source")
						return sourcecache.ProducerResult{}, err
					},
				)
				assert.NoError(t, err)

				testutil.WriteFile(t, changed, "cm.yaml", cmYAML("new"))
				close(first.release)
				<-done
				queued.Wait()
				assert.Equal(t, builds.Load(), int64(2))
				run(secondCache, second, "new")
				assert.Equal(t, builds.Load(), int64(2))
			})
		})
	}
}

func TestRender_CacheStorageFailures(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"lookup", "read", "publication"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			testRenderCacheStorageFailuresCase(t, &fault)
		},
		)
	}
}

func TestRender_RemoteCacheReachability(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted reachability assertions executed=%v; validator assertions always execute", eligible)
	t.Run("empty-and-canceled", func(t *testing.T) {
		t.Parallel()
		testRenderRemoteCacheReachabilityEmptyAndCanceled(t)
	})
	t.Run(
		"cancellation-during-local-mismatch",
		func(t *testing.T) {
			t.Parallel()
			testRenderRemoteCacheReachabilityCancellationDuringLocalMismatch(t)
		},
	)
	for _, scenario := range []string{"local-removes", "nested-changes", "parent-removes", "reachable-fails", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testRenderRemoteCacheReachabilityCase(t, &scenario)
		},
		)
	}
}

func testRenderCacheStorageFailuresCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	if !renderPersistenceEligible(t) {
		t.Skip("storage faults require an eligible build")
	}
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")},
	)

	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	if (*testCaseInput) == "lookup" || (*testCaseInput) == "read" {
		_, receipt26, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := cache.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt26},
				)
				assert.NoError(t, recordErr)
			}
		}
		{
			assert.NoError(t, err)
		}
	}
	if (*testCaseInput) == "read" {
		key := sourcecache.BlobKey{Content: renderKey(&req, root, nil), Render: sourcecache.RenderAccepted}
		reader, _, _, err := cache.owner.Blob(
			t.Context(),
			key,
			sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		assert.NoError(t, err)
		data, err := fs.ReadFile(reader, "content")
		assert.NoError(t, err)

		sum := digest.FromBytes(data)
		blob := filepath.Join(cacheDir, "oci", "blobs", sum.Algorithm().String(), sum.Encoded())
		assert.NoError(t, os.Remove(blob))
		assert.NoError(t, os.Mkdir(blob, 0o750))
	} else {
		assert.NoError(t, os.RemoveAll(cacheDir))
	}
	for range 2 {
		out, receipt27, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			err = cache.RecordRender(t.Context(), RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt27})
		}

		if (*testCaseInput) == "read" {
			{
				pathErr, ok := errors.AsType[*fs.PathError](err)
				assert.FatalIf(
					t,
					!ok || pathErr.Op != "read",
					"read fault did not precede frame decoding: %v",
					err,
				)
			}
		}
		wantDocuments := 0
		if (*testCaseInput) == "publication" {
			wantDocuments = 1
		}
		{
			assert.FatalIf(t, len(out) != wantDocuments || !errors.Is(err, manifest.ErrFlux) ||
				errors.Is(
					err,
					errdef.ErrNotFound,
				), "%s fault returned output %q, error %v", (*testCaseInput), out, err)
		}
	}
}

func testRenderRemoteCacheReachabilityEmptyAndCanceled(t *testing.T) {
	t.Helper()
	valid, err := (readSetSnapshot{}).stillValid(t.Context(), nil, &treeFS{}, false)
	{
		assert.FatalIf(t, err != nil || !valid, "empty snapshot did work: %t, %v", valid, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	valid, err = (readSetSnapshot{Files: []localEntry{{Path: "unread", Hash: "hash"}}}).stillValid(
		ctx,
		nil,
		&treeFS{},
		false,
	)
	{
		assert.FatalIf(
			t,
			valid || !errors.Is(err, context.Canceled),
			"cancellation did not precede replay: %t, %v",
			valid,
			err,
		)
	}
}

func testRenderRemoteCacheReachabilityCancellationDuringLocalMismatch(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lower := &pausedRenderFS{
		FS:      memoryTree(t, map[string]string{"cm.yaml": cmYAML("local")}),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		_, err := (readSetSnapshot{Files: []localEntry{{Path: "cm.yaml", Hash: "mismatch"}}}).stillValid(
			ctx,
			nil,
			&treeFS{lower: lower},
			false,
		)
		done <- err
	}()
	<-lower.entered
	cancel()
	close(lower.release)
	{
		err := <-done
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "local mismatch swallowed cancellation: %v", err)
	}
}

func testRenderRemoteCacheReachabilityCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	var version, hits atomic.Int32
	var unavailable, blocked atomic.Bool
	entered := make(chan struct{}, 1)
	var trace remoteHTTPTrace
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record := remoteHTTPRequest{
			Method:   r.Method,
			Path:     r.URL.Path,
			ETag:     r.Header.Get("If-None-Match"),
			Modified: r.Header.Get("If-Modified-Since"),
			Status:   http.StatusOK,
		}
		defer func() {
			trace.mu.Lock()
			trace.requests = append(trace.requests, record)
			trace.mu.Unlock()
		}()
		hits.Add(1)
		if blocked.Load() {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		if unavailable.Load() {
			record.Status, record.Bytes = http.StatusNotFound, len("unavailable\n")
			http.Error(w, "unavailable", http.StatusNotFound)
			return
		}
		record.Bytes, _ = io.WriteString(w, cmYAML(fmt.Sprintf("child-v%d", version.Load())))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			trace.connections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(func() {
		srv.Close()
		t.Logf("reachability: requests=%+v accepted_connections=%d", trace.snapshot(), trace.connections.Load())
	})
	const gitURL = "file:///example.invalid/o/parent.git"
	control := "resources: ['" + gitURL + "?ref=main']\n"
	if (*testCaseInput) == "local-removes" {
		control = "resources: ['" + srv.URL + "/child.yaml']\n"
	}
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": control, "local.yaml": cmYAML("local")},
	)
	cacheDir := t.TempDir()
	parentChanged := false
	newCache := func() *TreeCache {
		c := newTestTreeCache(t, cacheDir)
		files := map[string]string{"kustomization.yaml": "resources: ['" + srv.URL + "/child.yaml']\n"}
		revision := "1"
		if parentChanged {
			files = map[string]string{
				"kustomization.yaml": "resources: [local.yaml]\n",
				"local.yaml":         cmYAML("parent-v2"),
			}
			revision = "2"
		}
		remote := memoryTree(t, files)
		c.SetGitBaseFetcher(
			func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				return &store.SourceArtifact{
					FS:       remote,
					Key:      "stable-parent",
					Content:  revision,
					Revision: revision,
				}, nil
			},
		)
		return c
	}
	request := func() RenderRequest {
		return RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	}
	version.Store(1)
	c := newCache()
	out, receipt28, err := testutil.RenderMaps(Render(t.Context(), c, request(), sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := c.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt28},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(out, "name", "child-v1"),
			"initial render: %s, %v",
			out,
			err,
		)
	}
	rec := newReadSet(root)
	view := newTreeFS(request().Source.FS, root, rec)
	{
		preflightRemoteResourcesErr := preflightRemoteResources(t.Context(), c, view, root)
		assert.NoError(t, preflightRemoteResourcesErr)
	}
	snapFrame, err := decodeFrame(testRenderFrame(t, rec.snapshot(), testRenderEncoding(t, out)))
	snap := snapFrame.header.Reads
	{
		assert.NoError(t, err)
	}
	{
		valid, stillValidErr := snap.stillValid(
			t.Context(),
			c,
			&treeFS{lower: request().Source.FS},
			false,
		)
		assert.FatalIf(t, stillValidErr != nil ||
			!valid, "unchanged recorded graph: valid=%t, %v", valid, stillValidErr)
	}
	renderRequestSnapshot := request()
	key := renderKey(&renderRequestSnapshot, root, nil)
	if persisted, _, hit := readTestRenderFrame(t, c, key); hit {
		snap = persisted
		publishTestFrame(
			t,
			c.owner,
			sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
			testRenderFrame(t, snap, testRenderPayload(t, "SENTINEL")),
		)
	}
	closeTestTreeCache(t, c)
	hits.Store(0)
	switch *testCaseInput {
	case "local-removes":
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [local.yaml]\n")
		unavailable.Store(true)
	case "nested-changes":
		version.Store(2)
	case "parent-removes":
		parentChanged = true
		unavailable.Store(true)
	case "reachable-fails":
		unavailable.Store(true)
	case "cancellation":
		blocked.Store(true)
	}
	c = newCache()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if (*testCaseInput) == "cancellation" {
		done := make(chan error, 1)
		go func() {
			valid, stillValidErr2 := snap.stillValid(ctx, c, &treeFS{lower: request().Source.FS}, false)
			if valid {
				stillValidErr2 = errors.New("canceled validation certified a hit")
			}
			done <- stillValidErr2
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("validation did not reach the remote")
		}
		cancel()
		select {
		case errResult := <-done:
			{
				assert.FatalIf(t, !errors.Is(errResult, context.Canceled), "cancellation error: %v", errResult)
			}
		case <-time.After(time.Second):
			t.Fatal("validation did not promptly observe cancellation")
		}
	} else {
		valid, stillValidErr3 := snap.stillValid(ctx, c, &treeFS{lower: request().Source.FS}, false)
		{
			assert.FatalIf(
				t,
				stillValidErr3 != nil || valid,
				"changed/unavailable graph: valid=%t, %v",
				valid,
				stillValidErr3,
			)
		}
	}
	if ((*testCaseInput) == "local-removes" || (*testCaseInput) == "parent-removes") &&
		hits.Load() != 0 {
		t.Fatalf("removed edge fetched %d times during validation", hits.Load())
	}
	var receipt29 *manifest.RenderReceipt
	out, receipt29, err = testutil.RenderMaps(Render(ctx, c, request(), sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := c.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt29},
			)
			assert.NoError(t, recordErr)
		}
	}

	switch *testCaseInput {
	case "reachable-fails":
		{
			assert.FatalIf(
				t,
				err == nil || len(out) != 0,
				"unavailable reachable input returned %q, %v",
				out,
				err,
			)
		}
	case "cancellation":
		{
			assert.FatalIf(
				t,
				!errors.Is(err, context.Canceled) || len(out) != 0,
				"canceled render returned %q, %v",
				out,
				err,
			)
		}
	default:
		want := map[string]string{"local-removes": "name: local", "nested-changes": "name: child-v2", "parent-removes": "name: parent-v2"}[(*testCaseInput)]
		{
			assert.FatalIf(t, err != nil ||
				!hasRenderField(
					out,
					strings.SplitN(want, ": ", 2)[0],
					strings.SplitN(want, ": ", 2)[1],
				), "current graph: %s, %v; want %q", out, err, want)
		}
	}
	if ((*testCaseInput) == "local-removes" || (*testCaseInput) == "parent-removes") &&
		hits.Load() != 0 {
		t.Fatalf("removed edge fetched %d times after fresh rendering", hits.Load())
	}
	closeTestTreeCache(t, c)
}
