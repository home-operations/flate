package kustomize

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/tree"
)

//nolint:paralleltest // test replaces process-wide dependency configuration.
func TestRender_RemoteValidationSlots(t *testing.T) {
	for _, mode := range []string{"validation", "miss", "standalone"} {
		t.Run(mode, func(t *testing.T) {
			testRenderRemoteValidationSlotsCase(t, &mode)
		},
		)
	}
}

//nolint:paralleltest // allocation measurements require serial execution.
func TestReadSet_NoRemotesNoAlloc(t *testing.T) {
	ctx, input := t.Context(), &treeFS{}
	var snap readSetSnapshot
	{
		allocations := testing.AllocsPerRun(1, func() {
			valid, err := snap.stillValid(ctx, nil, input, true)
			if !valid || err != nil {
				t.Fatalf("zero remote validation: valid=%t err=%v", valid, err)
			}
		})
		assert.FatalIf(t, allocations != 0, "zero remote validation allocations=%v, want 0", allocations)
	}
}

type remoteBody struct {
	io.Reader

	close func() error
}

func (b remoteBody) Close() error { return b.close() }

func testRenderRemoteValidationSlotsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		tasks := task.NewBounded(2)
		owner := cachetest.New(t, "", sourcecache.New)
		first, second := make(chan struct{}), make(chan struct{})
		releaseFirst, releaseSecond := sync.OnceFunc(
			func() { close(first) },
		), sync.OnceFunc(
			func() { close(second) },
		)
		defer releaseFirst()
		defer releaseSecond()
		entered := make(chan string, 16)
		var active atomic.Int32
		var exercise atomic.Bool
		var counts sync.Map
		start := func(name string) {
			{
				n := active.Add(1)
				assert.ErrorIf(t, n > 2, "active acquisition owners=%d, bound=2", n)
			}
			value, _ := counts.LoadOrStore(name, new(atomic.Int32))
			value.(*atomic.Int32).Add(1)
			entered <- name
		}
		gitInput := memoryTree(t, map[string]string{
			"app/kustomization.yaml": "resources: [git.yaml]\n",
			"app/git.yaml":           "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: git}\n",
		})
		newCache := func() *TreeCache {
			return newRemoteSlotCache(t, &remoteSlotCacheInputs{
				owner: &owner, tasks: &tasks, exercise: &exercise, start: &start,
				first: &first, second: &second, active: &active, gitInput: &gitInput,
			})
		}
		c := newCache()
		defer func() {
			releaseFirst()
			releaseSecond()
			c.Close()
			c.remoteInputs.Close()
			tasks.BlockTillDone()
		}()
		descriptors := []remoteInput{
			{Operation: remoteHTTP, URL: "https://example.invalid/a"},
			{Operation: remoteHTTP, URL: "https://example.invalid/b"},
			{Operation: remoteGit, URL: "https://example.invalid/base.git", Ref: "main"},
			{Operation: remoteHTTP, URL: "https://example.invalid/shared"},
		}
		var snap readSetSnapshot
		if (*testCaseInput) == "validation" {
			for index, descriptor := range descriptors {
				_, identity, err := descriptor.acquire(t.Context(), c, &treeFS{})
				assert.NoError(t, err)
				entry := remoteEntry{Input: descriptor, Identity: identity}
				if index >= 2 {
					entry.Parents = []remoteInput{descriptors[index-2]}
				}
				snap.Remotes = append(snap.Remotes, entry)
			}
			c.Close()
			c.remoteInputs.Close()
			c = newCache()
		}
		exercise.Store(true)
		ready, begin := make(chan struct{}, 2), make(chan struct{})

		results := make(chan remoteSlotResult, 2)
		for index := range 2 {
			render := func(context.Context) {
				executeRemoteSlotRender(t.Context(), t, &remoteSlotRenderInputs{
					scenario: &testCaseInput, ready: &ready, begin: &begin, tasks: &tasks,
					snapshot: &snap, cache: &c, index: &index, results: &results,
				})
			}
			if (*testCaseInput) == "standalone" {
				go render(t.Context())
			} else {
				tasks.Go(t.Context(), "parent-render", render)
			}
		}
		<-ready
		<-ready
		close(begin)
		a, b := <-entered, <-entered
		assert.FatalIf(
			t,
			(a != "/a" || b != "/b") && (a != "/b" || b != "/a"),
			"independent frontier did not start first: %q %q",
			a,
			b,
		)
		synctest.Wait()
		assert.Equal(t, active.Load(), int32(2))
		releaseFirst()
		<-entered
		<-entered
		synctest.Wait()
		assert.Equal(t, active.Load(), int32(2))
		releaseSecond()
		x, y := <-results, <-results
		assert.NoError(t, x.err)
		assert.NoError(t, y.err)
		assertRemoteSlotAcquisitions(t, &remoteSlotCheckInputs{
			scenario: &testCaseInput, result: &x, otherResult: &y, cache: &c, tasks: &tasks,
			active: &active, counts: &counts,
		})
	})
}

type remoteSlotResult struct {
	out []map[string]any
	err error
}

type remoteSlotCacheInputs struct {
	owner    **sourcecache.Cache
	tasks    **task.Service
	exercise *atomic.Bool
	start    *func(string)
	first    *chan struct{}
	second   *chan struct{}
	active   *atomic.Int32
	gitInput **tree.Tree
}

func newRemoteSlotCache(t *testing.T, inputs *remoteSlotCacheInputs) *TreeCache {
	t.Helper()

	c := newTestTreeCacheWithTasks(t, *inputs.owner, *inputs.tasks)
	stubHTTPTransport(t, remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
		body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n"
		if r.URL.Path == "/shared" {
			body = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n"
		}
		if !inputs.exercise.Load() {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}
		(*inputs.start)(r.URL.Path)
		wait := *inputs.first
		if r.URL.Path == "/shared" {
			wait = *inputs.second
		}
		<-wait
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: remoteBody{
			Reader: strings.NewReader(body),
			close:  func() error { inputs.active.Add(-1); return nil },
		}}, nil
	}))
	c.SetGitBaseFetcher(
		func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			if inputs.exercise.Load() {
				(*inputs.start)("git")
				defer inputs.active.Add(-1)
				<-*inputs.second
			}
			return &store.SourceArtifact{
				FS:       *inputs.gitInput,
				Key:      "git-key",
				Content:  "git-key-generation",
				Revision: "one",
			}, nil
		},
	)
	return c
}

type remoteSlotRenderInputs struct {
	scenario **string
	ready    *chan struct{}
	begin    *chan struct{}
	tasks    **task.Service
	snapshot *readSetSnapshot
	cache    **TreeCache
	index    *int
	results  *chan remoteSlotResult
}

func executeRemoteSlotRender(ctx context.Context, t *testing.T, inputs *remoteSlotRenderInputs) {
	t.Helper()

	*inputs.ready <- struct{}{}
	<-*inputs.begin
	var held *task.Service
	if **inputs.scenario != "standalone" {
		held = *inputs.tasks
	}
	if **inputs.scenario == "validation" {
		valid, err := inputs.snapshot.stillValid(ctx, *inputs.cache, &treeFS{slot: held}, true)
		if !valid && err == nil {
			err = fmt.Errorf("unchanged snapshot invalidated")
		}
		*inputs.results <- remoteSlotResult{err: err}
		return
	}
	path := "/a"
	if *inputs.index == 1 {
		path = "/b"
	}
	input := memoryTree(t, map[string]string{
		"kustomization.yaml": fmt.Sprintf(
			"resources:\n- https://example.invalid%s\n- git::https://example.invalid/base.git//app?ref=main\n- https://example.invalid/shared\n",
			path,
		),
	})
	out, _, err := testutil.RenderMaps(Render(ctx, *inputs.cache, RenderRequest{
		Source: &store.SourceArtifact{
			FS: input,
		},
		Spec:     demoSpec,
		HeldSlot: held,
		Label:    fmt.Sprintf("/source-%d", *inputs.index),
	}, sourcecache.RetryConfig{}))
	*inputs.results <- remoteSlotResult{out: out, err: err}
}

type remoteSlotCheckInputs struct {
	scenario    **string
	result      *remoteSlotResult
	otherResult *remoteSlotResult
	cache       **TreeCache
	tasks       **task.Service
	active      *atomic.Int32
	counts      *sync.Map
}

func assertRemoteSlotAcquisitions(t *testing.T, inputs *remoteSlotCheckInputs) {
	t.Helper()
	if **inputs.scenario != "validation" {
		assert.FatalIf(
			t,
			!reflect.DeepEqual(inputs.result.out, inputs.otherResult.out) || len(inputs.result.out) == 0,
			"%s",
			"two deterministic renders differ or are empty",
		)
		for _, name := range []string{"input", "shared", "git"} {
			assert.ErrorIf(t, !hasRenderField(inputs.result.out, "name", name), "render omitted %s", name)
		}
	}
	(*inputs.cache).Close()
	(*inputs.cache).remoteInputs.Close()
	(*inputs.tasks).BlockTillDone()
	assert.Equal(t, inputs.active.Load(), int32(0))
	assert.Equal(t, (*inputs.tasks).Failures(), int64(0))
	gitCalls := int32(2)
	if **inputs.scenario != "validation" {
		gitCalls = 4
	}
	for name, want := range map[string]int32{"/a": 1, "/b": 1, "/shared": 1, "git": gitCalls} {
		value, ok := inputs.counts.Load(name)
		if !ok {
			t.Errorf("missing acquisition %s", name)
			continue
		}
		assert.Equal(t, value.(*atomic.Int32).Load(), want)
	}
}
