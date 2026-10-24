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

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRenderFlux_RemoteValidationSlots(t *testing.T) {
	for _, mode := range []string{"validation", "miss", "standalone"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				tasks := task.NewBounded(2)
				owner := cachetest.New(t, "", sourcecache.New)
				first, second := make(chan struct{}), make(chan struct{})
				releaseFirst, releaseSecond := sync.OnceFunc(func() { close(first) }), sync.OnceFunc(func() { close(second) })
				defer releaseFirst()
				defer releaseSecond()
				entered := make(chan string, 16)
				var active atomic.Int32
				var exercise atomic.Bool
				var counts sync.Map
				start := func(name string) {
					if n := active.Add(1); n > 2 {
						t.Errorf("active acquisition owners=%d, bound=2", n)
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
					c := newTestTreeCacheWithTasks(t, owner, tasks)
					stubHTTPTransport(t, remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
						body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n"
						if r.URL.Path == "/shared" {
							body = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n"
						}
						if !exercise.Load() {
							return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
						}
						start(r.URL.Path)
						wait := first
						if r.URL.Path == "/shared" {
							wait = second
						}
						<-wait
						return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: remoteBody{
							Reader: strings.NewReader(body),
							close:  func() error { active.Add(-1); return nil },
						}}, nil
					}))
					c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
						if exercise.Load() {
							start("git")
							defer active.Add(-1)
							<-second
						}
						return &store.SourceArtifact{FS: gitInput, Key: "git-key", Revision: "one"}, nil
					})
					return c
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
				if mode == "validation" {
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
				type result struct {
					out []map[string]any
					err error
				}
				results := make(chan result, 2)
				for index := range 2 {
					render := func(context.Context) {
						ready <- struct{}{}
						<-begin
						var held *task.Service
						if mode != "standalone" {
							held = tasks
						}
						if mode == "validation" {
							valid, err := snap.stillValid(t.Context(), c, &treeFS{slot: held}, true)
							if !valid && err == nil {
								err = fmt.Errorf("unchanged snapshot invalidated")
							}
							results <- result{err: err}
							return
						}
						path := "/a"
						if index == 1 {
							path = "/b"
						}
						input := memoryTree(t, map[string]string{
							"kustomization.yaml": fmt.Sprintf("resources:\n- https://example.invalid%s\n- https://example.invalid/base.git//app?ref=main\n- https://example.invalid/shared\n", path),
						})
						out, err := RenderFlux(t.Context(), c, RenderRequest{
							Source: &store.SourceArtifact{FS: input}, Spec: demoSpec, HeldSlot: held,
						}, sourcecache.RetryConfig{})
						results <- result{out: out, err: err}
					}
					if mode == "standalone" {
						go render(t.Context())
					} else {
						tasks.Go(t.Context(), "parent-render", render)
					}
				}
				<-ready
				<-ready
				close(begin)
				a, b := <-entered, <-entered
				if (a != "/a" || b != "/b") && (a != "/b" || b != "/a") {
					t.Fatalf("independent frontier did not start first: %q %q", a, b)
				}
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
				if mode != "validation" {
					if !reflect.DeepEqual(x.out, y.out) || len(x.out) == 0 {
						t.Fatal("two deterministic renders differ or are empty")
					}
					for _, name := range []string{"input", "shared", "git"} {
						if !hasRenderField(x.out, "name", name) {
							t.Errorf("render omitted %s", name)
						}
					}
				}
				c.Close()
				c.remoteInputs.Close()
				tasks.BlockTillDone()
				assert.Equal(t, active.Load(), int32(0))
				assert.Equal(t, tasks.Failures(), int64(0))
				for name, want := range map[string]int32{"/a": 1, "/b": 1, "/shared": 1, "git": 2} {
					value, ok := counts.Load(name)
					if !ok {
						t.Errorf("missing acquisition %s", name)
						continue
					}
					assert.Equal(t, value.(*atomic.Int32).Load(), want)
				}
			})
		})
	}
}

func TestReadSet_NoRemotesNoAlloc(t *testing.T) {
	ctx, input := t.Context(), &treeFS{}
	var snap readSetSnapshot
	if allocations := testing.AllocsPerRun(1, func() {
		valid, err := snap.stillValid(ctx, nil, input, true)
		if !valid || err != nil {
			t.Fatalf("zero remote validation: valid=%t err=%v", valid, err)
		}
	}); allocations != 0 {
		t.Fatalf("zero remote validation allocations=%v, want 0", allocations)
	}
}

type remoteBody struct {
	io.Reader
	close func() error
}

func (b remoteBody) Close() error { return b.close() }
