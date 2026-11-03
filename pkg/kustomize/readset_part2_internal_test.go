package kustomize

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/google/go-cmp/cmp"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testReadSetRemoteValidationCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	var mu sync.Mutex
	var calls []string
	record := func(url string) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, url)
	}
	var changed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(r.URL.Path)
		body := "remote data"
		if changed.Load() &&
			((*testCaseInput) == "changed-leaf" && r.URL.Path == "/a-child" || (*testCaseInput) == "independent-input" && r.URL.Path == "/0-independent") {
			body = "changed data"
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	a := remoteInput{
		Operation:  remoteGit,
		URL:        "file:///example.invalid/a.git",
		Ref:        "main",
		Timeout:    27 * time.Second,
		Submodules: true,
	}
	b := remoteInput{
		Operation:  remoteGit,
		URL:        "file:///example.invalid/z.git",
		Ref:        "main",
		Timeout:    27 * time.Second,
		Submodules: true,
	}
	child := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/a-child"}
	independent := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/z-independent"}
	if (*testCaseInput) == "independent-input" || (*testCaseInput) == "descriptor-cycle" ||
		(*testCaseInput) == "missing-parent" {
		independent.URL = srv.URL + "/0-independent"
	}
	repositories := map[string]map[string]string{
		a.URL: {
			"subdir/kustomization.yaml": "resources: ['" + b.URL + "//subdir?ref=main']\n",
			"leaf/kustomization.yaml":   "resources: []\n",
		},
		b.URL: {"subdir/kustomization.yaml": "resources: ['" + a.URL + "//leaf?ref=main']\n"},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	newCache := func() *TreeCache {
		c := newTestTreeCache(t, "")
		c.SetGitBaseFetcher(
			func(_ context.Context, request GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				record(request.URL)
				revision := "1"
				if changed.Load() {
					switch {
					case (*testCaseInput) == "shared-parent-changes" && request.URL == b.URL:
						revision = "2"
					case (*testCaseInput) == "parent-failure" && request.URL == a.URL:
						return nil, fs.ErrNotExist
					case (*testCaseInput) == "canceled-parent" && request.URL == a.URL:
						cancel()
						return nil, ctx.Err()
					}
				}
				return &store.SourceArtifact{
					FS:       memoryTree(t, repositories[request.URL]),
					Key:      request.URL,
					Content:  revision,
					Revision: revision,
				}, nil
			},
		)
		return c
	}
	rec := newReadSet("/src")
	input := newTreeFS(
		memoryTree(
			t,
			map[string]string{
				"kustomization.yaml": "resources: ['" + independent.URL + "', '" + a.URL + "//subdir?ref=main']\n",
			},
		),
		"/src",
		rec,
	)
	c := newCache()
	if (*testCaseInput) == "descriptor-cycle" {
		{
			err := preflightRemoteResources(t.Context(), c, input, "/src")
			assert.NoError(t, err)
		}
		want := map[remoteInput][]remoteInput{a: {b}, b: {a}, independent: nil}
		for entryIndex := range rec.snapshot().Remotes {
			entry := rec.snapshot().Remotes[entryIndex]
			{
				diff := cmp.Diff(want[entry.Input], entry.Parents)
				assert.FatalIf(t, diff != "", "finite subpath cycle parents (-want +got):\n%s", diff)
			}
		}
		if len(rec.remotes) != len(want) {
			t.Fatalf("cycle observations=%+v", rec.snapshot().Remotes)
		}
	} else {
		for _, descriptor := range []remoteInput{child, independent, b, a} {
			_, identity, err := descriptor.acquire(t.Context(), c, input)
			{
				assert.NoError(t, err)
			}
			var parents []remoteInput
			switch descriptor {
			case child:
				parents = []remoteInput{b, a}
			case b:
				if (*testCaseInput) != "shared-parent-changes" {
					parents = []remoteInput{a}
				}
			}
			if (*testCaseInput) == "missing-parent" && descriptor == child {
				parents = append(
					parents,
					remoteInput{
						Operation:  remoteGit,
						URL:        "file:///example.invalid/missing.git",
						Timeout:    27 * time.Second,
						Submodules: true,
					},
				)
			}
			rec.recordRemote(descriptor, identity, parents)
		}
	}
	frame, err := encodeFrame(&renderHeader{Reads: rec.snapshot()}, nil)
	{
		assert.NoError(t, err)
	}
	snapFrame, err := decodeFrame(frame)
	snap := snapFrame.header.Reads
	{
		assert.NoError(t, err)
	}
	if (*testCaseInput) == "duplicate-entry" {
		snap.Remotes = append(snap.Remotes, snap.Remotes[0])
	}
	closeTestTreeCache(t, c)
	mu.Lock()
	calls = nil
	mu.Unlock()
	changed.Store(true)
	c = newCache()
	valid, err := snap.stillValid(ctx, c, input, true)
	{
		assert.FatalIf(
			t,
			valid != ((*testCaseInput) == "stable-chain") ||
				(err != nil) != ((*testCaseInput) == "canceled-parent"),
			"validation=%t error=%v",
			valid,
			err,
		)
	}
	{
		assert.FatalIf(
			t, (*testCaseInput) ==
				"canceled-parent" && !errors.Is(err, context.Canceled), "lost cancellation: %v",
			err,
		)
	}
	mu.Lock()
	defer mu.Unlock()
	counts := make(map[string]int)
	positions := make(map[string]int)
	for index, call := range calls {
		counts[call]++
		positions[call] = index
	}
	var want []string
	switch *testCaseInput {
	case "stable-chain", "changed-leaf":
		want = []string{a.URL, b.URL, "/a-child", "/z-independent"}
	case "shared-parent-changes":
		want = []string{a.URL, b.URL, "/z-independent"}
	case "parent-failure":
		want = []string{a.URL, "/z-independent"}
	case "canceled-parent":
		want = []string{a.URL}
		if counts["/z-independent"] == 1 {
			want = append(want, "/z-independent")
		}
	case "independent-input":
		want = []string{a.URL, "/0-independent"}
	}
	{
		assert.FatalIf(t, len(calls) != len(want), "requests=%v, want %v in dependency order", calls, want)
	}
	testReadSetRemoteValidationCaseLoop(t, struct {
		want   *[]string
		counts *map[string]int
	}{want: &want, counts: &counts})
	if (*testCaseInput) == "stable-chain" || (*testCaseInput) == "changed-leaf" {
		{
			assert.ErrorIf(
				t,
				positions[a.URL] >= positions[b.URL] || positions[b.URL] >= positions["/a-child"],
				"child preceded a validated parent: %v",
				calls,
			)
		}
	}
}

func testReadSetRemoteFailureCancellationCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	entered, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		if !(*testCaseInput) {
			<-release
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	c := newTestTreeCache(t, "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	probe := remoteInput{Operation: remoteHTTP, URL: srv.URL}
	snap := readSetSnapshot{
		Remotes: []remoteEntry{{Input: probe, Identity: remoteIdentity{HTTPStatus: http.StatusNotFound}}},
	}
	if *testCaseInput {
		_, identity, err := probe.acquire(t.Context(), c, &treeFS{})
		{
			assert.FatalIf(
				t,
				err == nil || identity != snap.Remotes[0].Identity,
				"completed probe=%+v error=%v",
				identity,
				err,
			)
		}
		git := remoteInput{Operation: remoteGit, URL: "file:///repo"}
		snap.Remotes = append(
			snap.Remotes,
			remoteEntry{
				Input:    git,
				Identity: remoteIdentity{Key: "family", Revision: "1"},
				Parents:  []remoteInput{probe},
			},
		)
		c.SetGitBaseFetcher(
			func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				cancel()
				return &store.SourceArtifact{FS: fstest.MapFS{}, Key: "family", Revision: "1"}, nil
			},
		)
	}
	type result struct {
		valid bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		valid, err := snap.stillValid(ctx, c, &treeFS{}, true)
		done <- result{valid, err}
	}()
	if !(*testCaseInput) {
		<-entered
		cancel()
		close(release)
	}
	{
		got := <-done
		assert.FatalIf(
			t,
			got.valid || !errors.Is(got.err, context.Canceled),
			"canceled validation=%t error=%v",
			got.valid,
			got.err,
		)
	}
}

func testReadSetRemoteValidationCaseLoop(t *testing.T, loopInputs struct {
	want   *[]string
	counts *map[string]int
},
) {
	t.Helper()

	for _, call := range *loopInputs.want {
		if (*loopInputs.counts)[call] != 1 {
			t.Errorf("request %s count=%d, want 1", call, (*loopInputs.counts)[call])
		}
	}
}
