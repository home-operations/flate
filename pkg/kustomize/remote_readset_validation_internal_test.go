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
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
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
	captureRemoteValidationReadSet(t.Context(), t, &remoteValidationCaptureInputs{
		scenario:    &testCaseInput,
		cache:       &c,
		reads:       &rec,
		tree:        &input,
		first:       &a,
		second:      &b,
		child:       &child,
		independent: &independent,
	})
	frame, err := testEncodeFrame(t, &renderHeader{Reads: rec.snapshot()}, nil)
	assert.NoError(t, err)
	snapFrame, err := testDecodeFrame(t, frame)
	snap := snapFrame.header.Reads
	assert.NoError(t, err)
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
	assert.FatalIf(
		t,
		valid != ((*testCaseInput) == "stable-chain") ||
			(err != nil) != ((*testCaseInput) == "canceled-parent"),
		"validation=%t error=%v",
		valid,
		err,
	)
	assert.FatalIf(
		t, (*testCaseInput) ==
			"canceled-parent" && !errors.Is(err, context.Canceled), "lost cancellation: %v",
		err,
	)
	mu.Lock()
	defer mu.Unlock()
	assertRemoteValidationRequests(t, &remoteValidationRequestInputs{
		scenario: &testCaseInput,
		calls:    &calls,
		first:    &a,
		second:   &b,
	})
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

type remoteValidationCaptureInputs struct {
	scenario    **string
	cache       **TreeCache
	reads       **readSet
	tree        **treeFS
	first       *remoteInput
	second      *remoteInput
	child       *remoteInput
	independent *remoteInput
}

func captureRemoteValidationReadSet(ctx context.Context, t *testing.T, inputs *remoteValidationCaptureInputs) {
	t.Helper()

	if **inputs.scenario == "descriptor-cycle" {
		{
			err := preflightRemoteResources(ctx, *inputs.cache, *inputs.tree, "/src")
			assert.NoError(t, err)
		}
		want := map[remoteInput][]remoteInput{
			*inputs.first:       {*inputs.second},
			*inputs.second:      {*inputs.first},
			*inputs.independent: nil,
		}
		for entryIndex := range (*inputs.reads).snapshot().Remotes {
			entry := (*inputs.reads).snapshot().Remotes[entryIndex]
			{
				diff := cmp.Diff(want[entry.Input], entry.Parents)
				assert.FatalIf(t, diff != "", "finite subpath cycle parents (-want +got):\n%s", diff)
			}
		}
		if len((*inputs.reads).remotes) != len(want) {
			t.Fatalf("cycle observations=%+v", (*inputs.reads).snapshot().Remotes)
		}
	} else {
		for _, descriptor := range []remoteInput{*inputs.child, *inputs.independent, *inputs.second, *inputs.first} {
			_, identity, err := descriptor.acquire(ctx, *inputs.cache, *inputs.tree)
			assert.NoError(t, err)
			var parents []remoteInput
			switch descriptor {
			case *inputs.child:
				parents = []remoteInput{*inputs.second, *inputs.first}
			case *inputs.second:
				if **inputs.scenario != "shared-parent-changes" {
					parents = []remoteInput{*inputs.first}
				}
			}
			if **inputs.scenario == "missing-parent" && descriptor == *inputs.child {
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
			(*inputs.reads).recordRemote(descriptor, &identity, parents)
		}
	}
}

type remoteValidationRequestInputs struct {
	scenario **string
	calls    *[]string
	first    *remoteInput
	second   *remoteInput
}

func assertRemoteValidationRequests(t *testing.T, inputs *remoteValidationRequestInputs) {
	t.Helper()

	counts := make(map[string]int)
	positions := make(map[string]int)
	for index, call := range *inputs.calls {
		counts[call]++
		positions[call] = index
	}
	var want []string
	switch **inputs.scenario {
	case "stable-chain", "changed-leaf":
		want = []string{inputs.first.URL, inputs.second.URL, "/a-child", "/z-independent"}
	case "shared-parent-changes":
		want = []string{inputs.first.URL, inputs.second.URL, "/z-independent"}
	case "parent-failure":
		want = []string{inputs.first.URL, "/z-independent"}
	case "canceled-parent":
		want = []string{inputs.first.URL}
		if counts["/z-independent"] == 1 {
			want = append(want, "/z-independent")
		}
	case "independent-input":
		want = []string{inputs.first.URL, "/0-independent"}
	}
	assert.FatalIf(
		t,
		len(*inputs.calls) != len(want),
		"requests=%v, want %v in dependency order",
		*inputs.calls,
		want,
	)
	testReadSetRemoteValidationCaseLoop(t, struct {
		want   *[]string
		counts *map[string]int
	}{want: &want, counts: &counts})
	if **inputs.scenario == "stable-chain" || **inputs.scenario == "changed-leaf" {
		assert.ErrorIf(
			t,
			positions[inputs.first.URL] >= positions[inputs.second.URL] ||
				positions[inputs.second.URL] >= positions["/a-child"],
			"child preceded a validated parent: %v",
			*inputs.calls,
		)
	}
}
