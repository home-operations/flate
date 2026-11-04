package kustomize

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

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
